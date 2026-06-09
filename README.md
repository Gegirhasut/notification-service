# Notification Service

A containerized microservice for **mass SMS/Email dispatch** with priority routing, durable
queues, retry-with-backoff, two-layer request idempotency, and business-level
**exactly-once** delivery with full delivery-status tracking. Clients submit a batch of
recipients; the service persists each notification as `queued`, fans one message per
recipient onto a priority work queue, and drives it through `queued → sent → delivered`
(or terminal `rejected`) while auditing every transition. Transactional traffic overtakes
marketing traffic, redeliveries never double-send, and the whole stack comes up with a
single `docker compose up --build`.

## Tech stack

- **Python 3.12** · **FastAPI** (async, SQLAlchemy 2.0 + asyncpg, aio-pika, redis-py)
- **PostgreSQL 16** — durable state, the compare-and-set delivery gate, append-only audit
- **RabbitMQ 3.13** — priority work queue, dead-letter exchange, tiered TTL retry, parking
- **Redis 7** — idempotency fast path and outbound per-channel rate limiting
- **Docker** / Docker Compose — the entire stack (app + infra) is containerized

## Quick start

**Prerequisites:** Docker Engine + the Compose plugin. Nothing else is installed on the host.

```bash
# Optional — install Docker on a clean Ubuntu host (idempotent). Run from the repo root.
./app/scripts/bootstrap.sh

# Bring up the whole stack (api, dispatcher, receipts, Postgres, RabbitMQ, Redis, migrate).
cd app
docker compose up --build
```

A `.env` is optional — compose has sane local-dev defaults baked in (copy `.env.example`
to `.env` to override). Only **two host ports are published**:

- **8000** — the API (and Swagger UI)
- **15672** — the RabbitMQ management UI

Postgres, RabbitMQ (AMQP) and Redis are reached over the compose network by service name;
their ports are deliberately not exposed to the host, so they never clash with anything
already running on it.

Canonical clean restart (the acceptance check):

```bash
docker compose down -v && docker compose up --build
```

## API

Base prefix `/api/v1`. Interactive docs: **Swagger UI at <http://localhost:8000/docs>**.
Full schema is committed at [`app/docs/openapi.json`](app/docs/openapi.json); a Postman
collection is at [`app/docs/postman_collection.json`](app/docs/postman_collection.json).

| Method & path | Description |
|---|---|
| `POST /api/v1/notifications` | Body `{channel, type, message, recipient_ids[]}`, optional `Idempotency-Key` header. → **202** with `batch_id` + per-recipient `{id, subscriber_id, status:queued}`. Validation → **422**. Duplicate key → **200** with the original batch. |
| `GET /api/v1/notifications/{id}` | Single notification: current status + ordered `history`. |
| `GET /api/v1/subscribers/{subscriber_id}/notifications` | Optional `status`, `limit`, `offset`; current status + ordered `history` per notification. |
| `GET /health` | DB / broker / Redis reachability. **200** healthy, **503** degraded. |

Create a transactional SMS batch for three recipients (idempotency key included):

```bash
curl -i -X POST http://localhost:8000/api/v1/notifications \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-1' \
  -d '{
        "channel": "sms",
        "type": "transactional",
        "message": "Your code is 123456",
        "recipient_ids": ["alice", "bob", "carol"]
      }'
```

Repeating the same request with the same `Idempotency-Key` returns the original batch with
**200** and creates nothing new. Follow a single notification, or a subscriber's history:

```bash
curl -s http://localhost:8000/api/v1/notifications/<id>
curl -s http://localhost:8000/api/v1/subscribers/alice/notifications
```

### Request validation

`recipient_ids` are treated as **opaque identifiers** — no phone/email parsing,
structural rules only. Invalid input → `422`.

- **message**: trimmed; must be non-empty after trimming; max 1000 characters.
- **recipient_ids**: 1…`MAX_RECIPIENTS` items (`MAX_RECIPIENTS` env, default 1000);
  each entry trimmed, non-empty, max 128 characters; the list is **de-duplicated**
  preserving first-seen order, and `total` reflects the unique count.
- **channel** / **type**: must be one of the allowed values; a missing required
  field is also `422`.
- **Idempotency-Key** header (optional): if present, must be non-blank and ≤ 255
  characters.

## Status model

`queued → sent → delivered`, with `rejected` as the terminal failure state.

| English   | Русский    |
|-----------|------------|
| queued    | в очереди  |
| sent      | отправлено |
| delivered | доставлено |
| rejected  | отброшено  |

## Architecture

One codebase, one image, three app processes (separate compose services, same image,
different command):

| Process       | Role |
|---------------|------|
| **api**       | FastAPI HTTP. Accepts requests, enforces idempotency, persists notifications as `queued`, publishes one message per recipient. **Never calls providers.** |
| **dispatcher**| Consumes the priority work queue (`prefetch=1`), performs the atomic `queued → sent` compare-and-set, calls the provider mock, routes failures to retry/parking. |
| **receipts**  | Consumes provider delivery receipts, performs `sent → delivered \| rejected`. |
| **sweeper**   | Periodic reconciler (safety net): redrives rows stranded in `queued` (the commit→publish window) and re-resolves rows stuck in `sent` (a lost receipt). Idempotent — the CAS gate makes a redundant redrive a no-op. |

### Message flow

```
client → api → Postgres(queued) + publish
                       → RabbitMQ(notifications.work, x-max-priority:10)
                       → dispatcher  (CAS queued→sent, then provider.send)
                       → provider emits a receipt asynchronously
                       → RabbitMQ(notifications.receipts)
                       → receipts consumer → delivered | rejected
```

### Broker topology

Declared idempotently on startup; durable exchanges/queues, persistent messages
(`delivery_mode=2`), so the topology survives a broker restart.

- Exchange `notifications` (direct).
- `notifications.work` — `x-max-priority: 10`, routing key `work`, consumed with prefetch=1
  (transactional published at priority 10, marketing at 1); dead-letters to parking as a
  backstop.
- `notifications.dlx` — dead-letter exchange.
- `notifications.retry.5s | .30s | .120s` — fixed `x-message-ttl`, dead-letter back to the
  work exchange.
- `notifications.parking` — terminal dead messages, no consumer.
- `notifications.receipts` — routing key `receipt`; dead-letters to parking as a backstop.

## Delivery semantics

- **Priority (prefetch=1).** The work queue is a single `x-max-priority: 10` queue.
  Priority ordering only holds if the consumer's QoS prefetch is 1 — otherwise prefetch
  buffering defeats it. This is the mechanism by which transactional overtakes
  earlier-enqueued marketing.
- **At-least-once (ack after commit).** Manual acks everywhere; a consumer acks only
  **after** the handler's DB transaction commits. Nothing is ever silently discarded: an
  *undecodable/poison* message is dead-lettered to `notifications.parking`; an *unexpected
  runtime fault* (a DB/broker blip) is treated as transient and re-driven — the dispatcher
  republishes it through the retry tiers (→ parking + `rejected` once `MAX_RETRIES` is
  exhausted) and the receipts consumer requeues it. Both the work and receipts queues carry a
  dead-letter route to parking as the backstop, so a `nack(requeue=False)` always lands in
  parking, never in the void.
- **Business exactly-once (CAS gate + provider idempotency key).** Before calling the
  provider, the dispatcher runs, inside a transaction,
  `UPDATE notifications SET status='sent', sent_at=now() WHERE id=:id AND status='queued'`.
  Only the caller whose update flips exactly one row proceeds; redeliveries observe
  `status != 'queued'` and ack-and-skip. Because a row briefly returns to `queued` during a
  retry window, the second guarantee is `notification.id` passed to the provider as its
  idempotency key — the provider mock deduplicates on it and emits at most one effective
  delivery per id, across any retries or concurrent duplicates.
- **Retry / backoff.** Transient failures hop through fixed-TTL retry queues
  `retry.5s → retry.30s → retry.120s` (each dead-letters back to the work exchange). After
  `MAX_RETRIES` (default 3) the message goes to `notifications.parking` and the row becomes
  `rejected` with `last_error`. No delayed-message plugin is used.
- **Deliberate `queued → sent → queued` flap.** The CAS flips `queued → sent` *before* the
  provider call (that ordering is what makes exactly-once hold). On a transient provider
  failure the row is genuinely reverted `sent → queued`, the retry counter bumps, and the
  message is republished. Since every change writes a `status_events` row in the same
  transaction, a notification that succeeds on its second attempt audits as
  `queued → sent → queued → sent → delivered`. The flap is intentional, not a bug — the
  retry `queued` event carries a detail like `retry 1: gateway temporarily unavailable`.

## Known limitations & production hardening

Honest notes on where this take-home stops short of a production deployment, and
how each gap is mitigated here:

- **The DB-commit → publish step is a non-atomic dual-write.** The API commits the
  `queued` rows and *then* publishes one work message per recipient. A crash (or a
  broker outage) in that window strands rows in `queued`. **Mitigation:** the
  **sweeper** reconciler redrives stale `queued` rows. The full production answer
  is a **transactional outbox + relay** (write the intent to an `outbox` table in
  the same transaction, publish from there), which this design deliberately omits
  for scope.
- **Delivery receipts are emitted in-process by the provider mock.** In production
  a receipt arrives from the gateway via **webhook**, independent of our process
  lifecycle; if our dispatcher dies after marking `sent` but before the mock's
  in-memory receipt task fires, that receipt is lost. **Mitigation:** the sweeper
  re-resolves rows stuck in `sent` (it re-drives them; the idempotent provider key
  re-emits the receipt).
- **Publisher confirms** *are* on: aio-pika opens channels with
  `publisher_confirms=True`, so every `await exchange.publish(...)` waits for the
  broker to acknowledge the message. No change was needed here.
- **The `/health` broker check is shallow** — it reports the AMQP connection's
  `is_closed` flag, not a round-trip probe, so a mid-reconnect broker can briefly
  read healthy.
- **The rate limiter rebounds indefinitely under sustained over-limit, by design.**
  Over-limit sends are requeued to the 5s retry tier rather than dropped or
  counted against `max_retries`, so a persistent overload keeps messages cycling
  (never lost) until capacity frees up.
- **Topology change note:** the work and receipts queues now declare a dead-letter
  route to parking. Because that changes existing queue arguments, an *already
  running* dev broker must be reset once — `docker compose down -v` — before the
  next `up`. A fresh stack and the ephemeral test infra are unaffected.

## Testing

Integration tests run against **real** Postgres/RabbitMQ/Redis via **testcontainers** (only
the provider is mocked, in a deterministic mode), plus unit tests for the CAS transition and
the idempotency service.

**Prerequisite: Docker only.** No host Python or virtualenv is needed — the suite runs
inside the Dockerfile's `test` stage, whose venv lives at `/opt/venv` inside the image, so
the vboxsf/shared-folder symlink problem never arises.

Run the full suite (the image's default CMD is `pytest -q`):

```bash
cd app
./scripts/run-tests.sh
```

Any arguments you pass **replace** the image CMD, so include the literal `pytest` before any
flags:

```bash
./scripts/run-tests.sh pytest -v
./scripts/run-tests.sh pytest -v tests/integration/test_flows.py
./scripts/run-tests.sh pytest -v -k idempotency
```

Mechanics: the runner mounts the host Docker socket, so testcontainers drives the host
Docker to spawn ephemeral Postgres/RabbitMQ/Redis on random ports, reached via
`host.docker.internal` — the test infra never clashes with anything already on the host
(e.g. a host Redis on 6379). Only the provider is mocked.

Coverage (each asserts DB state + provider calls): bulk accept, full happy chain, rejected
path, priority overtaking, two-layer idempotency, exactly-once under redelivery and under a
mid-retry duplicate, tiered retry → parking → `rejected`, and the history API — plus CAS and
idempotency unit tests.
