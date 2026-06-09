# CLAUDE.md

Project rulebook for the Claude Code agent. Read this fully before writing code.

## What this is

A **Notification Service** microservice: mass SMS/Email dispatch with priority routing,
persistent queues, at-least-once + business-level exactly-once delivery, idempotent request
handling, retry with backoff, and full delivery-status tracking.

Stack: **Python 3.12 / FastAPI / PostgreSQL 16 / RabbitMQ 3.13 / Redis 7**, fully containerized.
The entire stack must come up with one `docker compose up --build`. This is a take-home
assignment — optimize for clean architecture, correct distributed-systems behavior, and a
real integration test suite, not feature volume.

---

## Working directory (read first)

This `CLAUDE.md` lives at the repo root, which is also the directory the agent is launched
from. **Build the entire project inside `app/`.** Do not create project files at the repo
root — the root holds only this `CLAUDE.md`. Everything else (source, Dockerfile,
docker-compose.yml, tests, migrations, scripts, README, `.env.example`) goes under `app/`.

- `app/` is the project root. `cd app` before running build/run/test commands.
- The Python package inside it is `app/notification_service/` (do not name it `app` — avoid
  `app/app`). Import root is `notification_service`.
- All relative paths elsewhere in this file are relative to `app/`.

---

## Commands

```bash
# Host bootstrap (installs Docker on a clean Ubuntu host; idempotent) — run from repo root
./app/scripts/bootstrap.sh

# Everything below runs from inside app/
cd app

# Run the whole stack from clean
docker compose up --build

# Full clean restart (the canonical acceptance check)
docker compose down -v && docker compose up --build

# Migrations (also run automatically by the `migrate` one-shot service)
alembic upgrade head
alembic revision --autogenerate -m "msg"

# Tests (integration via testcontainers — needs Docker available)
pytest
pytest -q tests/integration

# Lint / format
ruff check . && ruff format .

# Process entrypoints (used by the compose service commands)
#   api:        uvicorn notification_service.main:app --host 0.0.0.0 --port 8000
#   dispatcher: python -m notification_service.workers.dispatcher
#   receipts:   python -m notification_service.workers.receipts
```

API docs: Swagger at `/docs`, OpenAPI JSON at `/openapi.json` (commit a snapshot to `docs/`).

---

## Environment rules (non-negotiable)

- **Everything runs in containers.** The host (a VirtualBox guest) gets **only** Docker
  Engine + the Compose plugin via `app/scripts/bootstrap.sh`. Postgres, RabbitMQ, Redis are
  compose services — **never** install them on the host.
- **Do not use any host MySQL.** It is irrelevant to this project. The database is the
  Postgres container.
- All config comes from env (`.env`, with a committed `.env.example`). No hardcoded
  secrets, hosts, or ports.

---

## Architecture

One codebase, three app processes as separate compose services (same image, different command):

- **api** — FastAPI HTTP. Accepts requests, enforces idempotency, persists notification rows
  as `queued`, publishes one message per recipient. **Never calls providers.**
- **dispatcher** — consumes the priority work queue, performs the atomic `queued → sent`
  transition, calls the provider mock, routes failures to retry/parking.
- **receipts** — consumes provider delivery receipts, performs `sent → delivered | rejected`.

Flow: `client → api → Postgres(queued) + publish → RabbitMQ(work) → dispatcher (CAS + provider)
→ provider emits receipt → RabbitMQ(receipts) → receipts consumer → delivered|rejected`.

---

## CRITICAL INVARIANTS — do not violate

These are the points the assignment is actually testing. Get them exactly right.

1. **Priority needs `prefetch_count = 1`.** The work queue is a single
   `x-max-priority: 10` queue (transactional published at priority 10, marketing at 1).
   Priority ordering only holds if the consumer prefetch is 1 — otherwise prefetch buffering
   defeats it. Set QoS prefetch=1 on the dispatcher. This is *the* mechanism that makes
   transactional overtake marketing.

2. **Business exactly-once = compare-and-set transition.** Before calling the provider, the
   dispatcher does, inside a transaction:
   `UPDATE notifications SET status='sent', sent_at=now() WHERE id=:id AND status='queued'`.
   Only the row that actually flips (rowcount == 1) proceeds to call the provider.
   Redeliveries see `status != 'queued'` → ack and skip. Pass `notification.id` to the
   provider mock as its idempotency key so the mock is idempotent too.

3. **Ack only after the DB commit.** Manual acks everywhere. On handler exception:
   `nack(requeue=False)` and republish to the appropriate retry tier — never silently drop,
   never ack before the state change is durable. This preserves at-least-once across crashes.

4. **Two-layer request idempotency.** `Idempotency-Key` header → Redis `SETNX idemp:{key}`
   (24h TTL) as the fast path, **backed by** a UNIQUE constraint on `batches.idempotency_key`
   as the source of truth. Duplicate request → return the original batch with `200`, create
   nothing new.

5. **Tiered TTL retry, no plugins.** Transient failures hop through fixed-delay retry queues
   `retry.5s → retry.30s → retry.120s` (each `x-message-ttl` + dead-letter back to the work
   exchange). Max 3 retries; then publish to `notifications.parking` and set status
   `rejected` with `last_error`. Do not use the delayed-message exchange plugin.

6. **Durability.** Durable exchanges/queues, persistent messages (`delivery_mode=2`).
   Topology must survive a broker restart.

7. **Every status change writes a `status_events` row in the same transaction** as the
   `notifications.status` update.

---

## Statuses

`queued` → `sent` → `delivered`, with `rejected` as the terminal failure state.
RU mapping for README/docs: queued=в очереди, sent=отправлено, delivered=доставлено,
rejected=отброшено.

---

## Data model (Alembic-migrated)

UUID PKs.

- **batches**: id, channel(`sms|email`), type(`transactional|marketing`), body,
  idempotency_key **UNIQUE nullable**, total, created_at.
- **notifications**: id (also provider idempotency key), batch_id FK, subscriber_id
  **indexed**, channel, type, body, status **indexed** (`queued|sent|delivered|rejected`),
  provider_message_id, retry_count, last_error, created_at, sent_at, delivered_at, updated_at.
  Composite index `(subscriber_id, created_at desc)`.
- **status_events** (append-only): id, notification_id FK indexed, status, detail, created_at.

---

## Broker topology (RabbitMQ)

Declared idempotently on startup.

- Exchange `notifications` (direct).
- `notifications.work` — `x-max-priority: 10`, routing key `work`, consumed with prefetch=1.
- `notifications.dlx` — dead-letter exchange.
- `notifications.retry.5s | .30s | .120s` — fixed TTL, dead-letter back to `work`.
- `notifications.parking` — terminal dead messages, no consumer.
- `notifications.receipts` — routing key `receipt`.

---

## Providers

`providers/base.py` defines a `Provider` protocol with
`async send(notification_id, recipient, body) -> SendResult`. `SmsMockProvider` and
`EmailMockProvider`, selected by a factory on `channel`.

- `send()` simulates gateway acceptance and returns a `provider_message_id`, then
  **asynchronously emits a delivery receipt** to `notifications.receipts`
  (delivered or rejected).
- Behavior is configurable via `PROVIDER_MODE`:
  `always_deliver | always_reject | transient_then_deliver | random` (default dev: `random`,
  ~90% delivered / ~5% rejected / ~5% transient). Tests force a deterministic mode.
- Mocks must record their calls so tests can assert the right provider was called with the
  right args.

---

## Redis usage

- Idempotency fast path (see invariant 4).
- Outbound rate limiting per channel (sliding-window sorted set or fixed-window counter,
  keys `rate:{channel}`, limits from `RATE_LIMIT_SMS_PER_SEC` / `RATE_LIMIT_EMAIL_PER_SEC`).
  Over limit → requeue to the 5s retry tier, never drop.

---

## API

Prefix `/api/v1`.

- `POST /notifications` — body `{channel, type, message, recipient_ids[]}`, optional
  `Idempotency-Key` header. → `202` with `batch_id` + per-recipient `{id, subscriber_id, status:queued}`.
  Validation → `422`. Duplicate key → `200` with original batch.
- `GET /subscribers/{subscriber_id}/notifications` — optional `status`, `limit`, `offset`;
  returns current status + ordered `history` per notification.
- `GET /notifications/{id}` — single notification status.
- `GET /health` — checks DB, broker, Redis reachability.

---

## Code conventions

- Async everywhere: SQLAlchemy 2.0 async + asyncpg, aio-pika, redis-py async, httpx for tests.
- pydantic-settings for config; pydantic models for all API request/response schemas.
- Keep layers separate: `api/` (HTTP) → `services/` (business logic) → `db/repositories.py`
  (data access). Broker concerns in `broker/`, consumer entrypoints in `workers/`.
- The CAS transition and the status-event write live in `services/status.py` — one place.
- Prefer clarity over cleverness. Small, logical commits. Run `ruff` before finishing.

---

## Project structure

```
<repo root>/                       # launch dir — holds ONLY this file
  CLAUDE.md
  app/                             # project root — all work happens here
    notification_service/          # Python package (import root)
      main.py                      # FastAPI app + lifespan (declare topology)
      config.py
      api/{routes_notifications.py, routes_subscribers.py, schemas.py}
      db/{models.py, session.py, repositories.py}
      broker/{rabbit.py, publisher.py, consumer.py}
      services/{notification_service.py, idempotency.py, rate_limiter.py, status.py}
      providers/{base.py, sms_mock.py, email_mock.py, factory.py}
      workers/{dispatcher.py, receipts.py}
    migrations/                    # alembic
    tests/{conftest.py, integration/}
    scripts/bootstrap.sh
    Dockerfile  docker-compose.yml  alembic.ini  pyproject.toml  .env.example  README.md
```

Module references inside the package use the `notification_service.` root, e.g.
`notification_service.services.status`, `notification_service.workers.dispatcher`.

---

## Docker / compose

- One multi-stage Dockerfile (`python:3.12-slim`, non-root user); same image for api/dispatcher/receipts.
- compose services: `postgres`, `rabbitmq` (management), `redis` (all with healthchecks),
  `migrate` (one-shot `alembic upgrade head`), `api` (uvicorn, :8000), `dispatcher`, `receipts`.
- App services `depends_on` the `migrate` completion and infra healthchecks.

---

## Testing rules (mandatory)

Integration tests with **pytest + pytest-asyncio + httpx.AsyncClient + testcontainers**
(real Postgres/RabbitMQ/Redis containers; mock only the provider, set to a deterministic mode).
Cover, each asserting DB state + provider calls:

1. Bulk accept → N `queued` rows + N messages + `202`.
2. Full happy chain → provider called with correct args → `queued→sent→delivered`,
   all transitions in `status_events`.
3. Rejected path (`always_reject`) → terminal `rejected` + `last_error`.
4. Priority → transactional processed before earlier-enqueued marketing.
5. Idempotency → same key twice → one batch, no duplicate rows, original `batch_id` returned.
6. Exactly-once under redelivery → provider `send` called once.
7. Retry/backoff (`transient_then_deliver`) → `retry_count` increments → `delivered`;
   and retries exhausted → `rejected` + message in parking.
8. History API → correct current status + ordered history.

Plus unit tests for the CAS transition and the idempotency service.

---

## Build order

1. Scaffold: pyproject, config, `.env.example`, ruff.
2. Models + initial Alembic migration.
3. Broker topology (work/dlx/retry/parking/receipts).
4. Provider mocks + factory (deterministic modes).
5. API: schemas, `POST` (persist + idempotency + publish), history `GET`, `/health`.
6. Workers: dispatcher (CAS + provider + rate limit + retry routing), receipts (finalization).
7. Dockerfile + docker-compose + `migrate` one-shot.
8. Integration tests (testcontainers).
9. README, Postman collection, committed `openapi.json`, bootstrap script.
10. `docker compose down -v && docker compose up --build` from clean, then `pytest` green.

---

## Definition of done

- [ ] `app/scripts/bootstrap.sh` installs Docker on a clean Ubuntu host (idempotent).
- [ ] `docker compose up --build` brings the whole stack up from scratch; `/health` green.
- [ ] All endpoints work; Swagger renders at `/docs`; `openapi.json` + Postman committed.
- [ ] Durable queues + persistent messages survive a broker restart.
- [ ] At-least-once + business exactly-once (CAS) verified by tests.
- [ ] Two-layer request idempotency verified.
- [ ] Priority (transactional overtakes marketing) verified.
- [ ] Tiered retry → parking → `rejected` verified.
- [ ] Full integration suite green.
- [ ] No host MySQL used; nothing installed on host beyond Docker.
