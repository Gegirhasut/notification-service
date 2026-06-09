# Architecture

A reference to how the Notification Service is actually built — verified against the source
under `app/notification_service/`. Names below are real module / function / class names; line
numbers are deliberately omitted.

## 1. Overview

One codebase, one Docker image, four long-running app processes plus a one-shot migrator.
Every process imports the same `notification_service` package and is selected by its compose
command:

- **api** — `notification_service.main:app` (FastAPI under uvicorn). Accepts requests, enforces
  request idempotency, persists `batches` + `notifications` rows as `queued`, and publishes one
  work message per recipient. Never calls a provider. Its lifespan also declares the broker
  topology, so the api may be the first process to boot.
- **dispatcher** — `notification_service.workers.dispatcher`. Consumes the priority work queue
  with `prefetch=1`, performs the atomic `queued → sent` compare-and-set, calls the provider
  mock, and routes failures to the retry tiers or parking.
- **receipts** — `notification_service.workers.receipts`. Consumes provider delivery receipts
  and performs the terminal `sent → delivered | rejected` transition.
- **sweeper** — `notification_service.workers.sweeper`. A periodic reconciler that redrives rows
  stranded in `queued` (the commit→publish window) and re-resolves rows stuck in `sent` (a lost
  receipt). Redrives are no-ops when the row already moved on, thanks to the CAS gate.

Infrastructure (compose services, never installed on the host):

- **Postgres 16** — source of truth for batches, notifications, and the append-only
  `status_events` audit log.
- **RabbitMQ 3.13** — single priority work queue, fixed-TTL retry tiers, a dead-letter exchange,
  parking, and a receipts queue.
- **Redis 7** — idempotency fast-path key and the per-channel outbound rate-limit window.

## 2. Directory map

```
<repo root>/
  CLAUDE.md                       # project rulebook (agent instructions)
  README.md                       # user-facing docs
  ARCHITECTURE.md                 # this file
  docker-compose.yml              # root entrypoint — `include:` pulls in app/docker-compose.yml
  app/                            # project root
    docker-compose.yml            # full stack: infra + migrate + api/dispatcher/receipts/sweeper
    Dockerfile                    # multi-stage: builder / runtime / test; non-root runtime user
    alembic.ini                   # Alembic config (script_location = migrations)
    pyproject.toml                # deps, ruff, pytest config (asyncio_mode=auto)
    .env.example                  # committed config template; copy to .env
    scripts/
      bootstrap.sh                # install Docker Engine + Compose plugin on clean Ubuntu (idempotent)
      run-tests.sh               # build the `test` image, run pytest with host Docker socket mounted
    migrations/
      env.py                      # Alembic async/sync env, reads effective_alembic_url
      versions/0001_initial.py    # batches / notifications / status_events schema + indexes
      versions/0002_status_event_seq.py  # adds status_events.seq identity for deterministic ordering
    docs/
      openapi.json                # committed OpenAPI snapshot
      postman_collection.json     # committed Postman collection
    notification_service/
      main.py                     # FastAPI app, lifespan (broker connect + declare topology), /health
      config.py                   # pydantic-settings Settings, ProviderMode, effective_* URL builders
      api/
        routes_notifications.py   # POST /notifications, GET /notifications/{id}
        routes_subscribers.py     # GET /subscribers/{id}/notifications (history)
        schemas.py                # pydantic request/response models + input validators
      db/
        models.py                 # ORM: Batch, Notification, StatusEvent; priority + domain constants
        repositories.py           # all SQL; never commits (caller owns the transaction)
        session.py                # async engine + sessionmaker; session_scope / get_session
      broker/
        rabbit.py                 # topology names, declare_topology, connect, passive queue getters
        publisher.py              # message construction: publish_work / _retry / _parking / _receipt
        consumer.py               # generic consume() runner: manual ack, Disposition, failure policy
      services/
        notification_service.py   # create_and_dispatch: idempotency + persist + publish
        idempotency.py            # Redis SETNX fast path (claim), redis client singleton
        rate_limiter.py           # per-channel sliding-window limiter (Redis Lua), allow()
        status.py                 # the one place status transitions + status_events writes happen
      providers/
        base.py                   # Provider protocol, SendResult, BaseMockProvider (deterministic)
        sms_mock.py / email_mock.py  # channel-specific subclasses (channel = "sms" / "email")
        factory.py                # ProviderFactory: one instance per channel, shared receipt emitter
      workers/
        dispatcher.py             # work-queue consumer: CAS + provider + rate limit + retry routing
        receipts.py               # receipts consumer: finalize delivered/rejected; poison vs transient
        sweeper.py                # reconciler: sweep_once() redrives stuck queued/sent rows
    tests/
      conftest.py                 # testcontainers infra + Harness (in-process app + real consumers)
      test_cas.py                 # unit: CAS transition (invariant 2)
      test_idempotency_service.py # unit: Redis claim exclusivity + TTL
      integration/
        test_flows.py             # the eight required scenarios + priority/rate-limit/sweeper
        test_failure_recovery.py  # poison vs transient faults, parking, bounded receipt redelivery
        test_shutdown.py          # graceful drain, clean dispatcher shutdown, unacked requeue
        test_validation.py        # 422 input-validation cases + dedup/trim behaviour
```

## 3. Request lifecycle

**Happy path — POST to delivered:**

1. `api/routes_notifications.create_notifications` receives `CreateNotificationRequest`
   (validated by `api/schemas.py`: channel/type literals, message non-blank ≤1000 chars,
   recipient_ids trimmed, de-duplicated preserving order, ≤`MAX_RECIPIENTS`). The optional
   `Idempotency-Key` header is trimmed and length-checked here.
2. The route calls `services/notification_service.create_and_dispatch` with the work exchange
   pulled from `request.app.state.exchange`.
3. Idempotency fast path: if a key was supplied, `services/idempotency.claim` does a Redis
   `SETNX`. A non-newly-claimed key triggers a lookup of the original batch via
   `repositories.get_batch_by_idempotency_key` → return it as a duplicate.
4. Persist (source of truth): `repositories.create_batch` inserts the `batches` header and one
   `queued` `notifications` row per recipient, plus a `queued` `status_events` row each (all in
   one flush). `create_and_dispatch` commits. A `UNIQUE(idempotency_key)` collision raises
   `IntegrityError`, which is caught and resolved to the existing batch.
5. After commit, `create_and_dispatch` publishes one message per recipient via
   `broker/publisher.publish_work` (priority from `priority_for_type`, persistent delivery mode).
6. The route returns `202` with `batch_id` and per-recipient `{id, subscriber_id, status:queued}`
   (`200` instead if the request was a duplicate).
7. **dispatcher** (`workers/dispatcher.make_handler` → `handle`) consumes the work message with
   `prefetch=1`. It reloads the row (`repositories.get_notification`), skips if not `queued`,
   applies the rate limiter for marketing, then runs the CAS via `services/status.cas_queued_to_sent`
   and commits — `sent` is durable before any provider call.
8. `providers/factory.ProviderFactory.for_channel` returns the channel's `BaseMockProvider`;
   `provider.send` is called outside the transaction. On acceptance the dispatcher stores the
   `provider_message_id` (`status.set_provider_message_id`) and acks.
9. The mock asynchronously emits a receipt (`BaseMockProvider._emit_after_delay` →
   `publisher.publish_receipt`) onto the receipts queue.
10. **receipts** (`workers/receipts.handle`) applies the receipt: `status.mark_delivered`
    (`sent → delivered`) or `status.mark_rejected` (`sent → rejected`), each a CAS + `status_events`
    write in one transaction, then commits and acks.

**GET history path:**

- `api/routes_subscribers.list_subscriber_notifications` → `repositories.list_notifications_for_subscriber`
  (ordered `created_at desc`, optional `status`, `limit`/`offset`). Each notification is serialized as
  `NotificationWithHistory` with its `status_events` mapped to `StatusEventOut`, ordered by `seq`.
- `api/routes_notifications.get_notification` → `repositories.get_notification(with_events=True)`
  for a single notification + its ordered history.

## 4. Message flow + broker topology

Declared idempotently by `broker/rabbit.declare_topology` (every process calls it on startup).

```
                         POST /notifications
                                 │  publish_work (priority 10 transactional / 1 marketing)
                                 ▼
   exchange: notifications (direct)
        │  rk=work
        ▼
   ┌──────────────────────────────┐
   │ notifications.work           │  x-max-priority: 10
   │ (priority queue)             │  DLX→parking backstop
   └──────────────────────────────┘
        │ consume prefetch=1  (dispatcher)
        │
        ├── accepted ───────────► provider emits receipt
        │                              │ publish_receipt (rk=receipt)
        │                              ▼
        │                        ┌───────────────────────────┐
        │                        │ notifications.receipts     │  DLX→parking backstop
        │                        └───────────────────────────┘
        │                              │ consume (receipts worker)
        │                              ▼  mark_delivered / mark_rejected
        │
        ├── transient fail ─► publish_retry via DLX
        │        │
        │        ▼  exchange: notifications.dlx (direct)
        │   ┌────────────────────────┐  rk=retry.5s / .30s / .120s
        │   │ notifications.retry.5s  │  x-message-ttl, DLX→notifications rk=work
        │   │ notifications.retry.30s │   (on TTL expiry, dead-letters back to work)
        │   │ notifications.retry.120s│
        │   └────────────────────────┘
        │        │ TTL elapses → dead-letter back to work exchange (rk=work)
        │        └────────────────────────────────► (re-enters work queue)
        │
        └── permanent / retries exhausted ─► publish_parking via DLX (rk=parking)
                                                 ▼
                                          ┌──────────────────────┐
                                          │ notifications.parking │  no consumer (terminal)
                                          └──────────────────────┘
```

Names live in `broker/rabbit.py`: `EXCHANGE="notifications"`, `DLX="notifications.dlx"`,
`WORK_QUEUE`, `RECEIPTS_QUEUE`, `PARKING_QUEUE`, routing keys `work`/`receipt`/`parking`, retry
suffixes `("5s","30s","120s")`. The work queue and receipts queue each carry
`x-dead-letter-*` arguments routing to parking, so any `nack(requeue=False)` lands in parking
rather than the void. Retry tiers also carry `x-max-priority` so priority survives a retry hop.
All exchanges/queues are durable; all messages are published with `delivery_mode=PERSISTENT`.

## 5. Requirement → implementation map

| Requirement | Implementation |
|---|---|
| **Priority (transactional overtakes marketing)** | Single `x-max-priority: 10` work queue (`rabbit.declare_topology`); priorities set in `publisher.priority_for_type` from `PRIORITY_TRANSACTIONAL=10` / `PRIORITY_MARKETING=1` (`db/models.py`); dispatcher consumes with `prefetch=1` (`consumer.consume(..., prefetch=1)` called by `dispatcher.run`). |
| **At-least-once** | Manual acks only, after the DB commit (`consumer.consume`); handler faults go through `dispatcher.make_failure_policy` / `receipts.make_failure_policy` which re-drive or park — never silent-drop. |
| **Business exactly-once** | `services/status.cas_queued_to_sent` — `UPDATE ... WHERE status='queued'`; only `rowcount==1` proceeds to the provider. Provider-key idempotency backstop in `providers/base.BaseMockProvider.send` (keyed on `notification_id`). |
| **Retry with backoff** | Fixed-TTL tiers `retry.5s/.30s/.120s` (`rabbit.declare_topology`); `dispatcher._tier_for_attempt` selects the tier; `publisher.publish_retry`; `status.revert_sent_to_queued_for_retry` bumps `retry_count`; exhausted at `MAX_RETRIES` → parking. |
| **Request idempotency (two layers)** | Layer 1 Redis `SETNX` (`services/idempotency.claim`); Layer 2 `UNIQUE(idempotency_key)` on `batches` (`db/models.Batch`), `IntegrityError` resolved in `services/notification_service.create_and_dispatch`. |
| **Persistence / durability** | Durable exchanges + queues, persistent messages (`rabbit.declare_topology`, `publisher._message` with `DeliveryMode.PERSISTENT`); Postgres rows are the source of truth; topology re-declared idempotently on every boot. |
| **Statuses + audit** | `services/status.py` is the only writer; every transition writes a `status_events` row in the same transaction (`_add_event`). |
| **Tests** | `tests/` — unit (`test_cas.py`, `test_idempotency_service.py`) + integration via testcontainers (`tests/integration/`). |
| **One-command deploy** | Root `docker-compose.yml` `include:`s `app/docker-compose.yml`; `migrate` one-shot runs `alembic upgrade head`; app services `depends_on` its completion + infra healthchecks. |
| **Provider mocks** | `providers/base.BaseMockProvider` (deterministic per `PROVIDER_MODE`, records calls, emits async receipts); `sms_mock`/`email_mock`; `factory.ProviderFactory` selects by channel. |

## 6. Reliability internals

- **CAS gate** — `status.cas_queued_to_sent` is the business exactly-once mechanism: a conditional
  `UPDATE ... WHERE id=:id AND status='queued'`. Only the winner (`rowcount==1`) calls the
  provider; losers (redeliveries) observe `status != 'queued'` in `dispatcher.handle` and ack-skip.
- **Ack-after-commit** — `broker/consumer.consume` acks only when the handler returns normally,
  i.e. after the handler has committed its state change. The dispatcher commits `sent` before the
  provider call; the receipts worker commits the terminal status before its ack.
- **Poison → parking** — undecodable bodies are caught in `consumer.consume` and
  `nack(requeue=False)` (DLX → parking). The receipts worker raises `PoisonReceipt` for
  structurally invalid receipts (missing/non-UUID id, unknown notification, unknown outcome);
  its failure policy parks them immediately rather than re-driving.
- **Tiered retry** — `dispatcher.handle` on a transient provider failure reverts the row
  (`status.revert_sent_to_queued_for_retry`), commits, and republishes to the next tier via
  `publisher.publish_retry`. `dispatcher.make_failure_policy` does the same for *unexpected*
  faults (DB/broker blips), returning `Disposition.ACK` after a successful republish so the
  original is acked, or `Disposition.PARK` if the republish itself fails.
- **The queued→sent flap** — when a transient failure happens after the CAS, the row is reverted
  `sent → queued` (`revert_sent_to_queued_for_retry`, guarded to `sent`/`queued` only so it can
  never resurrect a terminal row) and `retry_count` is bumped; the timed retry redelivery wins the
  CAS again. The retry attempt is itself recorded as a `queued` `status_events` row.
- **Bounded receipt redelivery** — `receipts.make_failure_policy` re-drives a transient
  apply-fault by republishing with an incremented `x-receipt-attempt` header, capped at
  `MAX_RECEIPT_REDELIVERIES`, with a small pacing sleep; past the cap it parks. This prevents a
  hot loop while a dependency recovers.
- **Sweeper reconciler** — `sweeper.sweep_once` finds rows stuck in `queued` past
  `SWEEPER_QUEUED_SECONDS` (the commit→publish dual-write window) and reverts rows stuck in `sent`
  past `SWEEPER_SENT_SECONDS` (a lost receipt), then republishes work. Redrives are safe because
  the CAS gate makes a redundant work message a no-op; thresholds exceed normal latency so live
  work isn't redriven prematurely.
- **Graceful shutdown** — `dispatcher.run` / `receipts.run` register SIGTERM/SIGINT handlers that
  set a stop event, cancel the consume task, and (dispatcher) `drain()` each provider's in-flight
  receipt tasks before closing the connection, so `docker compose down` unwinds cleanly instead of
  hard-killing mid-flight work. Tests inject their own stop event and skip signal registration.

## 7. Status model

Four statuses (`db/models.STATUSES`): `queued → sent → delivered`, with `rejected` as the terminal
failure state.

- **queued** — set on insert (`repositories.create_batch`), and again on a retry revert
  (`status.revert_sent_to_queued_for_retry`). Initial creation writes a `queued` event with detail
  `created`.
- **sent** — set by the CAS claim (`status.cas_queued_to_sent`). `sent` denotes "this dispatcher
  won the right to call the provider"; it is the exactly-once claim, not yet delivery confirmation.
- **delivered** — set by `status.mark_delivered` (CAS from `sent`) when a `delivered` receipt is
  applied.
- **rejected** — set by `status.mark_rejected` from a `rejected` receipt (`sent → rejected`) or by
  the dispatcher on a permanent failure / exhausted retries (`queued`/`sent → rejected`), with
  `last_error` recorded.

Every transition appends a `status_events` row in the same transaction (`status._add_event`,
invariant 7). Ordering uses the database-generated `seq` identity column (`db/models.StatusEvent.seq`,
migration `0002_status_event_seq`), not `created_at` — `created_at` is the transaction timestamp and
can tie across rapid transitions or a whole batch's creation events, whereas `seq` strictly increases
per insert and gives the history a deterministic total order. The `Notification.events` relationship
and the history endpoints order by `seq`.

## 8. Tests

Driven by `tests/conftest.py`: a session-scoped `_infra` fixture starts real Postgres / RabbitMQ /
Redis via **testcontainers** (Ryuk disabled, shrunk retry TTLs, large rate limits for determinism)
and runs Alembic migrations once. Each test gets a `Harness` that runs the FastAPI app in-process
(httpx `ASGITransport`) sharing one broker exchange, starts the dispatcher and receipts consumers as
background tasks against the real broker, and uses a deterministic mock provider whose recorded calls
the tests assert on. Only the provider is mocked; queueing, CAS, retries, rate limiting, and
idempotency all run against real infrastructure. Tables and queues are reset between tests.

- **`tests/test_cas.py`** (unit) — CAS flips exactly once, writes a `sent` `status_event`,
  `delivered` only allowed from `sent`.
- **`tests/test_idempotency_service.py`** (unit) — Redis `claim` is exclusive and sets the TTL.
- **`tests/integration/test_flows.py`** — the eight required scenarios and more: bulk accept → N
  `queued` rows + `202`; happy chain `queued→sent→delivered` with provider args asserted; rejected
  path; priority (immediate and backlog overtaking); idempotency returns the original batch;
  exactly-once under redelivery and during retry; retry-then-deliver and retries-exhausted-to-parking;
  history API ordering; email channel; `/health`; unknown-id 404; rate-limited marketing requeued
  (not dropped); transactional bypasses the limiter; sweeper redrives stuck `queued`.
- **`tests/integration/test_failure_recovery.py`** — unexpected work fault retries then parks;
  poison work message dead-lettered to parking; transient receipt fault requeued not lost; malformed
  receipt parked not looped; receipt transient fault bounded then parked.
- **`tests/integration/test_shutdown.py`** — provider `drain()` awaits in-flight receipts;
  `dispatcher.run` shuts down cleanly; unacked work is requeued on shutdown.
- **`tests/integration/test_validation.py`** — `422` cases (bad channel/type, missing/blank/over-long
  fields, too many recipients, bad idempotency key) plus recipient dedup and trim behaviour.

`scripts/run-tests.sh` builds the Dockerfile `test` stage and runs `pytest` with the host Docker
socket mounted, so testcontainers can spawn sibling containers reachable via `host.docker.internal`.

## 9. Config & deployment

**Config** — `config.py` (`pydantic-settings`), all values from env / `.env` (`.env.example` is the
committed template). Discrete `POSTGRES_*` / `RABBITMQ_*` / `REDIS_*` vars drive both the app
(asyncpg) and Alembic (psycopg) via computed `effective_database_url` / `effective_alembic_url` /
`effective_amqp_url` / `effective_redis_url`, unless an explicit `*_URL` override is set. Key tunables:
`PROVIDER_MODE` (`always_deliver`/`always_reject`/`transient_then_deliver`/`random`; compose default
`always_deliver` for a clean demo), `PROVIDER_RECEIPT_DELAY`, `MAX_RETRIES`,
`MAX_RECEIPT_REDELIVERIES`, `RETRY_TTL_{5S,30S,120S}_MS`, `RATE_LIMIT_{SMS,EMAIL}_PER_SEC`,
`IDEMPOTENCY_TTL_SECONDS`, `MAX_RECIPIENTS`, `SWEEPER_*`, `LOG_LEVEL`.

**Dockerfile** — three stages on `python:3.12-slim`:
- `builder` installs the package into an isolated `/opt/venv`.
- `runtime` copies that venv, adds a non-root `appuser`, copies `notification_service` + `migrations`
  + `alembic.ini`, and defaults to the uvicorn command (compose overrides it per service).
- `test` installs dev extras editable and defaults to `pytest -q` (used by `run-tests.sh`).

**Compose** — the repo-root `docker-compose.yml` is a thin entrypoint that `include:`s
`app/docker-compose.yml` (Compose v2.20+), so `docker compose up --build` works from the root with no
`cd`. The app compose defines: infra (`postgres`, `rabbitmq` with management UI, `redis`) each with
healthchecks; a one-shot `migrate` running `alembic upgrade head`; and `api` (uvicorn :8000, with a
`/health` healthcheck), `dispatcher`, `receipts`, `sweeper` — all the same `notification-service:latest`
image with different commands. Every app service `depends_on` `migrate` completing successfully and the
infra healthchecks. Shared env is defined once via the `x-app-env` YAML anchor.
