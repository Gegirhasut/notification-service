"""Test harness: real Postgres / RabbitMQ / Redis via testcontainers.

A single set of containers is started once per session. Each test gets a fresh
``Harness`` that:

* runs the FastAPI app in-process (httpx ASGITransport) sharing one broker
  exchange,
* runs the dispatcher and receipts consumers as background tasks against the
  real broker,
* uses a deterministic mock provider whose recorded calls tests assert on.

Only the provider is mocked; the queueing, CAS, retries and idempotency all run
against real infrastructure. Tables and queues are reset between tests.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path

import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer
from testcontainers.rabbitmq import RabbitMqContainer
from testcontainers.redis import RedisContainer

APP_DIR = Path(__file__).resolve().parent.parent


# --------------------------------------------------------------------------- #
# Session-scoped infrastructure
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="session", autouse=True)
def _infra() -> AsyncIterator[None]:
    os.environ.setdefault("TESTCONTAINERS_RYUK_DISABLED", "true")

    with (
        PostgresContainer("postgres:16") as pg,
        RabbitMqContainer("rabbitmq:3.13-management") as mq,
        RedisContainer("redis:7") as rd,
    ):
        os.environ.update(
            {
                "POSTGRES_USER": pg.username,
                "POSTGRES_PASSWORD": pg.password,
                "POSTGRES_DB": pg.dbname,
                "POSTGRES_HOST": pg.get_container_host_ip(),
                "POSTGRES_PORT": str(pg.get_exposed_port(5432)),
                "RABBITMQ_DEFAULT_USER": "guest",
                "RABBITMQ_DEFAULT_PASS": "guest",
                "RABBITMQ_HOST": mq.get_container_host_ip(),
                "RABBITMQ_PORT": str(mq.get_exposed_port(5672)),
                "REDIS_HOST": rd.get_container_host_ip(),
                "REDIS_PORT": str(rd.get_exposed_port(6379)),
                "REDIS_DB": "0",
                # Deterministic, fast behaviour for tests.
                "PROVIDER_RECEIPT_DELAY": "0",
                "MAX_RETRIES": "3",
                "RATE_LIMIT_SMS_PER_SEC": "100000",
                "RATE_LIMIT_EMAIL_PER_SEC": "100000",
                # Shrink the retry tiers so the retry cycle runs in milliseconds.
                "RETRY_TTL_5S_MS": "300",
                "RETRY_TTL_30S_MS": "300",
                "RETRY_TTL_120S_MS": "300",
            }
        )

        from notification_service.config import get_settings

        get_settings.cache_clear()

        _run_migrations()
        yield


def _run_migrations() -> None:
    from alembic import command
    from alembic.config import Config

    cfg = Config(str(APP_DIR / "alembic.ini"))
    cfg.set_main_option("script_location", str(APP_DIR / "migrations"))
    command.upgrade(cfg, "head")


# --------------------------------------------------------------------------- #
# Harness
# --------------------------------------------------------------------------- #
class Harness:
    def __init__(
        self,
        *,
        mode: str = "always_deliver",
        transient_attempts: int | None = None,
        max_retries: int | None = None,
        start_consumers: bool = True,
        rate_limit_sms_per_sec: int | None = None,
        rate_limit_email_per_sec: int | None = None,
        max_recipients: int | None = None,
        max_receipt_redeliveries: int | None = None,
    ) -> None:
        from notification_service.config import ProviderMode

        self.mode = ProviderMode(mode)
        self.transient_attempts = transient_attempts
        self.max_retries = max_retries
        self._start_consumers = start_consumers
        self.rate_limit_sms_per_sec = rate_limit_sms_per_sec
        self.rate_limit_email_per_sec = rate_limit_email_per_sec
        self.max_recipients = max_recipients
        self.max_receipt_redeliveries = max_receipt_redeliveries
        self._tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        import httpx

        from notification_service.broker import publisher, rabbit
        from notification_service.config import get_settings
        from notification_service.main import app
        from notification_service.providers.factory import ProviderFactory

        # Set every per-harness tunable explicitly (don't rely on leftover env
        # from a previous harness) so tests are isolated, then refresh the cache.
        os.environ["MAX_RETRIES"] = str(self.max_retries if self.max_retries is not None else 3)
        os.environ["RATE_LIMIT_SMS_PER_SEC"] = str(self.rate_limit_sms_per_sec or 100_000)
        os.environ["RATE_LIMIT_EMAIL_PER_SEC"] = str(self.rate_limit_email_per_sec or 100_000)
        os.environ["MAX_RECIPIENTS"] = str(
            self.max_recipients if self.max_recipients is not None else 1000
        )
        os.environ["MAX_RECEIPT_REDELIVERIES"] = str(
            self.max_receipt_redeliveries if self.max_receipt_redeliveries is not None else 5
        )
        get_settings.cache_clear()
        self.settings = get_settings()

        await self._reset_state()

        self.connection = await rabbit.connect()
        self.consume_channel = await self.connection.channel()
        self.receipts_channel = await self.connection.channel()
        self.publish_channel = await self.connection.channel()
        self.topology = await rabbit.declare_topology(self.publish_channel)
        await self._purge_queues()

        async def emit_receipt(nid, outcome, provider_message_id, detail):
            await publisher.publish_receipt(
                self.topology.exchange,
                nid,
                outcome=outcome,
                provider_message_id=provider_message_id,
                detail=detail,
            )

        self.factory = ProviderFactory(
            mode=self.mode,
            emit_receipt=emit_receipt,
            receipt_delay=0.0,
            transient_attempts=self.transient_attempts,
        )

        # Wire the in-process API to the same broker exchange + connection (we
        # skip the FastAPI lifespan, so set what /health reads ourselves).
        app.state.exchange = self.topology.exchange
        app.state.broker_connection = self.connection
        self.app = app
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        )

        if self._start_consumers:
            await self.start_consumers()

    async def start_consumers(self) -> None:
        from notification_service.broker import rabbit
        from notification_service.broker.consumer import consume
        from notification_service.workers import receipts as receipts_worker
        from notification_service.workers.dispatcher import make_failure_policy, make_handler

        work_queue = await rabbit.get_work_queue(self.consume_channel)
        receipts_queue = await rabbit.get_receipts_queue(self.receipts_channel)

        # Wire the same recovery policies the production workers use, so tests
        # exercise the real "never silently drop" behaviour (A1).
        handler = make_handler(
            self.topology.exchange, self.topology.dlx, self.factory, self.settings
        )
        work_on_failure = make_failure_policy(self.topology.dlx, self.settings)
        self._tasks.append(
            asyncio.create_task(
                consume(
                    self.consume_channel,
                    work_queue,
                    handler,
                    prefetch=1,
                    on_failure=work_on_failure,
                )
            )
        )
        receipts_on_failure = receipts_worker.make_failure_policy(
            self.topology.exchange, self.settings
        )
        self._tasks.append(
            asyncio.create_task(
                consume(
                    self.receipts_channel,
                    receipts_queue,
                    receipts_worker.handle,
                    prefetch=16,
                    on_failure=receipts_on_failure,
                )
            )
        )
        # Give the consumers a moment to register before tests publish.
        await asyncio.sleep(0.2)

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        await self.client.aclose()
        await self.connection.close()

    # --- helpers ---------------------------------------------------------- #
    async def _reset_state(self) -> None:
        from sqlalchemy import text

        from notification_service.db.session import dispose_engine, get_engine
        from notification_service.services import idempotency

        await dispose_engine()
        await idempotency.close()

        async with get_engine().begin() as conn:
            await conn.execute(
                text("TRUNCATE status_events, notifications, batches RESTART IDENTITY CASCADE")
            )
        await idempotency.get_redis().flushdb()

    async def _purge_queues(self) -> None:
        from notification_service.broker import rabbit

        names = [
            rabbit.WORK_QUEUE,
            rabbit.RECEIPTS_QUEUE,
            rabbit.PARKING_QUEUE,
            *(rabbit.retry_queue_name(s) for s in rabbit.RETRY_SUFFIXES),
        ]
        for name in names:
            queue = await self.publish_channel.declare_queue(name, passive=True)
            await queue.purge()

    async def publish_work_direct(self, notification_id: str, type_: str) -> None:
        """Publish a raw work message (used to simulate redelivery / ordering)."""
        from notification_service.broker import publisher

        await publisher.publish_work(self.topology.exchange, notification_id, type_)

    async def publish_raw_work(self, body: bytes) -> None:
        """Publish an arbitrary (e.g. undecodable) body to the work queue."""
        import aio_pika

        from notification_service.broker import rabbit

        msg = aio_pika.Message(body=body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT)
        await self.topology.exchange.publish(msg, routing_key=rabbit.ROUTING_WORK)

    async def publish_raw_receipt(self, payload: dict) -> None:
        """Publish an arbitrary (e.g. malformed) JSON receipt to the receipts queue."""
        import json

        import aio_pika

        from notification_service.broker import rabbit

        msg = aio_pika.Message(
            body=json.dumps(payload).encode(),
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )
        await self.topology.exchange.publish(msg, routing_key=rabbit.ROUTING_RECEIPT)

    async def get_notification(self, notification_id: str):
        from notification_service.db import repositories as repo
        from notification_service.db.session import session_scope

        async with session_scope() as session:
            return await repo.get_notification(
                session, uuid.UUID(notification_id), with_events=True
            )

    async def history(self, notification_id: str) -> list[str]:
        notification = await self.get_notification(notification_id)
        return [e.status for e in notification.events]

    async def wait_for_status(
        self, notification_id: str, statuses: set[str], timeout: float = 15.0
    ) -> str:
        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            notification = await self.get_notification(notification_id)
            if notification is not None and notification.status in statuses:
                return notification.status
            await asyncio.sleep(0.1)
        notification = await self.get_notification(notification_id)
        current = notification.status if notification else "missing"
        raise AssertionError(f"{notification_id} did not reach {statuses}; current={current}")

    async def queue_message_count(self, queue_name: str) -> int:
        queue = await self.publish_channel.declare_queue(queue_name, passive=True)
        return queue.declaration_result.message_count

    async def wait_until(self, predicate, *, timeout: float = 15.0, interval: float = 0.05):
        """Poll an async predicate until it returns truthy, or raise on timeout.

        Used instead of fixed sleeps so tests wait on an actual observable
        (queue depth, status, provider call count) rather than a guessed delay.
        """
        deadline = asyncio.get_event_loop().time() + timeout
        last = None
        while asyncio.get_event_loop().time() < deadline:
            last = await predicate()
            if last:
                return last
            await asyncio.sleep(interval)
        raise AssertionError(f"condition not met within {timeout}s (last={last!r})")


HarnessFactory = Callable[..., Awaitable[Harness]]


@pytest_asyncio.fixture
async def harness_factory() -> AsyncIterator[HarnessFactory]:
    harnesses: list[Harness] = []

    async def _build(**kwargs) -> Harness:
        h = Harness(**kwargs)
        await h.start()
        harnesses.append(h)
        return h

    yield _build

    for h in harnesses:
        await h.stop()

    from notification_service.db.session import dispose_engine
    from notification_service.services import idempotency

    await dispose_engine()
    await idempotency.close()


@pytest_asyncio.fixture
async def db_session() -> AsyncIterator:
    """A clean DB session for unit tests (tables truncated, singletons reset)."""
    from sqlalchemy import text

    from notification_service.db.session import dispose_engine, get_engine, session_scope
    from notification_service.services import idempotency

    await dispose_engine()
    await idempotency.close()

    async with get_engine().begin() as conn:
        await conn.execute(
            text("TRUNCATE status_events, notifications, batches RESTART IDENTITY CASCADE")
        )
    await idempotency.get_redis().flushdb()

    async with session_scope() as session:
        yield session

    await dispose_engine()
    await idempotency.close()
