"""Dispatcher worker.

Consumes the priority work queue with **prefetch=1** (invariant 1), performs the
atomic **queued -> sent** compare-and-set (invariant 2) before calling the
provider, and routes outcomes:

- accepted        -> stays 'sent'; provider will emit a receipt that the receipts
                     consumer finalizes to delivered/rejected.
- transient fail  -> revert 'sent' -> 'queued', bump retry_count, republish to the
                     next fixed-TTL retry tier (5s -> 30s -> 120s). Retries
                     exhausted -> rejected + parking (invariant 5).
- permanent fail  -> rejected + parking immediately.
- rate limited    -> requeue to the 5s retry tier, row stays 'queued', never drop.

Acks happen only after the relevant DB commit (invariant 3). The generic consumer
runner acks on normal handler return; on an *unexpected* fault it consults
``make_failure_policy`` below, which re-drives the work through the retry tiers
(then parking) so nothing is ever silently discarded.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
import uuid

from aio_pika.abc import AbstractExchange, AbstractIncomingMessage

from notification_service.broker import publisher, rabbit
from notification_service.broker.consumer import Disposition, consume
from notification_service.config import Settings, get_settings
from notification_service.db import repositories as repo
from notification_service.db.session import session_scope
from notification_service.providers.factory import ProviderFactory
from notification_service.services import rate_limiter, status

logger = logging.getLogger(__name__)


def _tier_for_attempt(attempt: int) -> str:
    """Map a 1-based retry attempt to a tier suffix, clamped to the last tier."""
    idx = min(attempt - 1, len(rabbit.RETRY_SUFFIXES) - 1)
    return rabbit.RETRY_SUFFIXES[idx]


def make_handler(
    exchange: AbstractExchange,
    dlx: AbstractExchange,
    factory: ProviderFactory,
    settings: Settings,
):
    async def handle(payload: dict, message: AbstractIncomingMessage) -> None:
        notification_id = payload["notification_id"]
        nid = uuid.UUID(notification_id)
        retry_count = int(payload.get("retry_count", 0))

        # 1. Load the row (source of truth).
        async with session_scope() as session:
            notification = await repo.get_notification(session, nid)
            if notification is None:
                logger.warning("Unknown notification %s; acking", notification_id)
                return

            type_ = notification.type
            channel = notification.channel
            recipient = notification.subscriber_id
            body = notification.body

            # Redelivery of an already-processed message: ack & skip (invariant 2).
            if notification.status != "queued":
                logger.debug("notification %s status=%s; skip", nid, notification.status)
                return

            # 2. Rate limit BEFORE the CAS so the row stays 'queued' and can be
            #    re-attempted from the 5s tier (never dropped).
            if not await rate_limiter.allow(channel):
                logger.info("rate limited on %s; requeue %s to 5s tier", channel, nid)
                await publisher.publish_retry(
                    dlx, notification_id, type_, suffix="5s", retry_count=retry_count
                )
                return

            # 3. CAS queued -> sent. Only the winner proceeds to the provider.
            won = await status.cas_queued_to_sent(session, nid)
            if not won:
                await session.rollback()
                logger.debug("lost CAS for %s; skip", nid)
                return
            await session.commit()  # 'sent' is durable before we call the provider

        # 4. Call the provider OUTSIDE the transaction (no lock held over I/O).
        provider = factory.for_channel(channel)
        result = await provider.send(notification_id, recipient, body)

        if result.accepted:
            async with session_scope() as session:
                await status.set_provider_message_id(session, nid, result.provider_message_id or "")
                await session.commit()
            return  # ack; the receipts consumer will finalize delivered/rejected

        # 5. Provider rejected at acceptance.
        error = result.error or "provider rejected"
        if result.transient and retry_count < settings.max_retries:
            attempt = retry_count + 1
            async with session_scope() as session:
                await status.revert_sent_to_queued_for_retry(
                    session, nid, retry_count=attempt, last_error=error
                )
                await session.commit()
            await publisher.publish_retry(
                dlx,
                notification_id,
                type_,
                suffix=_tier_for_attempt(attempt),
                retry_count=attempt,
            )
            logger.info("transient fail for %s; retry %s queued", nid, attempt)
            return

        # Permanent failure or retries exhausted -> rejected + parking.
        async with session_scope() as session:
            await status.mark_rejected(
                session, nid, last_error=error, from_statuses=("sent", "queued")
            )
            await session.commit()
        await publisher.publish_parking(dlx, notification_id, type_, last_error=error)
        logger.info("notification %s rejected to parking: %s", nid, error)

    return handle


def make_failure_policy(dlx: AbstractExchange, settings: Settings):
    """Recovery for *unexpected* dispatcher faults (DB/broker blips).

    Such faults are not poison messages — discarding them would strand the row in
    'queued'/'sent' and lose the work (an at-least-once violation). Instead we
    treat them as transient: bump retry_count and republish through the same
    fixed-TTL retry tiers, and on exhausting max retries dead-letter to parking
    and mark the row 'rejected'. DB writes are best-effort (the fault may *be* the
    DB); the broker republish is what guarantees the work is not lost.
    """

    async def on_failure(payload: dict, message: AbstractIncomingMessage, exc: Exception):
        notification_id = payload["notification_id"]
        type_ = payload.get("type", "marketing")
        retry_count = int(payload.get("retry_count", 0))
        nid = uuid.UUID(notification_id)
        error = f"dispatch fault: {type(exc).__name__}: {exc}"[:500]

        try:
            if retry_count < settings.max_retries:
                attempt = retry_count + 1
                try:
                    async with session_scope() as session:
                        await status.revert_sent_to_queued_for_retry(
                            session, nid, retry_count=attempt, last_error=error
                        )
                        await session.commit()
                except Exception:
                    logger.exception("could not record retry state for %s", nid)
                await publisher.publish_retry(
                    dlx,
                    notification_id,
                    type_,
                    suffix=_tier_for_attempt(attempt),
                    retry_count=attempt,
                )
                logger.warning("unexpected fault on %s; retry %s queued: %s", nid, attempt, error)
                return Disposition.ACK

            # Retries exhausted -> parking + rejected.
            try:
                async with session_scope() as session:
                    await status.mark_rejected(
                        session, nid, last_error=error, from_statuses=("sent", "queued")
                    )
                    await session.commit()
            except Exception:
                logger.exception("could not mark rejected for %s", nid)
            await publisher.publish_parking(dlx, notification_id, type_, last_error=error)
            logger.warning("unexpected fault on %s; retries exhausted -> parking: %s", nid, error)
            return Disposition.ACK
        except Exception:
            # The republish itself failed (broker down). Do NOT ack: park as the
            # backstop so the message is recoverable rather than discarded.
            logger.exception("recovery republish failed for %s; parking", nid)
            return Disposition.PARK

    return on_failure


async def run(stop: asyncio.Event | None = None) -> None:
    settings = get_settings()
    logging.basicConfig(level=settings.log_level)

    connection = await rabbit.connect()
    consume_channel = await connection.channel()
    publish_channel = await connection.channel()

    topology = await rabbit.declare_topology(publish_channel)
    work_queue = await rabbit.get_work_queue(consume_channel)

    async def emit_receipt(nid, outcome, provider_message_id, detail):
        await publisher.publish_receipt(
            topology.exchange,
            nid,
            outcome=outcome,
            provider_message_id=provider_message_id,
            detail=detail,
        )

    factory = ProviderFactory(
        mode=settings.provider_mode,
        emit_receipt=emit_receipt,
        receipt_delay=settings.provider_receipt_delay,
    )

    handler = make_handler(topology.exchange, topology.dlx, factory, settings)
    on_failure = make_failure_policy(topology.dlx, settings)

    # Graceful shutdown: stop consuming on SIGTERM/SIGINT, drain in-flight provider
    # receipt tasks, then close the connection so `docker compose down` unwinds
    # cleanly instead of hard-killing mid-flight work. Tests inject their own stop
    # event (and skip signal registration).
    own_signals = stop is None
    if stop is None:
        stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    if own_signals:
        for sig in (signal.SIGTERM, signal.SIGINT):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, stop.set)

    logger.info("Dispatcher started; consuming %s with prefetch=1", rabbit.WORK_QUEUE)
    consume_task = asyncio.create_task(
        consume(consume_channel, work_queue, handler, prefetch=1, on_failure=on_failure)
    )
    stop_task = asyncio.create_task(stop.wait())
    try:
        done, _ = await asyncio.wait({consume_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
        if consume_task in done:  # consumer exited on its own (surface any error)
            consume_task.result()
    finally:
        for task in (consume_task, stop_task):
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        for provider in factory.providers():
            with contextlib.suppress(Exception):
                await provider.drain()
        await connection.close()
        if own_signals:
            for sig in (signal.SIGTERM, signal.SIGINT):
                with contextlib.suppress(NotImplementedError, ValueError):
                    loop.remove_signal_handler(sig)
        logger.info("Dispatcher stopped cleanly")


if __name__ == "__main__":
    asyncio.run(run())
