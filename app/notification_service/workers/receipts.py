"""Receipts worker.

Consumes provider delivery receipts and performs the terminal transition
**sent -> delivered | rejected** (each as a CAS + status_event in one
transaction). Transitions are idempotent: a duplicate receipt finds the row no
longer in 'sent', the CAS is a no-op, and the message is still acked.

An *unexpected* fault here (e.g. a Postgres blip) must not drop the receipt or
strand the row in 'sent', so the failure policy requeues for another attempt
rather than discarding. A small pacing delay avoids a hot redelivery loop while
the dependency recovers; the queue's dead-letter route to parking is the ultimate
backstop.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
import uuid

from aio_pika.abc import AbstractIncomingMessage

from notification_service.broker import rabbit
from notification_service.broker.consumer import Disposition, consume
from notification_service.config import get_settings
from notification_service.db import repositories as repo
from notification_service.db.session import session_scope
from notification_service.services import status

logger = logging.getLogger(__name__)

_PREFETCH = 16
_REQUEUE_BACKOFF_SECONDS = 0.5


async def handle(payload: dict, message: AbstractIncomingMessage) -> None:
    notification_id = payload["notification_id"]
    nid = uuid.UUID(notification_id)
    outcome = payload.get("outcome")
    detail = payload.get("detail")

    async with session_scope() as session:
        notification = await repo.get_notification(session, nid)
        if notification is None:
            logger.warning("receipt for unknown notification %s; acking", nid)
            return

        if outcome == "delivered":
            await status.mark_delivered(session, nid, detail=detail)
        elif outcome == "rejected":
            await status.mark_rejected(
                session,
                nid,
                last_error=detail or "rejected by provider",
                from_statuses=("sent",),
            )
        else:
            logger.warning("unknown receipt outcome %r for %s; acking", outcome, nid)
            return

        await session.commit()


async def on_failure(
    payload: dict, message: AbstractIncomingMessage, exc: Exception
) -> Disposition:
    """Transient fault applying a receipt: requeue so the receipt is not lost."""
    logger.warning(
        "receipt apply failed for %s; requeueing: %s", payload.get("notification_id"), exc
    )
    # Pace the redelivery so a persistent dependency outage doesn't hot-loop.
    await asyncio.sleep(_REQUEUE_BACKOFF_SECONDS)
    return Disposition.REQUEUE


async def run(stop: asyncio.Event | None = None) -> None:
    settings = get_settings()
    logging.basicConfig(level=settings.log_level)

    connection = await rabbit.connect()
    channel = await connection.channel()
    await rabbit.declare_topology(channel)
    receipts_queue = await rabbit.get_receipts_queue(channel)

    own_signals = stop is None
    if stop is None:
        stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    if own_signals:
        for sig in (signal.SIGTERM, signal.SIGINT):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, stop.set)

    logger.info("Receipts worker started; consuming %s", rabbit.RECEIPTS_QUEUE)
    consume_task = asyncio.create_task(
        consume(channel, receipts_queue, handle, prefetch=_PREFETCH, on_failure=on_failure)
    )
    stop_task = asyncio.create_task(stop.wait())
    try:
        done, _ = await asyncio.wait({consume_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
        if consume_task in done:
            consume_task.result()
    finally:
        for task in (consume_task, stop_task):
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        await connection.close()
        if own_signals:
            for sig in (signal.SIGTERM, signal.SIGINT):
                with contextlib.suppress(NotImplementedError, ValueError):
                    loop.remove_signal_handler(sig)
        logger.info("Receipts worker stopped cleanly")


if __name__ == "__main__":
    asyncio.run(run())
