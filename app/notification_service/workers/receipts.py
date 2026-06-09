"""Receipts worker.

Consumes provider delivery receipts and performs the terminal transition
**sent -> delivered | rejected** (each as a CAS + status_event in one
transaction). Transitions are idempotent: a duplicate receipt finds the row no
longer in 'sent', the CAS is a no-op, and the message is still acked.

Two failure classes are handled distinctly, mirroring the work queue's poison
handling so nothing can hot-loop or be silently dropped:

- **Poison** (undecodable body, missing/invalid ``notification_id``, unknown id,
  unknown outcome) can *never* be applied, so it is dead-lettered to parking
  immediately — never requeued. Undecodable bodies are caught by the generic
  consumer; the structural-validity checks here raise ``PoisonReceipt``.
- **Transient** apply faults (e.g. a Postgres blip) must not drop the receipt or
  strand the row in 'sent', so they are re-driven a *bounded* number of times
  (``MAX_RECEIPT_REDELIVERIES``) by republishing with an incremented attempt
  counter, then parked. A small pacing delay avoids a hot loop while the
  dependency recovers.
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
from notification_service.services import status

logger = logging.getLogger(__name__)

_PREFETCH = 16
_REQUEUE_BACKOFF_SECONDS = 0.5


class PoisonReceipt(Exception):
    """A receipt that can never be applied (bad shape / unknown id or outcome).

    Distinct from a transient apply fault: poison is dead-lettered to parking
    immediately rather than re-driven, so it cannot hot-loop the consumer.
    """


async def handle(payload: dict, message: AbstractIncomingMessage) -> None:
    raw_id = payload.get("notification_id")
    if not raw_id:
        raise PoisonReceipt("receipt missing notification_id")
    try:
        nid = uuid.UUID(str(raw_id))
    except (ValueError, TypeError) as exc:
        raise PoisonReceipt(f"receipt notification_id is not a UUID: {raw_id!r}") from exc

    outcome = payload.get("outcome")
    detail = payload.get("detail")

    async with session_scope() as session:
        notification = await repo.get_notification(session, nid)
        if notification is None:
            raise PoisonReceipt(f"receipt for unknown notification {nid}")

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
            raise PoisonReceipt(f"unknown receipt outcome {outcome!r} for {nid}")

        await session.commit()


def make_failure_policy(exchange: AbstractExchange, settings: Settings):
    """Failure policy for the receipts consumer.

    Poison -> park immediately. Transient -> republish with an incremented
    attempt counter (bounded by ``MAX_RECEIPT_REDELIVERIES``) and ack the
    original; once the cap is exceeded, park. This bounds redelivery instead of
    requeueing forever.
    """

    async def on_failure(
        payload: dict, message: AbstractIncomingMessage, exc: Exception
    ) -> Disposition:
        notification_id = payload.get("notification_id")

        # Poison: cannot ever be applied -> dead-letter to parking (never loop).
        if isinstance(exc, PoisonReceipt):
            logger.warning("poison receipt for %s; parking: %s", notification_id, exc)
            return Disposition.PARK

        headers = message.headers or {}
        attempt = int(headers.get("x-receipt-attempt", 0)) + 1
        if attempt > settings.max_receipt_redeliveries:
            logger.warning(
                "receipt for %s exceeded %s redeliveries; parking: %s",
                notification_id,
                settings.max_receipt_redeliveries,
                exc,
            )
            return Disposition.PARK

        # Transient: re-drive a bounded number of times. Republish with the bumped
        # counter and ack the original; pace it so a dependency outage can't spin.
        logger.warning(
            "receipt apply failed for %s; re-driving (attempt %s/%s): %s",
            notification_id,
            attempt,
            settings.max_receipt_redeliveries,
            exc,
        )
        await asyncio.sleep(_REQUEUE_BACKOFF_SECONDS)
        await publisher.publish_receipt(
            exchange,
            notification_id,
            outcome=payload.get("outcome"),
            provider_message_id=payload.get("provider_message_id"),
            detail=payload.get("detail"),
            attempt=attempt,
        )
        return Disposition.ACK

    return on_failure


async def run(stop: asyncio.Event | None = None) -> None:
    settings = get_settings()
    logging.basicConfig(level=settings.log_level)

    connection = await rabbit.connect()
    channel = await connection.channel()
    topology = await rabbit.declare_topology(channel)
    receipts_queue = await rabbit.get_receipts_queue(channel)

    own_signals = stop is None
    if stop is None:
        stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    if own_signals:
        for sig in (signal.SIGTERM, signal.SIGINT):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, stop.set)

    on_failure = make_failure_policy(topology.exchange, settings)
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
