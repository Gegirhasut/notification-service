"""Receipts worker.

Consumes provider delivery receipts and performs the terminal transition
**sent -> delivered | rejected** (each as a CAS + status_event in one
transaction). Transitions are idempotent: a duplicate receipt finds the row no
longer in 'sent', the CAS is a no-op, and the message is still acked.
"""

from __future__ import annotations

import asyncio
import logging
import uuid

from aio_pika.abc import AbstractIncomingMessage

from notification_service.broker import rabbit
from notification_service.broker.consumer import consume
from notification_service.config import get_settings
from notification_service.db import repositories as repo
from notification_service.db.session import session_scope
from notification_service.services import status

logger = logging.getLogger(__name__)

_PREFETCH = 16


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


async def run() -> None:
    settings = get_settings()
    logging.basicConfig(level=settings.log_level)

    connection = await rabbit.connect()
    channel = await connection.channel()
    await rabbit.declare_topology(channel)
    receipts_queue = await rabbit.get_receipts_queue(channel)

    logger.info("Receipts worker started; consuming %s", rabbit.RECEIPTS_QUEUE)
    try:
        await consume(channel, receipts_queue, handle, prefetch=_PREFETCH)
    finally:
        await connection.close()


if __name__ == "__main__":
    asyncio.run(run())
