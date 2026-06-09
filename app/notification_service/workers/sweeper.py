"""Reconciler (sweeper) worker.

A periodic safety net for the two windows the happy path cannot close on its own:

1. **Stranded ``queued`` rows.** Batch creation commits the rows and *then*
   publishes one work message per recipient (a non-atomic dual-write). A crash —
   or the broker being briefly unavailable — between the commit and the publish
   leaves rows ``queued`` with no work message. The sweeper finds rows that have
   been ``queued`` longer than a threshold and republishes their work message.

2. **Stuck ``sent`` rows.** A delivery receipt can be lost (the mock emits it
   in-process; a real gateway webhook can simply fail to arrive). The sweeper
   finds rows that have been ``sent`` longer than a threshold, reverts them to
   ``queued`` and republishes — the dispatcher re-drives them and the provider
   re-emits the (idempotent) receipt.

Redrives are always safe: the CAS gate (invariant 2) means a redundant work
message for a row that already moved on simply loses the CAS and is skipped, so
the sweeper can never cause a double-send. The thresholds must exceed normal
processing + retry latency so live work is not redriven prematurely.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal

from aio_pika.abc import AbstractExchange

from notification_service.broker import publisher, rabbit
from notification_service.config import get_settings
from notification_service.db import repositories as repo
from notification_service.db.session import session_scope
from notification_service.services import status

logger = logging.getLogger(__name__)


async def sweep_once(
    exchange: AbstractExchange,
    *,
    queued_after_seconds: int | None = None,
    sent_after_seconds: int | None = None,
    batch_size: int | None = None,
) -> dict[str, int]:
    """Run a single reconciliation pass. Returns counts of redriven rows."""
    settings = get_settings()
    q_age = (
        settings.sweeper_queued_seconds if queued_after_seconds is None else queued_after_seconds
    )
    s_age = settings.sweeper_sent_seconds if sent_after_seconds is None else sent_after_seconds
    limit = settings.sweeper_batch_size if batch_size is None else batch_size

    # 1. Collect stuck rows and revert stuck 'sent' -> 'queued' in one transaction.
    async with session_scope() as session:
        stuck_queued = await repo.list_stuck_notifications(
            session, status="queued", older_than_seconds=q_age, limit=limit
        )
        stuck_sent = await repo.list_stuck_notifications(
            session, status="sent", older_than_seconds=s_age, limit=limit
        )
        for n in stuck_sent:
            await status.revert_sent_to_queued_for_retry(
                session,
                n.id,
                retry_count=n.retry_count,
                last_error="reconciler: re-driving row stuck in 'sent'",
            )
        await session.commit()

        # Snapshot ids/types before the session closes.
        queued = [(str(n.id), n.type, n.retry_count) for n in stuck_queued]
        sent = [(str(n.id), n.type, n.retry_count) for n in stuck_sent]

    # 2. Republish work OUTSIDE the transaction (publishes are confirmed I/O).
    for notification_id, type_, retry_count in queued:
        await publisher.publish_work(exchange, notification_id, type_, retry_count=retry_count)
    for notification_id, type_, retry_count in sent:
        await publisher.publish_work(exchange, notification_id, type_, retry_count=retry_count)

    counts = {"queued": len(queued), "sent": len(sent)}
    if counts["queued"] or counts["sent"]:
        logger.info("reconciler redrove %s queued, %s sent", counts["queued"], counts["sent"])
    return counts


async def run() -> None:
    settings = get_settings()
    logging.basicConfig(level=settings.log_level)

    connection = await rabbit.connect()
    channel = await connection.channel()
    topology = await rabbit.declare_topology(channel)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)

    logger.info(
        "Reconciler started; interval=%ss queued>%ss sent>%ss",
        settings.sweeper_interval_seconds,
        settings.sweeper_queued_seconds,
        settings.sweeper_sent_seconds,
    )
    try:
        while not stop.is_set():
            try:
                await sweep_once(topology.exchange)
            except Exception:  # never let one bad pass kill the loop
                logger.exception("reconciler pass failed; continuing")
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=settings.sweeper_interval_seconds)
    finally:
        await connection.close()
        logger.info("Reconciler stopped cleanly")


if __name__ == "__main__":
    asyncio.run(run())
