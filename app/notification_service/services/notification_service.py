"""Batch-creation business logic: idempotency + persist + publish.

Ties the two idempotency layers (invariant 4) together and then publishes one
work message per recipient. Publishing happens only *after* the DB commit, so a
duplicate request never produces orphan messages and a failed insert never
publishes.
"""

from __future__ import annotations

from dataclasses import dataclass

from aio_pika.abc import AbstractExchange
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from notification_service.broker import publisher
from notification_service.db import repositories as repo
from notification_service.db.models import Batch, Notification
from notification_service.services import idempotency


@dataclass(slots=True)
class BatchResult:
    batch: Batch
    notifications: list[Notification]
    duplicate: bool


async def create_and_dispatch(
    session: AsyncSession,
    exchange: AbstractExchange,
    *,
    channel: str,
    type_: str,
    body: str,
    recipient_ids: list[str],
    idempotency_key: str | None,
) -> BatchResult:
    # --- Idempotency fast path (Redis) ---
    if idempotency_key is not None:
        newly_claimed = await idempotency.claim(idempotency_key)
        if not newly_claimed:
            existing = await repo.get_batch_by_idempotency_key(session, idempotency_key)
            if existing is not None:
                return BatchResult(existing, list(existing.notifications), duplicate=True)
            # Redis claim present but no batch yet: a concurrent in-flight request,
            # or a stale key. Fall through and let the DB UNIQUE constraint decide.

    # --- Persist (source of truth) ---
    try:
        batch, notifications = await repo.create_batch(
            session,
            channel=channel,
            type_=type_,
            body=body,
            idempotency_key=idempotency_key,
            recipient_ids=recipient_ids,
        )
        await session.commit()
    except IntegrityError:
        # Lost the race on the UNIQUE(idempotency_key) constraint -> duplicate.
        await session.rollback()
        existing = await repo.get_batch_by_idempotency_key(session, idempotency_key)
        if existing is None:  # pragma: no cover - constraint without a row is impossible
            raise
        return BatchResult(existing, list(existing.notifications), duplicate=True)

    # Capture ids before publishing (objects survive commit; expire_on_commit=False).
    published = [(str(n.id), n.type) for n in notifications]
    batch_id = batch.id

    # --- Publish work (after commit) ---
    for notification_id, type_value in published:
        await publisher.publish_work(exchange, notification_id, type_value)

    # Re-fetch a clean batch with notifications for the response.
    result_batch = await repo.get_batch(session, batch_id)
    assert result_batch is not None
    return BatchResult(result_batch, list(result_batch.notifications), duplicate=False)
