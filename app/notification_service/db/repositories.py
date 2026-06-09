"""Data-access layer. All SQL lives here; services call these functions.

Repositories never commit — the calling service/worker owns the transaction
boundary (so commit can be sequenced with broker acks).
"""

from __future__ import annotations

import uuid
from datetime import timedelta

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from notification_service.db.models import Batch, Notification, StatusEvent


async def create_batch(
    session: AsyncSession,
    *,
    channel: str,
    type_: str,
    body: str,
    idempotency_key: str | None,
    recipient_ids: list[str],
) -> tuple[Batch, list[Notification]]:
    """Insert a batch header + one queued notification per recipient (flush only).

    Returns the batch and its notifications (ids assigned by the flush).
    """
    batch = Batch(
        channel=channel,
        type=type_,
        body=body,
        idempotency_key=idempotency_key,
        total=len(recipient_ids),
    )
    session.add(batch)
    await session.flush()  # assign batch.id

    notifications = [
        Notification(
            batch_id=batch.id,
            subscriber_id=subscriber_id,
            channel=channel,
            type=type_,
            body=body,
            status="queued",
        )
        for subscriber_id in recipient_ids
    ]
    session.add_all(notifications)
    await session.flush()  # assign notification ids

    # Invariant 7: the initial queued state is itself a status change and must
    # have a status_events row, written in the same transaction as the insert.
    session.add_all(
        StatusEvent(notification_id=n.id, status="queued", detail="created") for n in notifications
    )
    await session.flush()
    return batch, notifications


async def get_batch_by_idempotency_key(session: AsyncSession, idempotency_key: str) -> Batch | None:
    result = await session.execute(
        select(Batch)
        .where(Batch.idempotency_key == idempotency_key)
        .options(selectinload(Batch.notifications))
    )
    return result.scalar_one_or_none()


async def get_batch(session: AsyncSession, batch_id: uuid.UUID) -> Batch | None:
    result = await session.execute(
        select(Batch).where(Batch.id == batch_id).options(selectinload(Batch.notifications))
    )
    return result.scalar_one_or_none()


async def list_batch_notifications(
    session: AsyncSession, batch_id: uuid.UUID
) -> list[Notification]:
    result = await session.execute(
        select(Notification)
        .where(Notification.batch_id == batch_id)
        .order_by(Notification.created_at)
    )
    return list(result.scalars().all())


async def get_notification(
    session: AsyncSession, notification_id: uuid.UUID, *, with_events: bool = False
) -> Notification | None:
    stmt = select(Notification).where(Notification.id == notification_id)
    if with_events:
        stmt = stmt.options(selectinload(Notification.events))
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def list_stuck_notifications(
    session: AsyncSession,
    *,
    status: str,
    older_than_seconds: int,
    limit: int,
) -> list[Notification]:
    """Rows in `status` whose last update is older than the threshold.

    Used by the reconciler to find work that was stranded (e.g. a crash between
    the DB commit and the publish, or a lost delivery receipt). The cutoff is
    computed with the database clock (``now()``) to avoid client/server skew.
    """
    cutoff = func.now() - timedelta(seconds=older_than_seconds)
    result = await session.execute(
        select(Notification)
        .where(Notification.status == status, Notification.updated_at <= cutoff)
        .order_by(Notification.updated_at)
        .limit(limit)
    )
    return list(result.scalars().all())


async def list_notifications_for_subscriber(
    session: AsyncSession,
    subscriber_id: str,
    *,
    status: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[Notification]:
    stmt = (
        select(Notification)
        .where(Notification.subscriber_id == subscriber_id)
        .options(selectinload(Notification.events))
        .order_by(Notification.created_at.desc())
        .limit(limit)
        .offset(offset)
    )
    if status is not None:
        stmt = stmt.where(Notification.status == status)
    result = await session.execute(stmt)
    return list(result.scalars().all())
