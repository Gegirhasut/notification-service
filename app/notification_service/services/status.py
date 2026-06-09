"""The single place where notification status transitions happen.

Every transition is a compare-and-set on ``notifications.status`` plus an
append to ``status_events``, both inside the *same* transaction (invariants 2 &
7). Keeping this logic in one module guarantees no code path mutates status
without also writing the audit event.

The functions here do NOT commit — the caller owns the transaction boundary so
that the ack-after-commit discipline (invariant 3) is enforced at the worker
level.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from notification_service.db.models import Notification, StatusEvent


def _now() -> datetime:
    return datetime.now(UTC)


async def _add_event(
    session: AsyncSession, notification_id: uuid.UUID, status: str, detail: str | None
) -> None:
    session.add(StatusEvent(notification_id=notification_id, status=status, detail=detail))


async def cas_queued_to_sent(
    session: AsyncSession,
    notification_id: uuid.UUID,
    *,
    detail: str | None = None,
) -> bool:
    """Atomically flip queued -> sent. Returns True iff this call won the race.

    This is the business exactly-once gate: only the single caller whose UPDATE
    affects one row (rowcount == 1) is allowed to call the provider. Redeliveries
    observe status != 'queued', get rowcount 0, and must ack-and-skip.
    """
    result = await session.execute(
        update(Notification)
        .where(Notification.id == notification_id, Notification.status == "queued")
        .values(status="sent", sent_at=_now(), updated_at=_now())
    )
    won = result.rowcount == 1
    if won:
        await _add_event(session, notification_id, "sent", detail)
    return won


async def mark_delivered(
    session: AsyncSession, notification_id: uuid.UUID, *, detail: str | None = None
) -> bool:
    """sent -> delivered. Returns True iff a row transitioned."""
    result = await session.execute(
        update(Notification)
        .where(Notification.id == notification_id, Notification.status == "sent")
        .values(status="delivered", delivered_at=_now(), updated_at=_now())
    )
    won = result.rowcount == 1
    if won:
        await _add_event(session, notification_id, "delivered", detail)
    return won


async def mark_rejected(
    session: AsyncSession,
    notification_id: uuid.UUID,
    *,
    last_error: str,
    from_statuses: tuple[str, ...] = ("queued", "sent"),
) -> bool:
    """-> rejected (terminal). Allowed from queued or sent by default.

    Used both by the receipts consumer (sent -> rejected on a reject receipt) and
    by the dispatcher when retries are exhausted (queued/sent -> rejected).
    """
    result = await session.execute(
        update(Notification)
        .where(
            Notification.id == notification_id,
            Notification.status.in_(from_statuses),
        )
        .values(status="rejected", last_error=last_error, updated_at=_now())
    )
    won = result.rowcount == 1
    if won:
        await _add_event(session, notification_id, "rejected", last_error)
    return won


async def revert_sent_to_queued_for_retry(
    session: AsyncSession,
    notification_id: uuid.UUID,
    *,
    retry_count: int,
    last_error: str,
) -> None:
    """Compensate a transient provider failure: sent -> queued, bump retry_count.

    The dispatcher flips queued -> sent *before* calling the provider (invariant
    2). When the provider then fails transiently we revert the row to 'queued' so
    the timed retry redelivery can win the CAS again and re-attempt the send. The
    retry attempt is recorded as a 'queued' status_event for the audit trail.
    """
    await session.execute(
        update(Notification)
        .where(Notification.id == notification_id)
        .values(
            status="queued",
            sent_at=None,
            retry_count=retry_count,
            last_error=last_error,
            updated_at=_now(),
        )
    )
    await _add_event(session, notification_id, "queued", f"retry {retry_count}: {last_error}")


async def set_provider_message_id(
    session: AsyncSession, notification_id: uuid.UUID, provider_message_id: str
) -> None:
    """Persist the gateway-assigned provider_message_id after acceptance."""
    await session.execute(
        update(Notification)
        .where(Notification.id == notification_id)
        .values(provider_message_id=provider_message_id, updated_at=_now())
    )
