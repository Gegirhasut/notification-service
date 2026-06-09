"""Unit tests for the compare-and-set status transition (invariant 2)."""

from __future__ import annotations

import pytest
from sqlalchemy import select

from notification_service.db import repositories as repo
from notification_service.db.models import StatusEvent
from notification_service.services import status as status_svc

pytestmark = pytest.mark.asyncio


async def _make_notification(session):
    _, notifications = await repo.create_batch(
        session,
        channel="sms",
        type_="transactional",
        body="hi",
        idempotency_key=None,
        recipient_ids=["sub-1"],
    )
    await session.commit()
    return notifications[0].id


async def test_cas_flips_once(db_session):
    nid = await _make_notification(db_session)

    # First CAS wins.
    won1 = await status_svc.cas_queued_to_sent(db_session, nid)
    await db_session.commit()
    assert won1 is True

    # Second CAS loses (already 'sent').
    won2 = await status_svc.cas_queued_to_sent(db_session, nid)
    await db_session.commit()
    assert won2 is False

    notification = await repo.get_notification(db_session, nid)
    assert notification.status == "sent"
    assert notification.sent_at is not None


async def test_cas_writes_status_event(db_session):
    nid = await _make_notification(db_session)
    await status_svc.cas_queued_to_sent(db_session, nid, detail="dispatched")
    await db_session.commit()

    events = (
        (await db_session.execute(select(StatusEvent).where(StatusEvent.notification_id == nid)))
        .scalars()
        .all()
    )
    statuses = [e.status for e in events]
    # 'queued' (creation) then 'sent' (this transition), same audit trail.
    assert statuses == ["queued", "sent"]


async def test_delivered_only_from_sent(db_session):
    nid = await _make_notification(db_session)

    # Cannot deliver straight from queued.
    won = await status_svc.mark_delivered(db_session, nid)
    await db_session.commit()
    assert won is False

    await status_svc.cas_queued_to_sent(db_session, nid)
    await db_session.commit()
    won = await status_svc.mark_delivered(db_session, nid)
    await db_session.commit()
    assert won is True

    notification = await repo.get_notification(db_session, nid)
    assert notification.status == "delivered"
    assert notification.delivered_at is not None
