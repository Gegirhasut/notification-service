"""Subscriber endpoint: notification history for a subscriber."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from notification_service.api.schemas import (
    NotificationWithHistory,
    Status,
    StatusEventOut,
)
from notification_service.db import repositories as repo
from notification_service.db.session import get_session

router = APIRouter(prefix="/api/v1", tags=["subscribers"])


@router.get(
    "/subscribers/{subscriber_id}/notifications",
    response_model=list[NotificationWithHistory],
)
async def list_subscriber_notifications(
    subscriber_id: str,
    status: Status | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(get_session),
) -> list[NotificationWithHistory]:
    notifications = await repo.list_notifications_for_subscriber(
        session, subscriber_id, status=status, limit=limit, offset=offset
    )
    out: list[NotificationWithHistory] = []
    for n in notifications:
        item = NotificationWithHistory.model_validate(n)
        item.history = [StatusEventOut.model_validate(e) for e in n.events]
        out.append(item)
    return out
