"""Notification endpoints: create a batch, fetch a single notification."""

from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from notification_service.api.schemas import (
    CreateNotificationRequest,
    CreateNotificationResponse,
    NotificationWithHistory,
    RecipientStatus,
    StatusEventOut,
)
from notification_service.db import repositories as repo
from notification_service.db.session import get_session
from notification_service.services import notification_service

router = APIRouter(prefix="/api/v1", tags=["notifications"])


@router.post(
    "/notifications",
    response_model=CreateNotificationResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def create_notifications(
    payload: CreateNotificationRequest,
    request: Request,
    response: Response,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    session: AsyncSession = Depends(get_session),
) -> CreateNotificationResponse:
    if idempotency_key is not None:
        idempotency_key = idempotency_key.strip()
        if not idempotency_key or len(idempotency_key) > 255:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Idempotency-Key must be non-blank and at most 255 characters",
            )

    exchange = request.app.state.exchange
    result = await notification_service.create_and_dispatch(
        session,
        exchange,
        channel=payload.channel,
        type_=payload.type,
        body=payload.message,
        recipient_ids=payload.recipient_ids,
        idempotency_key=idempotency_key,
    )

    # Duplicate request -> 200 with the original batch; new -> 202.
    if result.duplicate:
        response.status_code = status.HTTP_200_OK

    return CreateNotificationResponse(
        batch_id=result.batch.id,
        channel=result.batch.channel,
        type=result.batch.type,
        total=result.batch.total,
        duplicate=result.duplicate,
        recipients=[
            RecipientStatus(id=n.id, subscriber_id=n.subscriber_id, status=n.status)
            for n in result.notifications
        ],
    )


@router.get("/notifications/{notification_id}", response_model=NotificationWithHistory)
async def get_notification(
    notification_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
) -> NotificationWithHistory:
    notification = await repo.get_notification(session, notification_id, with_events=True)
    if notification is None:
        raise HTTPException(status_code=404, detail="notification not found")

    out = NotificationWithHistory.model_validate(notification)
    out.history = [StatusEventOut.model_validate(e) for e in notification.events]
    return out
