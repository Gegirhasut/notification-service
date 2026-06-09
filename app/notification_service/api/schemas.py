"""Pydantic request/response schemas for the public API."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

Channel = Literal["sms", "email"]
NotificationType = Literal["transactional", "marketing"]
Status = Literal["queued", "sent", "delivered", "rejected"]


class CreateNotificationRequest(BaseModel):
    channel: Channel
    type: NotificationType
    message: str = Field(min_length=1)
    recipient_ids: list[str] = Field(min_length=1)

    model_config = ConfigDict(extra="forbid")


class RecipientStatus(BaseModel):
    id: uuid.UUID
    subscriber_id: str
    status: Status


class CreateNotificationResponse(BaseModel):
    batch_id: uuid.UUID
    channel: Channel
    type: NotificationType
    total: int
    recipients: list[RecipientStatus]
    duplicate: bool = False


class StatusEventOut(BaseModel):
    status: Status
    detail: str | None
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class NotificationOut(BaseModel):
    id: uuid.UUID
    batch_id: uuid.UUID
    subscriber_id: str
    channel: Channel
    type: NotificationType
    status: Status
    provider_message_id: str | None
    retry_count: int
    last_error: str | None
    created_at: datetime
    sent_at: datetime | None
    delivered_at: datetime | None

    model_config = ConfigDict(from_attributes=True)


class NotificationWithHistory(NotificationOut):
    history: list[StatusEventOut] = []


class HealthComponent(BaseModel):
    ok: bool
    detail: str | None = None


class HealthResponse(BaseModel):
    status: Literal["ok", "degraded"]
    components: dict[str, HealthComponent]
