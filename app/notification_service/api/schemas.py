"""Pydantic request/response schemas for the public API."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, field_validator

Channel = Literal["sms", "email"]
NotificationType = Literal["transactional", "marketing"]
Status = Literal["queued", "sent", "delivered", "rejected"]

MAX_MESSAGE_LEN = 1000
MAX_RECIPIENT_ID_LEN = 128


class CreateNotificationRequest(BaseModel):
    channel: Channel
    type: NotificationType
    message: str
    recipient_ids: list[str]

    model_config = ConfigDict(extra="forbid")

    @field_validator("message")
    @classmethod
    def _validate_message(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("message must not be blank")
        if len(value) > MAX_MESSAGE_LEN:
            raise ValueError(f"message must be at most {MAX_MESSAGE_LEN} characters")
        return value

    @field_validator("recipient_ids")
    @classmethod
    def _validate_recipients(cls, value: list[str]) -> list[str]:
        # recipient_ids are opaque identifiers — only structural rules, no
        # phone/email parsing. Trim, reject blanks/over-long, then de-duplicate
        # preserving first-seen order so `total` reflects the unique recipients.
        from notification_service.config import get_settings

        max_recipients = get_settings().max_recipients
        if not value:
            raise ValueError("recipient_ids must not be empty")
        if len(value) > max_recipients:
            raise ValueError(f"recipient_ids must contain at most {max_recipients} items")

        seen: set[str] = set()
        unique: list[str] = []
        for raw in value:
            rid = raw.strip()
            if not rid:
                raise ValueError("recipient_ids entries must not be blank")
            if len(rid) > MAX_RECIPIENT_ID_LEN:
                raise ValueError(
                    f"recipient_ids entries must be at most {MAX_RECIPIENT_ID_LEN} characters"
                )
            if rid not in seen:
                seen.add(rid)
                unique.append(rid)
        return unique


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
