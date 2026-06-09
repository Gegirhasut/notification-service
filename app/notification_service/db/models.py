"""SQLAlchemy 2.0 ORM models.

The schema mirrors the data model in CLAUDE.md: UUID primary keys, a `batches`
header row per accepted request, one `notifications` row per recipient, and an
append-only `status_events` audit trail. Enum-like fields are stored as plain
strings (constrained at the application layer) to keep migrations simple and
avoid Postgres ENUM-alter pain.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


# --- Domain constants -------------------------------------------------------

CHANNELS = ("sms", "email")
TYPES = ("transactional", "marketing")
STATUSES = ("queued", "sent", "delivered", "rejected")

# Priority levels for the single x-max-priority work queue.
PRIORITY_TRANSACTIONAL = 10
PRIORITY_MARKETING = 1


def _uuid_col(*args, **kw) -> Mapped[uuid.UUID]:
    return mapped_column(PG_UUID(as_uuid=True), *args, **kw)


class Batch(Base):
    __tablename__ = "batches"

    id: Mapped[uuid.UUID] = _uuid_col(primary_key=True, default=uuid.uuid4)
    channel: Mapped[str] = mapped_column(String(16), nullable=False)
    type: Mapped[str] = mapped_column(String(16), nullable=False)
    body: Mapped[str] = mapped_column(Text, nullable=False)
    # Nullable + UNIQUE: requests without an Idempotency-Key are always distinct,
    # but two requests carrying the same key collide on this constraint.
    idempotency_key: Mapped[str | None] = mapped_column(String(255), nullable=True)
    total: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    notifications: Mapped[list[Notification]] = relationship(
        back_populates="batch", cascade="all, delete-orphan"
    )

    __table_args__ = (UniqueConstraint("idempotency_key", name="uq_batches_idempotency_key"),)


class Notification(Base):
    __tablename__ = "notifications"

    # The id doubles as the provider idempotency key (invariant 2).
    id: Mapped[uuid.UUID] = _uuid_col(primary_key=True, default=uuid.uuid4)
    batch_id: Mapped[uuid.UUID] = _uuid_col(
        ForeignKey("batches.id", ondelete="CASCADE"), nullable=False
    )
    subscriber_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    channel: Mapped[str] = mapped_column(String(16), nullable=False)
    type: Mapped[str] = mapped_column(String(16), nullable=False)
    body: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="queued", index=True)
    provider_message_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    batch: Mapped[Batch] = relationship(back_populates="notifications")
    events: Mapped[list[StatusEvent]] = relationship(
        back_populates="notification",
        cascade="all, delete-orphan",
        order_by="StatusEvent.created_at",
    )

    __table_args__ = (Index("ix_notifications_subscriber_created", "subscriber_id", "created_at"),)


class StatusEvent(Base):
    __tablename__ = "status_events"

    id: Mapped[uuid.UUID] = _uuid_col(primary_key=True, default=uuid.uuid4)
    notification_id: Mapped[uuid.UUID] = _uuid_col(
        ForeignKey("notifications.id", ondelete="CASCADE"), nullable=False, index=True
    )
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    notification: Mapped[Notification] = relationship(back_populates="events")
