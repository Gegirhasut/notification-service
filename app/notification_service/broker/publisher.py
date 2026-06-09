"""Message construction and publishing helpers.

A work message carries only the notification id; the dispatcher reloads the row
from Postgres (the source of truth) before acting. Messages are persistent and
priority is set from the notification type.
"""

from __future__ import annotations

import json

import aio_pika
from aio_pika.abc import AbstractExchange

from notification_service.broker import rabbit
from notification_service.db.models import (
    PRIORITY_MARKETING,
    PRIORITY_TRANSACTIONAL,
)


def priority_for_type(type_: str) -> int:
    return PRIORITY_TRANSACTIONAL if type_ == "transactional" else PRIORITY_MARKETING


def _message(payload: dict, *, priority: int = 0, headers: dict | None = None) -> aio_pika.Message:
    return aio_pika.Message(
        body=json.dumps(payload).encode(),
        content_type="application/json",
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        priority=priority,
        headers=headers or {},
    )


async def publish_work(
    exchange: AbstractExchange, notification_id: str, type_: str, *, retry_count: int = 0
) -> None:
    """Publish a notification onto the priority work queue."""
    priority = priority_for_type(type_)
    msg = _message(
        {"notification_id": notification_id, "type": type_, "retry_count": retry_count},
        priority=priority,
        headers={"x-retry-count": retry_count},
    )
    await exchange.publish(msg, routing_key=rabbit.ROUTING_WORK)


async def publish_retry(
    dlx: AbstractExchange,
    notification_id: str,
    type_: str,
    *,
    suffix: str,
    retry_count: int,
) -> None:
    """Publish to a fixed-TTL retry queue (via the dead-letter exchange)."""
    priority = priority_for_type(type_)
    msg = _message(
        {"notification_id": notification_id, "type": type_, "retry_count": retry_count},
        priority=priority,
        headers={"x-retry-count": retry_count},
    )
    await dlx.publish(msg, routing_key=rabbit.retry_routing_key(suffix))


async def publish_parking(
    dlx: AbstractExchange, notification_id: str, type_: str, *, last_error: str
) -> None:
    """Publish a terminally-failed notification to the parking queue."""
    msg = _message(
        {"notification_id": notification_id, "type": type_, "last_error": last_error},
        headers={"x-last-error": last_error[:255]},
    )
    await dlx.publish(msg, routing_key=rabbit.ROUTING_PARKING)


async def publish_receipt(
    exchange: AbstractExchange,
    notification_id: str,
    *,
    outcome: str,
    provider_message_id: str | None,
    detail: str | None = None,
    attempt: int = 0,
) -> None:
    """Publish a provider delivery receipt (delivered | rejected | transient).

    ``attempt`` carries a bounded redelivery counter (header ``x-receipt-attempt``)
    so the receipts consumer can re-drive a transient apply-failure a fixed number
    of times and then park it, instead of requeueing forever.
    """
    msg = _message(
        {
            "notification_id": notification_id,
            "outcome": outcome,
            "provider_message_id": provider_message_id,
            "detail": detail,
        },
        headers={"x-receipt-attempt": attempt},
    )
    await exchange.publish(msg, routing_key=rabbit.ROUTING_RECEIPT)
