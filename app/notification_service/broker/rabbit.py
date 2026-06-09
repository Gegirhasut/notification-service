"""RabbitMQ topology and connection helpers (aio-pika).

The topology is declared idempotently on startup by every process, so any of
api/dispatcher/receipts can be the first to boot. All exchanges and queues are
durable and messages are published persistent (delivery_mode=2) so the topology
and in-flight work survive a broker restart (invariant 6).

Retry is implemented with fixed-TTL queues that dead-letter back to the work
exchange — no delayed-message plugin (invariant 5).
"""

from __future__ import annotations

from typing import NamedTuple

import aio_pika
from aio_pika.abc import (
    AbstractRobustChannel,
    AbstractRobustConnection,
    AbstractRobustExchange,
    AbstractRobustQueue,
)

from notification_service.config import get_settings


class Topology(NamedTuple):
    exchange: AbstractRobustExchange  # main direct exchange (work + receipts)
    dlx: AbstractRobustExchange  # dead-letter exchange (retry tiers + parking)


# --- Names ------------------------------------------------------------------

EXCHANGE = "notifications"
DLX = "notifications.dlx"

WORK_QUEUE = "notifications.work"
RECEIPTS_QUEUE = "notifications.receipts"
PARKING_QUEUE = "notifications.parking"

ROUTING_WORK = "work"
ROUTING_RECEIPT = "receipt"
ROUTING_PARKING = "parking"

MAX_PRIORITY = 10

# Retry tiers, in escalation order. The TTL for each tier comes from settings so
# tests can shrink the fixed delays; the names mirror the production delays.
RETRY_SUFFIXES: tuple[str, ...] = ("5s", "30s", "120s")


def retry_ttl_ms(suffix: str) -> int:
    settings = get_settings()
    return {
        "5s": settings.retry_ttl_5s_ms,
        "30s": settings.retry_ttl_30s_ms,
        "120s": settings.retry_ttl_120s_ms,
    }[suffix]


def retry_queue_name(suffix: str) -> str:
    return f"notifications.retry.{suffix}"


def retry_routing_key(suffix: str) -> str:
    return f"retry.{suffix}"


async def connect() -> AbstractRobustConnection:
    settings = get_settings()
    return await aio_pika.connect_robust(settings.effective_amqp_url)


async def declare_topology(channel: AbstractRobustChannel) -> Topology:
    """Declare all exchanges and queues idempotently. Returns the exchanges."""
    # Main direct exchange + dead-letter exchange.
    exchange = await channel.declare_exchange(EXCHANGE, aio_pika.ExchangeType.DIRECT, durable=True)
    dlx = await channel.declare_exchange(DLX, aio_pika.ExchangeType.DIRECT, durable=True)

    # Priority work queue. Priority ordering only holds with consumer prefetch=1
    # (invariant 1); QoS is set on the dispatcher's channel, not here.
    work_queue = await channel.declare_queue(
        WORK_QUEUE,
        durable=True,
        arguments={"x-max-priority": MAX_PRIORITY},
    )
    await work_queue.bind(exchange, routing_key=ROUTING_WORK)

    # Retry tiers: each is a holding queue with a fixed TTL that dead-letters
    # back to the work exchange/routing-key once the TTL elapses.
    for suffix in RETRY_SUFFIXES:
        retry_queue = await channel.declare_queue(
            retry_queue_name(suffix),
            durable=True,
            arguments={
                "x-message-ttl": retry_ttl_ms(suffix),
                "x-dead-letter-exchange": EXCHANGE,
                "x-dead-letter-routing-key": ROUTING_WORK,
                "x-max-priority": MAX_PRIORITY,
            },
        )
        await retry_queue.bind(dlx, routing_key=retry_routing_key(suffix))

    # Parking: terminal dead messages, no consumer.
    parking_queue = await channel.declare_queue(PARKING_QUEUE, durable=True)
    await parking_queue.bind(dlx, routing_key=ROUTING_PARKING)

    # Receipts queue for provider delivery receipts.
    receipts_queue = await channel.declare_queue(RECEIPTS_QUEUE, durable=True)
    await receipts_queue.bind(exchange, routing_key=ROUTING_RECEIPT)

    return Topology(exchange=exchange, dlx=dlx)


async def get_work_queue(channel: AbstractRobustChannel) -> AbstractRobustQueue:
    """Fetch the already-declared work queue (priority queue) for consuming."""
    return await channel.declare_queue(
        WORK_QUEUE, durable=True, arguments={"x-max-priority": MAX_PRIORITY}, passive=True
    )


async def get_receipts_queue(channel: AbstractRobustChannel) -> AbstractRobustQueue:
    """Fetch the already-declared receipts queue for consuming."""
    return await channel.declare_queue(RECEIPTS_QUEUE, durable=True, passive=True)
