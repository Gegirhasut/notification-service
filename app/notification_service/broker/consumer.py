"""Generic consumer runner.

Wraps aio-pika queue consumption with manual acknowledgement and the ack-after-
commit discipline (invariant 3): the handler is responsible for making its state
change durable *before* returning. This runner only translates the handler's
outcome into ack / nack:

- handler returns normally  -> ack
- handler raises            -> nack(requeue=False)

Handlers that need to retry must republish to a retry tier themselves and then
return normally (so the original delivery is acked, not requeued blindly).
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable

from aio_pika.abc import AbstractIncomingMessage, AbstractRobustChannel, AbstractRobustQueue

logger = logging.getLogger(__name__)

Handler = Callable[[dict, AbstractIncomingMessage], Awaitable[None]]


async def consume(
    channel: AbstractRobustChannel,
    queue: AbstractRobustQueue,
    handler: Handler,
    *,
    prefetch: int,
) -> None:
    """Consume `queue` forever, dispatching JSON payloads to `handler`.

    Sets QoS prefetch on the channel first. For the work queue this MUST be 1 so
    priority ordering is preserved (invariant 1).
    """
    await channel.set_qos(prefetch_count=prefetch)

    async with queue.iterator() as it:
        async for message in it:
            try:
                payload = json.loads(message.body)
            except json.JSONDecodeError:
                logger.exception("Undecodable message, dropping to parking via nack")
                await message.nack(requeue=False)
                continue

            try:
                await handler(payload, message)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Handler failed; nack(requeue=False): %s", payload)
                await message.nack(requeue=False)
            else:
                await message.ack()
