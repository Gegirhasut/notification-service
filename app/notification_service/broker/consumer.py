"""Generic consumer runner.

Wraps aio-pika queue consumption with manual acknowledgement and the ack-after-
commit discipline (invariant 3): the handler is responsible for making its state
change durable *before* returning. This runner translates the handler's outcome
into ack / nack and, on an unexpected fault, consults a recovery policy so a
message is never silently discarded into the void:

- handler returns normally        -> ack
- payload is undecodable (poison)  -> dead-letter to parking (nack requeue=False)
- handler raises                   -> ask `on_failure` what to do; default is to
                                      dead-letter to parking (the DLX backstop),
                                      so a transient fault parks the message
                                      instead of dropping it.

A recovery policy can return ``Disposition.ACK`` (it republished the work to a
retry tier itself and the original should be acked), ``Disposition.REQUEUE`` (let
the broker redeliver for another attempt), or ``Disposition.PARK`` (dead-letter to
parking). Both the work queue and the receipts queue are declared with a
dead-letter exchange routed to parking, so a ``nack(requeue=False)`` always lands
in parking — never nowhere.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from enum import Enum

from aio_pika.abc import AbstractIncomingMessage, AbstractRobustChannel, AbstractRobustQueue

logger = logging.getLogger(__name__)


class Disposition(str, Enum):
    """What the runner should do with a message after a handler fault."""

    ACK = "ack"  # the policy already re-routed the work; ack the original
    REQUEUE = "requeue"  # transient: let the broker redeliver for another attempt
    PARK = "park"  # dead-letter to parking (the backstop; never the void)


Handler = Callable[[dict, AbstractIncomingMessage], Awaitable[None]]
FailurePolicy = Callable[[dict, AbstractIncomingMessage, Exception], Awaitable[Disposition]]


async def consume(
    channel: AbstractRobustChannel,
    queue: AbstractRobustQueue,
    handler: Handler,
    *,
    prefetch: int,
    on_failure: FailurePolicy | None = None,
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
                # Poison message: it can never be decoded, so requeuing would only
                # spin. Dead-letter it to parking via the queue's DLX.
                logger.exception("Undecodable message; dead-lettering to parking")
                await message.nack(requeue=False)
                continue

            try:
                await handler(payload, message)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("Handler failed: %s", payload)
                disposition = Disposition.PARK
                if on_failure is not None:
                    try:
                        disposition = await on_failure(payload, message, exc)
                    except Exception:
                        logger.exception("Failure policy raised; parking: %s", payload)
                        disposition = Disposition.PARK

                if disposition is Disposition.ACK:
                    await message.ack()
                elif disposition is Disposition.REQUEUE:
                    await message.nack(requeue=True)
                else:
                    # PARK: dead-letter to parking (DLX backstop), never discard.
                    await message.nack(requeue=False)
            else:
                await message.ack()
