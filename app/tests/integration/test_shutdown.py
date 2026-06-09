"""Regression tests for A2: graceful worker shutdown.

The shutdown handler must (1) stop consuming, (2) drain in-flight provider receipt
tasks, and (3) close the broker connection — without losing unacked work.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid

import pytest

from notification_service.broker import rabbit

pytestmark = pytest.mark.asyncio


async def _post(client, recipient_ids):
    return await client.post(
        "/api/v1/notifications",
        json={
            "channel": "sms",
            "type": "transactional",
            "message": "hi",
            "recipient_ids": recipient_ids,
        },
    )


async def test_provider_drain_awaits_in_flight_receipts():
    # drain() must await delayed receipt tasks so shutdown doesn't lose them.
    from notification_service.config import ProviderMode
    from notification_service.providers.sms_mock import SmsMockProvider

    emitted: list[tuple[str, str]] = []

    async def emit(nid, outcome, provider_message_id, detail):
        emitted.append((nid, outcome))

    provider = SmsMockProvider(
        mode=ProviderMode.always_deliver, emit_receipt=emit, receipt_delay=0.3
    )
    await provider.send("id-1", "rcpt", "hello")
    assert emitted == []  # receipt is still in flight (delayed)

    await provider.drain()
    assert emitted == [("id-1", "delivered")]  # drain awaited the in-flight task


async def test_dispatcher_run_shuts_down_cleanly(harness_factory, monkeypatch):
    # Run the real dispatcher.run() against the test broker with an injected stop
    # event: it consumes a message, then on stop returns and closes its connection.
    from notification_service.workers import dispatcher

    h = await harness_factory(start_consumers=False, mode="always_deliver")

    opened: list = []
    real_connect = rabbit.connect

    async def capturing_connect():
        conn = await real_connect()
        opened.append(conn)
        return conn

    monkeypatch.setattr(dispatcher.rabbit, "connect", capturing_connect)

    stop = asyncio.Event()
    run_task = asyncio.create_task(dispatcher.run(stop=stop))
    try:
        resp = await _post(h.client, ["shutdown-1"])
        nid = resp.json()["recipients"][0]["id"]
        # The dispatcher consumed and processed it off 'queued' (no receipts worker
        # here, so it settles at 'sent'); proves consuming works before shutdown.
        await h.wait_for_status(nid, {"sent", "delivered", "rejected"}, timeout=20)

        stop.set()
        await asyncio.wait_for(run_task, timeout=15)  # stops consuming and returns
    finally:
        if not run_task.done():
            run_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await run_task

    assert opened and opened[0].is_closed  # connection closed on shutdown


async def test_unacked_work_is_requeued_on_shutdown(harness_factory):
    # A message delivered but not yet acked when the worker stops must be requeued
    # by the broker (manual-ack discipline), not lost.
    from notification_service.broker import consumer as consumer_mod

    h = await harness_factory(start_consumers=False)
    channel = await h.connection.channel()
    work_queue = await rabbit.get_work_queue(channel)

    await h.publish_work_direct(str(uuid.uuid4()), "transactional")
    received = asyncio.Event()

    async def block_handler(payload, message):
        received.set()
        await asyncio.sleep(3600)  # hold the message unacked

    task = asyncio.create_task(consumer_mod.consume(channel, work_queue, block_handler, prefetch=1))
    await asyncio.wait_for(received.wait(), timeout=10)  # delivered, unacked

    # Simulate shutdown: stop consuming, then close the channel -> unacked requeued.
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    await channel.close()

    async def _back_in_queue():
        return await h.queue_message_count(rabbit.WORK_QUEUE) >= 1

    await h.wait_until(_back_in_queue, timeout=10)
