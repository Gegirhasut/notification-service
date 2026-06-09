"""Regression tests for A1: an unexpected handler fault must never silently drop
a message.

- A work-queue fault (a generic exception from the provider/CAS step) is treated
  as transient: re-driven through the retry tiers and, once MAX_RETRIES is
  exhausted, parked + marked 'rejected' — never stranded silently in 'queued'.
- A poison/undecodable message is dead-lettered to parking, not discarded.
- A receipts fault is requeued so the row still reaches a terminal status rather
  than stranding in 'sent'.
"""

from __future__ import annotations

import pytest

from notification_service.broker import rabbit

pytestmark = pytest.mark.asyncio


async def _post(client, *, channel="sms", type_="transactional", message="hi", recipient_ids):
    return await client.post(
        "/api/v1/notifications",
        json={
            "channel": channel,
            "type": type_,
            "message": message,
            "recipient_ids": recipient_ids,
        },
    )


async def test_unexpected_work_fault_retries_then_parks(harness_factory, monkeypatch):
    # Force the provider to raise a generic (non-transient) exception on every
    # attempt. The dispatcher must NOT drop the message: it re-drives through the
    # tiers and lands in parking + 'rejected' once retries are exhausted.
    h = await harness_factory(mode="always_deliver", start_consumers=False, max_retries=3)
    provider = h.factory.for_channel("sms")

    async def boom(notification_id, recipient, body):
        raise RuntimeError("provider exploded")

    monkeypatch.setattr(provider, "send", boom)
    await h.start_consumers()

    resp = await _post(h.client, recipient_ids=["boom-1"])
    nid = resp.json()["recipients"][0]["id"]

    # Reaches the terminal 'rejected' state — never stuck silently in 'queued'.
    assert await h.wait_for_status(nid, {"rejected"}, timeout=25) == "rejected"
    n = await h.get_notification(nid)
    assert n.retry_count == 3
    assert n.last_error is not None

    # And the terminally-failed message is in parking (not the void).
    async def _parked():
        return await h.queue_message_count(rabbit.PARKING_QUEUE) >= 1

    await h.wait_until(_parked, timeout=25)


async def test_poison_work_message_dead_lettered_to_parking(harness_factory):
    # An undecodable body can never be processed; it must be dead-lettered to
    # parking via the work queue's DLX, not silently discarded.
    h = await harness_factory(start_consumers=True)
    await h.publish_raw_work(b"definitely not json {{{")

    async def _parked():
        return await h.queue_message_count(rabbit.PARKING_QUEUE) >= 1

    await h.wait_until(_parked, timeout=15)


async def test_receipts_transient_fault_is_requeued_not_lost(harness_factory, monkeypatch):
    # Make the first receipt application fail (a simulated DB blip). The receipts
    # consumer must requeue it (not drop it / not strand the row in 'sent'); the
    # retry then succeeds and the row reaches 'delivered'.
    from notification_service.services import status as status_mod

    h = await harness_factory(mode="always_deliver", start_consumers=False)
    real_mark_delivered = status_mod.mark_delivered
    state = {"calls": 0}

    async def flaky_mark_delivered(session, nid, *, detail=None):
        state["calls"] += 1
        if state["calls"] == 1:
            raise RuntimeError("receipts db blip")
        return await real_mark_delivered(session, nid, detail=detail)

    monkeypatch.setattr(status_mod, "mark_delivered", flaky_mark_delivered)
    await h.start_consumers()

    resp = await _post(h.client, recipient_ids=["rcpt-1"])
    nid = resp.json()["recipients"][0]["id"]

    assert await h.wait_for_status(nid, {"delivered"}, timeout=25) == "delivered"
    assert state["calls"] >= 2  # failed once, re-driven, then succeeded


async def test_malformed_receipt_is_parked_not_looped(harness_factory):
    # A decodable receipt that can never be applied (no notification_id) is POISON:
    # it must be dead-lettered to parking immediately, never requeued in a hot loop.
    h = await harness_factory(mode="always_deliver", start_consumers=True)

    await h.publish_raw_receipt({"outcome": "delivered", "detail": "no id here"})

    async def _parked():
        return await h.queue_message_count(rabbit.PARKING_QUEUE) >= 1

    await h.wait_until(_parked, timeout=15)

    # Reaching parking proves it did NOT loop (a requeue never dead-letters): the
    # receipts queue is drained, not perpetually holding the poison message.
    async def _receipts_drained():
        return await h.queue_message_count(rabbit.RECEIPTS_QUEUE) == 0

    await h.wait_until(_receipts_drained, timeout=10)


async def test_receipt_transient_fault_is_bounded_then_parked(harness_factory, monkeypatch):
    # A receipt apply-fault that NEVER recovers must not requeue forever: it is
    # re-driven a bounded number of times (max_receipt_redeliveries) and then parked.
    from notification_service.services import status as status_mod

    h = await harness_factory(
        mode="always_deliver", start_consumers=False, max_receipt_redeliveries=2
    )
    state = {"calls": 0}

    async def always_fail_mark_delivered(session, nid, *, detail=None):
        state["calls"] += 1
        raise RuntimeError("permanent receipts db outage")

    monkeypatch.setattr(status_mod, "mark_delivered", always_fail_mark_delivered)
    await h.start_consumers()

    resp = await _post(h.client, recipient_ids=["bounded-1"])
    nid = resp.json()["recipients"][0]["id"]

    # The undeliverable receipt ends up parked after the bounded re-drives.
    async def _parked():
        return await h.queue_message_count(rabbit.PARKING_QUEUE) >= 1

    await h.wait_until(_parked, timeout=20)

    # Bounded: attempt 1, attempt 2, then attempt 3 exceeds the cap and parks.
    assert state["calls"] == 3
    # The row never reached 'delivered' (the receipt could not be applied).
    assert (await h.get_notification(nid)).status == "sent"
