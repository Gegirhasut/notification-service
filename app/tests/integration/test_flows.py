"""Integration tests for the eight required scenarios (CLAUDE.md testing rules).

Each asserts DB state and/or provider calls against real infrastructure.
"""

from __future__ import annotations

import pytest

from notification_service.broker import rabbit
from notification_service.db import repositories as repo
from notification_service.db.session import session_scope

pytestmark = pytest.mark.asyncio


async def _post(
    client,
    *,
    channel="sms",
    type_="transactional",
    message="hi",
    recipient_ids=None,
    idempotency_key=None,
):
    headers = {}
    if idempotency_key is not None:
        headers["Idempotency-Key"] = idempotency_key
    resp = await client.post(
        "/api/v1/notifications",
        json={
            "channel": channel,
            "type": type_,
            "message": message,
            "recipient_ids": recipient_ids or ["sub-1"],
        },
        headers=headers,
    )
    return resp


# 1. Bulk accept ----------------------------------------------------------- #
async def test_bulk_accept_creates_queued_rows(harness_factory):
    h = await harness_factory(start_consumers=False)  # keep rows in 'queued'
    recipients = [f"sub-{i}" for i in range(5)]
    resp = await _post(h.client, recipient_ids=recipients)

    assert resp.status_code == 202
    body = resp.json()
    assert body["total"] == 5
    assert len(body["recipients"]) == 5
    assert all(r["status"] == "queued" for r in body["recipients"])

    async with session_scope() as session:
        rows = await repo.list_batch_notifications(session, body["batch_id"])
    assert len(rows) == 5
    assert all(r.status == "queued" for r in rows)

    # Invariant 7: each notification has a 'queued' status_event at creation.
    for r in body["recipients"]:
        history = await h.history(r["id"])
        assert history == ["queued"]


# 2. Full happy chain ------------------------------------------------------ #
async def test_happy_chain_delivered(harness_factory):
    h = await harness_factory(mode="always_deliver")
    resp = await _post(h.client, message="hello world", recipient_ids=["alice"])
    nid = resp.json()["recipients"][0]["id"]

    status = await h.wait_for_status(nid, {"delivered"})
    assert status == "delivered"

    # Provider called once with the right args.
    calls = h.factory.for_channel("sms").calls
    assert len(calls) == 1
    assert calls[0].notification_id == nid
    assert calls[0].recipient == "alice"
    assert calls[0].body == "hello world"

    # All transitions recorded in order.
    assert await h.history(nid) == ["queued", "sent", "delivered"]
    notification = await h.get_notification(nid)
    assert notification.provider_message_id is not None
    assert notification.sent_at is not None
    assert notification.delivered_at is not None


# 3. Rejected path --------------------------------------------------------- #
async def test_rejected_path(harness_factory):
    h = await harness_factory(mode="always_reject")
    resp = await _post(h.client, recipient_ids=["bob"])
    nid = resp.json()["recipients"][0]["id"]

    status = await h.wait_for_status(nid, {"rejected"})
    assert status == "rejected"

    notification = await h.get_notification(nid)
    assert notification.last_error is not None
    assert await h.history(nid) == ["queued", "sent", "rejected"]


# 4. Priority -------------------------------------------------------------- #
async def test_priority_transactional_overtakes_marketing(harness_factory):
    # Consumers stopped so all messages accumulate before any is consumed.
    h = await harness_factory(mode="always_deliver", start_consumers=False)

    mkt = await _post(h.client, type_="marketing", recipient_ids=[f"m{i}" for i in range(10)])
    txn = await _post(h.client, type_="transactional", recipient_ids=["vip"])
    txn_id = txn.json()["recipients"][0]["id"]
    assert mkt.status_code == 202 and txn.status_code == 202

    await h.start_consumers()
    await h.wait_for_status(txn_id, {"delivered"})

    calls = h.factory.for_channel("sms").calls
    # With prefetch=1 and everything pre-enqueued, the priority-10 transactional
    # message is delivered before any priority-1 marketing message.
    assert calls[0].notification_id == txn_id


# 5. Idempotency ----------------------------------------------------------- #
async def test_idempotency_same_key_returns_original(harness_factory):
    h = await harness_factory(start_consumers=False)
    key = "order-42"

    first = await _post(h.client, recipient_ids=["x", "y"], idempotency_key=key)
    second = await _post(h.client, recipient_ids=["x", "y"], idempotency_key=key)

    assert first.status_code == 202
    assert second.status_code == 200
    assert second.json()["duplicate"] is True
    assert first.json()["batch_id"] == second.json()["batch_id"]

    # Exactly one batch + 2 notifications total (no duplicates created).
    async with session_scope() as session:
        batch = await repo.get_batch_by_idempotency_key(session, key)
        rows = await repo.list_batch_notifications(session, batch.id)
    assert len(rows) == 2


# 6. Exactly-once under redelivery ----------------------------------------- #
async def test_exactly_once_under_redelivery(harness_factory):
    h = await harness_factory(mode="always_deliver", start_consumers=False)
    resp = await _post(h.client, recipient_ids=["dup"])
    nid = resp.json()["recipients"][0]["id"]

    # Publish a second (duplicate) work message for the same notification.
    await h.publish_work_direct(nid, "transactional")

    await h.start_consumers()
    await h.wait_for_status(nid, {"delivered"})
    # Let any second delivery be processed (and skipped) too.
    import asyncio

    await asyncio.sleep(0.5)

    calls = [c for c in h.factory.for_channel("sms").calls if c.notification_id == nid]
    assert len(calls) == 1  # CAS gate => provider.send invoked exactly once


# 6b. Exactly-once when a duplicate arrives mid-retry ---------------------- #
async def test_exactly_once_duplicate_during_retry(harness_factory):
    # transient_then_deliver: attempt 1 fails transiently (row reverts to
    # 'queued'), attempt 2 delivers. With consumers stopped we pre-enqueue both
    # the original and a duplicate work message for the same id, so the duplicate
    # is processed *while the row is back in 'queued'* — the CAS gate cannot block
    # it, and exactly-once must rest on the provider's idempotency key.
    h = await harness_factory(mode="transient_then_deliver", start_consumers=False)
    resp = await _post(h.client, recipient_ids=["mid-retry"])
    nid = resp.json()["recipients"][0]["id"]

    # Duplicate work message for the same notification, sitting behind the
    # original in the work queue.
    await h.publish_work_direct(nid, "transactional")

    await h.start_consumers()
    await h.wait_for_status(nid, {"delivered"}, timeout=20)

    import asyncio

    await asyncio.sleep(0.5)  # let the timed retry come back and be skipped

    provider = h.factory.for_channel("sms")
    # send() may be invoked several times (transient attempt, the duplicate, the
    # returning retry) but the provider key admits exactly one effective send.
    assert provider.effective_sends.get(nid) == 1
    notification = await h.get_notification(nid)
    assert notification.status == "delivered"
    # Exactly one delivered event in the audit trail.
    assert (await h.history(nid)).count("delivered") == 1


# 7a. Retry then deliver --------------------------------------------------- #
async def test_retry_then_deliver(harness_factory):
    h = await harness_factory(mode="transient_then_deliver")  # 1 transient, then ok
    resp = await _post(h.client, recipient_ids=["retry-me"])
    nid = resp.json()["recipients"][0]["id"]

    status = await h.wait_for_status(nid, {"delivered"}, timeout=20)
    assert status == "delivered"

    notification = await h.get_notification(nid)
    assert notification.retry_count == 1
    # Provider attempted twice: transient, then accepted.
    calls = [c for c in h.factory.for_channel("sms").calls if c.notification_id == nid]
    assert len(calls) == 2
    # Audit trail of the CAS-before-provider design: the first attempt flips to
    # 'sent', the transient failure reverts to 'queued', then the retry flips to
    # 'sent' and the receipt finalizes 'delivered'.
    assert await h.history(nid) == ["queued", "sent", "queued", "sent", "delivered"]


# 7b. Retries exhausted -> rejected + parking ------------------------------ #
async def test_retries_exhausted_to_parking(harness_factory):
    h = await harness_factory(mode="transient_then_deliver", transient_attempts=10, max_retries=3)
    resp = await _post(h.client, recipient_ids=["doomed"])
    nid = resp.json()["recipients"][0]["id"]

    status = await h.wait_for_status(nid, {"rejected"}, timeout=20)
    assert status == "rejected"

    notification = await h.get_notification(nid)
    assert notification.retry_count == 3
    assert notification.last_error is not None

    # The terminally-failed message landed in the parking queue.
    import asyncio

    await asyncio.sleep(0.3)
    assert await h.queue_message_count(rabbit.PARKING_QUEUE) >= 1


# 8. History API ----------------------------------------------------------- #
async def test_history_api(harness_factory):
    h = await harness_factory(mode="always_deliver")
    resp = await _post(h.client, recipient_ids=["hist"])
    nid = resp.json()["recipients"][0]["id"]
    await h.wait_for_status(nid, {"delivered"})

    listing = await h.client.get("/api/v1/subscribers/hist/notifications")
    assert listing.status_code == 200
    items = listing.json()
    assert len(items) == 1
    item = items[0]
    assert item["id"] == nid
    assert item["status"] == "delivered"
    assert [e["status"] for e in item["history"]] == ["queued", "sent", "delivered"]
