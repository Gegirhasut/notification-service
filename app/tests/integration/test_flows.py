"""Integration tests for the eight required scenarios (CLAUDE.md testing rules).

Each asserts DB state and/or provider calls against real infrastructure.
"""

from __future__ import annotations

import uuid

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


# 4b. Priority under a marketing backlog ----------------------------------- #
async def test_priority_transactional_overtakes_marketing_backlog(harness_factory):
    # A heavier, deterministic version of the priority test: prove a single
    # transactional message overtakes a *backlog* of low-priority marketing
    # already sitting in the queue, even though it is enqueued last.
    #
    # Determinism: always_deliver has no transient failures, so retry/backoff
    # cannot reorder dispatch; consumers stay stopped until everything is
    # enqueued, so a live consumer can't grab whatever arrives first.
    h = await harness_factory(mode="always_deliver", start_consumers=False)

    backlog = [f"m{i}" for i in range(50)]
    mkt = await _post(h.client, type_="marketing", recipient_ids=backlog)
    assert mkt.status_code == 202

    # The single transactional message is enqueued LAST, behind all 50.
    txn = await _post(h.client, type_="transactional", recipient_ids=["vip"])
    assert txn.status_code == 202
    txn_id = txn.json()["recipients"][0]["id"]

    # Only now start the dispatcher (prefetch=1, same as production) and drain.
    await h.start_consumers()
    await h.wait_for_status(txn_id, {"delivered"})

    calls = h.factory.for_channel("sms").calls
    # Despite being enqueued last, the priority-10 transactional message is the
    # very first send() call — it overtook the entire marketing backlog.
    assert calls[0].notification_id == txn_id

    # And the whole batch eventually drains to delivered.
    mkt_ids = [r["id"] for r in mkt.json()["recipients"]]
    for nid in mkt_ids:
        await h.wait_for_status(nid, {"delivered"})
    assert len(mkt_ids) == 50


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

    # Wait until BOTH work messages (original + duplicate) have drained, rather
    # than guessing with a sleep — then the provider-call count is final.
    async def _work_drained():
        return await h.queue_message_count(rabbit.WORK_QUEUE) == 0

    await h.wait_until(_work_drained)

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

    # Wait until the timed retry copy has cycled back through the 5s tier and the
    # work queue (so it was processed and skipped) — no bare sleep.
    async def _retry_drained():
        work = await h.queue_message_count(rabbit.WORK_QUEUE)
        retry5s = await h.queue_message_count(rabbit.retry_queue_name("5s"))
        return work == 0 and retry5s == 0

    await h.wait_until(_retry_drained, timeout=20)

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

    # The terminally-failed message landed in the parking queue — poll for it.
    async def _parked():
        return await h.queue_message_count(rabbit.PARKING_QUEUE) >= 1

    await h.wait_until(_parked, timeout=20)


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


# 9. Email channel happy path --------------------------------------------- #
async def test_email_channel_delivered(harness_factory):
    h = await harness_factory(mode="always_deliver")
    resp = await _post(h.client, channel="email", message="hello mail", recipient_ids=["e1"])
    nid = resp.json()["recipients"][0]["id"]

    assert await h.wait_for_status(nid, {"delivered"}) == "delivered"

    # The EMAIL provider was selected and called; the SMS provider was not.
    email_calls = h.factory.for_channel("email").calls
    assert len(email_calls) == 1
    assert email_calls[0].notification_id == nid
    assert email_calls[0].recipient == "e1"
    assert h.factory.for_channel("sms").calls == []
    assert await h.history(nid) == ["queued", "sent", "delivered"]


# 10. Health endpoint ------------------------------------------------------ #
async def test_health_ok(harness_factory):
    h = await harness_factory(start_consumers=False)
    resp = await h.client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["components"]["database"]["ok"] is True
    assert body["components"]["broker"]["ok"] is True
    assert body["components"]["redis"]["ok"] is True


# 11. Unknown notification id -> 404 -------------------------------------- #
async def test_get_unknown_notification_404(harness_factory):
    h = await harness_factory(start_consumers=False)
    resp = await h.client.get(f"/api/v1/notifications/{uuid.uuid4()}")
    assert resp.status_code == 404


# 12. Rate limiter requeues (never drops) over the limit ------------------ #
async def test_rate_limited_marketing_is_requeued_not_dropped(harness_factory):
    # MARKETING SMS limit of 1/sec with 3 recipients: the dispatcher admits one
    # per window and requeues the rest to the 5s tier. None are dropped — all three
    # must still reach 'delivered', proving the rate-limit path requeues. (Only
    # marketing is shaped; transactional bypasses the limiter, see below.)
    h = await harness_factory(mode="always_deliver", rate_limit_sms_per_sec=1)
    resp = await _post(h.client, type_="marketing", recipient_ids=["r1", "r2", "r3"])
    ids = [r["id"] for r in resp.json()["recipients"]]

    for nid in ids:
        assert await h.wait_for_status(nid, {"delivered"}, timeout=20) == "delivered"
    assert len(ids) == 3


# 12b. Transactional bypasses the rate limiter ("без задержек") ------------ #
async def test_transactional_bypasses_rate_limiter(harness_factory, monkeypatch):
    # Transactional traffic must dispatch without delay, so the dispatcher must not
    # even consult the rate limiter for it (no requeue into the 5s tier). Spy on
    # rate_limiter.allow and assert it is never called, even with the limit at 1/sec.
    from notification_service.services import rate_limiter

    consulted: list[str] = []
    real_allow = rate_limiter.allow

    async def spy_allow(channel):
        consulted.append(channel)
        return await real_allow(channel)

    monkeypatch.setattr(rate_limiter, "allow", spy_allow)

    h = await harness_factory(mode="always_deliver", rate_limit_sms_per_sec=1)
    resp = await _post(h.client, type_="transactional", recipient_ids=["t1", "t2", "t3"])
    ids = [r["id"] for r in resp.json()["recipients"]]

    for nid in ids:
        assert await h.wait_for_status(nid, {"delivered"}, timeout=20) == "delivered"

    # The limiter was never consulted for the transactional sends.
    assert consulted == []


# 13. Reconciler redrives a stranded 'queued' row ------------------------- #
async def test_sweeper_redrives_stuck_queued(harness_factory):
    # Simulate the dual-write gap: rows are committed 'queued' but the work
    # message was never published (crash between commit and publish). The
    # reconciler must find and redrive them to delivery.
    from notification_service.workers import sweeper

    h = await harness_factory(mode="always_deliver")  # dispatcher + receipts live

    async with session_scope() as session:
        _, notifications = await repo.create_batch(
            session,
            channel="sms",
            type_="transactional",
            body="stranded",
            idempotency_key=None,
            recipient_ids=["stranded-1"],
        )
        await session.commit()
        nid = str(notifications[0].id)

    # No work message exists yet, so it stays 'queued'.
    assert (await h.get_notification(nid)).status == "queued"

    # One reconciliation pass (age threshold 0) must republish and drive it home.
    counts = await sweeper.sweep_once(h.topology.exchange, queued_after_seconds=0)
    assert counts["queued"] >= 1

    assert await h.wait_for_status(nid, {"delivered"}, timeout=20) == "delivered"
    calls = [c for c in h.factory.for_channel("sms").calls if c.notification_id == nid]
    assert len(calls) == 1
