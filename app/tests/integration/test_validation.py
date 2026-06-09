"""Structural input-validation tests for POST /api/v1/notifications.

recipient_ids are opaque identifiers — these assert structural rules only
(trim / non-empty / length / dedup / count) and the Idempotency-Key header
rules, not any phone/email format.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.asyncio

_VALID = {
    "channel": "sms",
    "type": "transactional",
    "message": "hello",
    "recipient_ids": ["a"],
}


def _body(**overrides):
    return {**_VALID, **overrides}


async def _post(client, body, headers=None):
    return await client.post("/api/v1/notifications", json=body, headers=headers or {})


# --- 422 cases ------------------------------------------------------------ #
async def test_invalid_channel_422(harness_factory):
    h = await harness_factory(start_consumers=False)
    assert (await _post(h.client, _body(channel="carrier"))).status_code == 422


async def test_invalid_type_422(harness_factory):
    h = await harness_factory(start_consumers=False)
    assert (await _post(h.client, _body(type="promo"))).status_code == 422


async def test_missing_required_field_422(harness_factory):
    h = await harness_factory(start_consumers=False)
    body = _body()
    del body["recipient_ids"]
    assert (await _post(h.client, body)).status_code == 422


async def test_empty_message_422(harness_factory):
    h = await harness_factory(start_consumers=False)
    assert (await _post(h.client, _body(message=""))).status_code == 422


async def test_whitespace_message_422(harness_factory):
    h = await harness_factory(start_consumers=False)
    assert (await _post(h.client, _body(message="    "))).status_code == 422


async def test_message_too_long_422(harness_factory):
    h = await harness_factory(start_consumers=False)
    assert (await _post(h.client, _body(message="x" * 1001))).status_code == 422


async def test_empty_recipient_ids_422(harness_factory):
    h = await harness_factory(start_consumers=False)
    assert (await _post(h.client, _body(recipient_ids=[]))).status_code == 422


async def test_blank_recipient_entry_422(harness_factory):
    h = await harness_factory(start_consumers=False)
    assert (await _post(h.client, _body(recipient_ids=["a", "   "]))).status_code == 422


async def test_recipient_id_too_long_422(harness_factory):
    h = await harness_factory(start_consumers=False)
    assert (await _post(h.client, _body(recipient_ids=["x" * 129]))).status_code == 422


async def test_too_many_recipients_422(harness_factory):
    h = await harness_factory(start_consumers=False, max_recipients=2)
    assert (await _post(h.client, _body(recipient_ids=["a", "b", "c"]))).status_code == 422


async def test_blank_idempotency_key_422(harness_factory):
    h = await harness_factory(start_consumers=False)
    resp = await _post(h.client, _body(), headers={"Idempotency-Key": "   "})
    assert resp.status_code == 422


async def test_idempotency_key_too_long_422(harness_factory):
    h = await harness_factory(start_consumers=False)
    resp = await _post(h.client, _body(), headers={"Idempotency-Key": "k" * 256})
    assert resp.status_code == 422


# --- accepted cases with normalization ------------------------------------ #
async def test_duplicate_recipients_deduplicated(harness_factory):
    h = await harness_factory(start_consumers=False)
    resp = await _post(h.client, _body(recipient_ids=["a", "a", "b"]))
    assert resp.status_code == 202
    body = resp.json()
    assert body["total"] == 2
    assert [r["subscriber_id"] for r in body["recipients"]] == ["a", "b"]


async def test_recipient_ids_trimmed(harness_factory):
    h = await harness_factory(start_consumers=False)
    resp = await _post(h.client, _body(recipient_ids=["  spaced  "]))
    assert resp.status_code == 202
    body = resp.json()
    assert body["recipients"][0]["subscriber_id"] == "spaced"
