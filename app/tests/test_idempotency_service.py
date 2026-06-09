"""Unit tests for the idempotency service (Redis fast-path layer, invariant 4)."""

from __future__ import annotations

import pytest

from notification_service.services import idempotency

pytestmark = pytest.mark.asyncio


async def test_claim_is_exclusive(db_session):
    # db_session fixture flushes Redis, giving us a clean keyspace.
    key = "abc-123"
    assert await idempotency.claim(key) is True  # first claim wins
    assert await idempotency.claim(key) is False  # duplicate sees the key
    assert await idempotency.claim("other") is True  # unrelated key independent


async def test_release_allows_reclaim(db_session):
    key = "release-me"
    assert await idempotency.claim(key) is True
    await idempotency.release(key)
    assert await idempotency.claim(key) is True  # reclaimable after release


async def test_claim_sets_ttl(db_session):
    key = "ttl-key"
    await idempotency.claim(key)
    ttl = await idempotency.get_redis().ttl(idempotency.redis_key(key))
    assert ttl > 0
