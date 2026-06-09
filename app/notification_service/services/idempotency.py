"""Two-layer request idempotency (invariant 4).

Layer 1 (fast path): Redis ``SETNX idemp:{key}`` with a 24h TTL. The first
request to claim a key proceeds; concurrent/duplicate requests see the key.

Layer 2 (source of truth): a UNIQUE constraint on ``batches.idempotency_key``.
Redis is a cache and may be flushed, so the database constraint is authoritative.
On an IntegrityError the service loads and returns the original batch.

This module only provides the Redis primitives + key helper; the batch-creation
flow in ``services/notification_service.py`` ties the two layers together.
"""

from __future__ import annotations

import redis.asyncio as redis

from notification_service.config import get_settings

_KEY_PREFIX = "idemp:"


def redis_key(idempotency_key: str) -> str:
    return f"{_KEY_PREFIX}{idempotency_key}"


_client: redis.Redis | None = None


def get_redis() -> redis.Redis:
    global _client
    if _client is None:
        settings = get_settings()
        _client = redis.from_url(settings.effective_redis_url, decode_responses=True)
    return _client


async def claim(idempotency_key: str) -> bool:
    """Try to claim the key. Returns True if newly claimed, False if already seen.

    Best-effort: if Redis is unreachable we fail open (return True) and rely on
    the database UNIQUE constraint to enforce idempotency.
    """
    settings = get_settings()
    try:
        acquired = await get_redis().set(
            redis_key(idempotency_key),
            "1",
            nx=True,
            ex=settings.idempotency_ttl_seconds,
        )
        return bool(acquired)
    except redis.RedisError:
        return True


async def release(idempotency_key: str) -> None:
    """Release a claim (used when batch creation fails after claiming)."""
    try:
        await get_redis().delete(redis_key(idempotency_key))
    except redis.RedisError:
        pass


async def close() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None
