"""Outbound per-channel rate limiting via Redis (sliding-window sorted set).

Keys ``rate:{channel}`` hold a sorted set of recent send timestamps (score =
unix millis). On each acquire we drop entries older than one second, count the
window, and admit if under the channel's per-second limit. Over the limit the
dispatcher requeues the message to the 5s retry tier — never drops it.

If Redis is unreachable the limiter fails open (admits) so a Redis blip cannot
halt all delivery; correctness of delivery does not depend on the limiter.
"""

from __future__ import annotations

import time

import redis.asyncio as redis

from notification_service.config import get_settings
from notification_service.services.idempotency import get_redis

_WINDOW_MS = 1000

# Atomic check-and-add: trim the window, count, and only add the new timestamp if
# under the limit. Returns 1 if admitted, 0 if rate-limited.
_LUA = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
local count = redis.call('ZCARD', key)
if count < limit then
    redis.call('ZADD', key, now, now .. '-' .. math.random(1000000))
    redis.call('PEXPIRE', key, window)
    return 1
end
return 0
"""


def _limit_for(channel: str) -> int:
    settings = get_settings()
    return (
        settings.rate_limit_sms_per_sec if channel == "sms" else settings.rate_limit_email_per_sec
    )


async def allow(channel: str) -> bool:
    """Return True if a send on `channel` is admitted under the rate limit."""
    limit = _limit_for(channel)
    now_ms = int(time.time() * 1000)
    try:
        admitted = await get_redis().eval(_LUA, 1, f"rate:{channel}", now_ms, _WINDOW_MS, limit)
        return bool(admitted)
    except redis.RedisError:
        return True
