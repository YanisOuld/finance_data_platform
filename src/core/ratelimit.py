"""Redis-backed rate limiting + dedup locks that fail open.

Same philosophy as src/core/cache.py: a Redis outage must never turn into a
500. When Redis is unreachable the limiter allows the call and the lock is
granted -- these are guards/optimizations, not hard dependencies. Rate limits
are per-key fixed windows; without a shared store (Redis) they can't be
enforced across uvicorn workers anyway, so failing open is the honest default.
"""

from __future__ import annotations

from fastapi import HTTPException

from src.core.cache import get_redis_client
from src.core.logger import get_logger

logger = get_logger(__name__)


def enforce_rate_limit(identity: str, bucket: str, limit: int, window_seconds: int) -> None:
    """Fixed-window counter keyed on (bucket, identity). Raises HTTP 429 once
    more than `limit` requests land in the same window. A no-op when limit <= 0
    (disabled) or Redis is unavailable (fail open)."""
    if limit <= 0:
        return
    client = get_redis_client()
    if client is None:
        return

    key = f"ratelimit:{bucket}:{identity}"
    try:
        count = client.incr(key)
        if count == 1:
            # Stamp the window's TTL on first hit. The tiny crash-window where
            # the process dies between INCR and EXPIRE (leaving a key with no
            # TTL) is acceptable given the fail-open contract.
            client.expire(key, window_seconds)
    except Exception as e:
        logger.warning("Rate-limit check failed (allowing request) for %s: %s", key, e)
        return

    if count > limit:
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded; slow down",
            headers={"Retry-After": str(window_seconds)},
        )


def try_acquire_lock(name: str, ttl_seconds: int) -> bool:
    """Best-effort dedup lock via SET NX EX. Returns True when the lock is
    acquired -- and also when Redis is down (fail open: we can't dedup without
    a shared store, so we'd rather double-run than block a legitimate call).
    Returns False only when the key already exists (a run is already in flight).
    The lock self-expires after ttl_seconds; it is never explicitly released, so
    it doubles as a debounce window."""
    client = get_redis_client()
    if client is None:
        return True
    try:
        return bool(client.set(f"lock:{name}", "1", nx=True, ex=ttl_seconds))
    except Exception as e:
        logger.warning("Lock acquire failed (allowing) for %s: %s", name, e)
        return True
