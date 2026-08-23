"""
Thin, fail-open Redis cache for the API's read (GET) endpoints. Redis was
already deployed in docker-compose.yml (with its own healthcheck) but
nothing in the codebase used it -- this wires it in instead of leaving that
infra running for nothing.

Fail-open by design: any Redis error (down, unreachable, REDIS_URL unset)
just disables caching for that call -- it never turns into a 500 for the
caller. A cache is an optimization, not a dependency the API should die on.
"""

from __future__ import annotations

import json
from typing import Any

from src.core.config import settings
from src.core.logger import get_logger

logger = get_logger(__name__)

_client: Any = None
_client_checked = False


def get_redis_client() -> Any | None:
    global _client, _client_checked
    if _client_checked:
        return _client
    _client_checked = True

    if not settings.redis_url:
        return None

    try:
        import redis

        client = redis.Redis.from_url(settings.redis_url, socket_connect_timeout=1, socket_timeout=1)
        client.ping()
        _client = client
    except Exception as e:
        logger.warning("Redis unavailable, caching disabled: %s", e)
        _client = None

    return _client


def cache_get_json(key: str) -> Any | None:
    client = get_redis_client()
    if client is None:
        return None
    try:
        raw = client.get(key)
    except Exception as e:
        logger.warning("Redis GET failed for key=%s: %s", key, e)
        return None
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return None


def cache_set_json(key: str, value: Any, ttl_seconds: int = 60) -> None:
    client = get_redis_client()
    if client is None:
        return
    try:
        client.setex(key, ttl_seconds, json.dumps(value, default=str))
    except Exception as e:
        logger.warning("Redis SET failed for key=%s: %s", key, e)
