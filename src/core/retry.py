"""Shared exponential backoff for the ingestion HTTP clients."""

from __future__ import annotations

import random
import time
from collections.abc import Callable
from typing import TypeVar

from src.core.logger import get_logger

logger = get_logger(__name__)

T = TypeVar("T")


def _retry_after_seconds(exc: Exception) -> float | None:
    """Seconds from a Retry-After header on the exception's response, if any
    (delta-seconds form only). Duck-typed so this module needs no requests import."""
    response = getattr(exc, "response", None)
    headers = getattr(response, "headers", None)
    if not headers:
        return None
    raw = headers.get("Retry-After")
    if raw is None:
        return None
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        return None


def call_with_backoff(
    fn: Callable[[], T],
    *,
    max_retries: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    retry_on: tuple[type[Exception], ...] = (Exception,),
    description: str = "call",
) -> T:
    """Call fn() with exponential backoff + jitter, retrying only exceptions in
    `retry_on` so non-transient errors fail immediately."""
    last_err: Exception | None = None
    for attempt in range(max_retries):
        try:
            return fn()
        except retry_on as e:
            last_err = e
            # Honor Retry-After (capped) if present, else exponential backoff.
            retry_after = _retry_after_seconds(e)
            if retry_after is not None:
                delay = min(max_delay, retry_after)
            else:
                delay = min(max_delay, base_delay * (2**attempt) + random.random())
            logger.warning(
                "%s failed attempt=%s/%s err=%s sleep=%.2fs", description, attempt + 1, max_retries, e, delay
            )
            time.sleep(delay)

    raise RuntimeError(f"{description} failed after {max_retries} retries: {last_err}") from last_err
