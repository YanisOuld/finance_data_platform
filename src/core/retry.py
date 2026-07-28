"""
Shared exponential backoff, factored out of the retry loop that already
existed ad-hoc in src/ingestion/clients/yahoo_client.py (fetch_prices_1d_safe,
fetch_info). fred_client.py, sec_edgar_client.py and openfigi_client.py each
made a single unretried request -- one transient network blip failed the
whole ingestion_runs row instead of just retrying.
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable
from typing import TypeVar

from src.core.logger import get_logger

logger = get_logger(__name__)

T = TypeVar("T")


def call_with_backoff(
    fn: Callable[[], T],
    *,
    max_retries: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    retry_on: tuple[type[Exception], ...] = (Exception,),
    description: str = "call",
) -> T:
    """Call fn() with exponential backoff + jitter, retrying only on
    exceptions matching `retry_on` (e.g. requests.RequestException) so
    non-transient errors -- a bad ticker, a malformed response -- fail
    immediately instead of being retried pointlessly.
    """
    last_err: Exception | None = None
    for attempt in range(max_retries):
        try:
            return fn()
        except retry_on as e:
            last_err = e
            delay = min(max_delay, base_delay * (2**attempt) + random.random())
            logger.warning(
                "%s failed attempt=%s/%s err=%s sleep=%.2fs", description, attempt + 1, max_retries, e, delay
            )
            time.sleep(delay)

    raise RuntimeError(f"{description} failed after {max_retries} retries: {last_err}") from last_err
