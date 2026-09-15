import pytest

from src.core.retry import call_with_backoff


def test_call_with_backoff_returns_on_first_success():
    calls = []

    def fn():
        calls.append(1)
        return "ok"

    result = call_with_backoff(fn, max_retries=3, base_delay=0)

    assert result == "ok"
    assert len(calls) == 1


def test_call_with_backoff_retries_then_succeeds(monkeypatch):
    monkeypatch.setattr("src.core.retry.time.sleep", lambda _seconds: None)
    attempts = {"count": 0}

    def fn():
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise ValueError("transient")
        return "ok"

    result = call_with_backoff(fn, max_retries=5, base_delay=0, retry_on=(ValueError,))

    assert result == "ok"
    assert attempts["count"] == 3


def test_call_with_backoff_raises_after_exhausting_retries(monkeypatch):
    monkeypatch.setattr("src.core.retry.time.sleep", lambda _seconds: None)

    def fn():
        raise ValueError("always fails")

    with pytest.raises(RuntimeError, match="failed after 2 retries"):
        call_with_backoff(fn, max_retries=2, base_delay=0, retry_on=(ValueError,))


def test_call_with_backoff_honors_retry_after_header(monkeypatch):
    """A 429/503 carrying Retry-After should make us sleep exactly that long
    (capped at max_delay), not the exponential-backoff amount."""
    slept: list[float] = []
    monkeypatch.setattr("src.core.retry.time.sleep", lambda s: slept.append(s))

    class _Resp:
        headers = {"Retry-After": "7"}

    class _HttpError(Exception):
        response = _Resp()

    attempts = {"count": 0}

    def fn():
        attempts["count"] += 1
        if attempts["count"] < 2:
            raise _HttpError("rate limited")
        return "ok"

    result = call_with_backoff(fn, max_retries=5, base_delay=1.0, max_delay=60.0, retry_on=(_HttpError,))

    assert result == "ok"
    assert slept == [7.0]


def test_call_with_backoff_does_not_retry_unmatched_exceptions(monkeypatch):
    monkeypatch.setattr("src.core.retry.time.sleep", lambda _seconds: None)
    calls = []

    def fn():
        calls.append(1)
        raise TypeError("not retryable")

    with pytest.raises(TypeError):
        call_with_backoff(fn, max_retries=5, base_delay=0, retry_on=(ValueError,))

    assert len(calls) == 1
