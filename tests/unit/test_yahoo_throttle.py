import src.ingestion.clients.yahoo_client as yahoo_client


def test_throttle_sleeps_when_called_too_soon(monkeypatch):
    monkeypatch.setattr(yahoo_client, "_last_call_at", 100.0)
    monkeypatch.setattr(yahoo_client.time, "monotonic", lambda: 100.5)  # only 0.5s since last call

    slept = []
    monkeypatch.setattr(yahoo_client.time, "sleep", lambda s: slept.append(s))

    yahoo_client._throttle_yahoo_calls()

    assert len(slept) == 1
    assert slept[0] == yahoo_client._MIN_SECONDS_BETWEEN_CALLS - 0.5


def test_throttle_does_not_sleep_when_enough_time_has_passed(monkeypatch):
    monkeypatch.setattr(yahoo_client, "_last_call_at", 100.0)
    monkeypatch.setattr(
        yahoo_client.time, "monotonic", lambda: 100.0 + yahoo_client._MIN_SECONDS_BETWEEN_CALLS + 1
    )

    slept = []
    monkeypatch.setattr(yahoo_client.time, "sleep", lambda s: slept.append(s))

    yahoo_client._throttle_yahoo_calls()

    assert slept == []


def test_throttle_updates_last_call_timestamp(monkeypatch):
    monkeypatch.setattr(yahoo_client, "_last_call_at", 0.0)
    monkeypatch.setattr(yahoo_client.time, "monotonic", lambda: 42.0)
    monkeypatch.setattr(yahoo_client.time, "sleep", lambda s: None)

    yahoo_client._throttle_yahoo_calls()

    assert yahoo_client._last_call_at == 42.0
