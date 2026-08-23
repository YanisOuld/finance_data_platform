import src.core.cache as cache


def _reset_cache_singleton(monkeypatch):
    monkeypatch.setattr(cache, "_client", None)
    monkeypatch.setattr(cache, "_client_checked", False)


def test_get_redis_client_returns_none_when_redis_url_unset(monkeypatch):
    _reset_cache_singleton(monkeypatch)
    monkeypatch.setattr(cache.settings, "redis_url", None)

    assert cache.get_redis_client() is None


def test_get_redis_client_fails_open_when_unreachable(monkeypatch):
    _reset_cache_singleton(monkeypatch)
    monkeypatch.setattr(cache.settings, "redis_url", "redis://localhost:0/0")  # port 0 -- never connects

    assert cache.get_redis_client() is None


def test_cache_get_json_returns_none_without_a_client(monkeypatch):
    monkeypatch.setattr(cache, "get_redis_client", lambda: None)

    assert cache.cache_get_json("some-key") is None


def test_cache_set_json_is_a_noop_without_a_client(monkeypatch):
    monkeypatch.setattr(cache, "get_redis_client", lambda: None)

    cache.cache_set_json("some-key", {"a": 1})  # should not raise


def test_cache_roundtrip_with_a_fake_client(monkeypatch):
    store: dict[str, str] = {}

    class _FakeRedis:
        def get(self, key):
            return store.get(key)

        def setex(self, key, ttl, value):
            store[key] = value

    monkeypatch.setattr(cache, "get_redis_client", lambda: _FakeRedis())

    cache.cache_set_json("prices:AAPL", {"rows": [1, 2, 3], "total": 3}, ttl_seconds=30)

    assert cache.cache_get_json("prices:AAPL") == {"rows": [1, 2, 3], "total": 3}
    assert cache.cache_get_json("missing-key") is None


def test_cache_get_json_returns_none_on_malformed_payload(monkeypatch):
    class _FakeRedis:
        def get(self, key):
            return b"not-json"

    monkeypatch.setattr(cache, "get_redis_client", lambda: _FakeRedis())

    assert cache.cache_get_json("some-key") is None


def test_cache_get_json_fails_open_on_redis_error(monkeypatch):
    class _FakeRedis:
        def get(self, key):
            raise ConnectionError("connection reset")

    monkeypatch.setattr(cache, "get_redis_client", lambda: _FakeRedis())

    assert cache.cache_get_json("some-key") is None


def test_cache_set_json_fails_open_on_redis_error(monkeypatch):
    class _FakeRedis:
        def setex(self, key, ttl, value):
            raise ConnectionError("connection reset")

    monkeypatch.setattr(cache, "get_redis_client", lambda: _FakeRedis())

    cache.cache_set_json("some-key", {"a": 1})  # should not raise
