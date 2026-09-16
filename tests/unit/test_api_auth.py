import pytest
from fastapi import HTTPException

import src.api.deps as deps
from src.api.deps import require_admin_key, require_api_key


def test_local_env_without_api_key_allows_request(monkeypatch):
    monkeypatch.setattr(deps.settings, "environment", "local")
    monkeypatch.setattr(deps.settings, "api_key", None)

    require_api_key(db=None, x_api_key=None)  # should not raise


def test_non_local_env_without_api_key_fails_closed(monkeypatch):
    monkeypatch.setattr(deps.settings, "environment", "prod")
    monkeypatch.setattr(deps.settings, "api_key", None)

    with pytest.raises(HTTPException) as exc_info:
        require_api_key(db=None, x_api_key=None)

    assert exc_info.value.status_code == 500


def test_api_key_set_rejects_missing_or_wrong_header(monkeypatch):
    monkeypatch.setattr(deps.settings, "environment", "local")
    monkeypatch.setattr(deps.settings, "api_key", "secret123")
    # No DB key matches -- wrong header must be rejected, not looked up as valid.
    monkeypatch.setattr(deps, "get_active_by_hash", lambda db, key_hash: None)

    with pytest.raises(HTTPException) as exc_info:
        require_api_key(db=None, x_api_key=None)
    assert exc_info.value.status_code == 401

    with pytest.raises(HTTPException) as exc_info:
        require_api_key(db=None, x_api_key="wrong")
    assert exc_info.value.status_code == 401


def test_api_key_set_accepts_matching_admin_header(monkeypatch):
    monkeypatch.setattr(deps.settings, "environment", "prod")
    monkeypatch.setattr(deps.settings, "api_key", "secret123")

    require_api_key(db=None, x_api_key="secret123")  # should not raise


def test_accepts_active_db_managed_key(monkeypatch):
    monkeypatch.setattr(deps.settings, "environment", "prod")
    monkeypatch.setattr(deps.settings, "api_key", "admin-master")

    class _Row:
        id = 7

    seen = {}
    monkeypatch.setattr(deps, "get_active_by_hash", lambda db, key_hash: _Row())
    monkeypatch.setattr(deps, "touch_last_used", lambda db, key_id: seen.update(id=key_id))

    require_api_key(db=None, x_api_key="fdp_live_whatever")  # should not raise
    assert seen["id"] == 7  # last-used bookkeeping fired


def test_admin_key_required_rejects_db_key(monkeypatch):
    """A DB-managed key can read data but must NOT pass the admin guard."""
    monkeypatch.setattr(deps.settings, "environment", "prod")
    monkeypatch.setattr(deps.settings, "api_key", "admin-master")

    with pytest.raises(HTTPException) as exc_info:
        require_admin_key(x_api_key="fdp_live_somekey")
    assert exc_info.value.status_code == 401

    require_admin_key(x_api_key="admin-master")  # the master key passes
