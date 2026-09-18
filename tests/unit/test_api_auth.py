import pytest
from fastapi import HTTPException

import src.api.deps as deps
from src.api.deps import (
    Principal,
    authenticate,
    require_admin_key,
    require_read,
    require_write,
)


def _principal(scopes, is_admin=False):
    return Principal(identity="k", scopes=frozenset(scopes), is_admin=is_admin)


def test_local_env_without_api_key_authenticates_as_admin(monkeypatch):
    monkeypatch.setattr(deps.settings, "environment", "local")
    monkeypatch.setattr(deps.settings, "api_key", None)

    principal = authenticate(db=None, x_api_key=None)  # type: ignore[arg-type]

    assert principal.is_admin is True
    assert {"read", "write"}.issubset(principal.scopes)


def test_non_local_env_without_api_key_fails_closed(monkeypatch):
    monkeypatch.setattr(deps.settings, "environment", "prod")
    monkeypatch.setattr(deps.settings, "api_key", None)

    with pytest.raises(HTTPException) as exc_info:
        authenticate(db=None, x_api_key=None)  # type: ignore[arg-type]

    assert exc_info.value.status_code == 500


def test_api_key_set_rejects_missing_or_wrong_header(monkeypatch):
    monkeypatch.setattr(deps.settings, "environment", "local")
    monkeypatch.setattr(deps.settings, "api_key", "secret123")
    # No DB key matches -- wrong header must be rejected, not looked up as valid.
    monkeypatch.setattr(deps, "get_active_by_hash", lambda db, key_hash: None)

    with pytest.raises(HTTPException) as exc_info:
        authenticate(db=None, x_api_key=None)  # type: ignore[arg-type]
    assert exc_info.value.status_code == 401

    with pytest.raises(HTTPException) as exc_info:
        authenticate(db=None, x_api_key="wrong")  # type: ignore[arg-type]
    assert exc_info.value.status_code == 401


def test_api_key_set_accepts_matching_admin_header(monkeypatch):
    monkeypatch.setattr(deps.settings, "environment", "prod")
    monkeypatch.setattr(deps.settings, "api_key", "secret123")

    principal = authenticate(db=None, x_api_key="secret123")  # type: ignore[arg-type]

    assert principal.is_admin is True


def test_accepts_active_db_managed_key_with_its_scopes(monkeypatch):
    monkeypatch.setattr(deps.settings, "environment", "prod")
    monkeypatch.setattr(deps.settings, "api_key", "admin-master")

    class _Row:
        id = 7
        key_hash = "deadbeef"
        scopes = "read"

    seen = {}
    monkeypatch.setattr(deps, "get_active_by_hash", lambda db, key_hash: _Row())
    monkeypatch.setattr(deps, "touch_last_used", lambda db, key_id: seen.update(id=key_id))

    principal = authenticate(db=None, x_api_key="fdp_live_whatever")  # type: ignore[arg-type]

    assert seen["id"] == 7  # last-used bookkeeping fired
    assert principal.is_admin is False
    assert principal.scopes == frozenset({"read"})


def test_require_read_enforces_read_scope():
    # A read-scoped principal passes; a write-only principal is 403'd.
    assert require_read(_principal({"read"})).scopes == frozenset({"read"})

    with pytest.raises(HTTPException) as exc_info:
        require_read(_principal({"write"}))
    assert exc_info.value.status_code == 403


def test_require_write_enforces_write_scope(monkeypatch):
    # Redis unset -> rate limit is a no-op, so this exercises the scope gate only.
    assert require_write(_principal({"read", "write"})).scopes >= {"write"}

    with pytest.raises(HTTPException) as exc_info:
        require_write(_principal({"read"}))
    assert exc_info.value.status_code == 403


def test_admin_key_required_rejects_db_key(monkeypatch):
    """A DB-managed key can read data but must NOT pass the admin guard."""
    monkeypatch.setattr(deps.settings, "environment", "prod")
    monkeypatch.setattr(deps.settings, "api_key", "admin-master")

    with pytest.raises(HTTPException) as exc_info:
        require_admin_key(x_api_key="fdp_live_somekey")
    assert exc_info.value.status_code == 401

    require_admin_key(x_api_key="admin-master")  # the master key passes
