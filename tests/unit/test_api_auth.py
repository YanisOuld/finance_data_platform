import pytest
from fastapi import HTTPException

import src.api.deps as deps
from src.api.deps import require_api_key


def test_local_env_without_api_key_allows_request(monkeypatch):
    monkeypatch.setattr(deps.settings, "environment", "local")
    monkeypatch.setattr(deps.settings, "api_key", None)

    require_api_key(x_api_key=None)  # should not raise


def test_non_local_env_without_api_key_fails_closed(monkeypatch):
    monkeypatch.setattr(deps.settings, "environment", "prod")
    monkeypatch.setattr(deps.settings, "api_key", None)

    with pytest.raises(HTTPException) as exc_info:
        require_api_key(x_api_key=None)

    assert exc_info.value.status_code == 500


def test_api_key_set_rejects_missing_or_wrong_header(monkeypatch):
    monkeypatch.setattr(deps.settings, "environment", "local")
    monkeypatch.setattr(deps.settings, "api_key", "secret123")

    with pytest.raises(HTTPException) as exc_info:
        require_api_key(x_api_key=None)
    assert exc_info.value.status_code == 401

    with pytest.raises(HTTPException) as exc_info:
        require_api_key(x_api_key="wrong")
    assert exc_info.value.status_code == 401


def test_api_key_set_accepts_matching_header(monkeypatch):
    monkeypatch.setattr(deps.settings, "environment", "prod")
    monkeypatch.setattr(deps.settings, "api_key", "secret123")

    require_api_key(x_api_key="secret123")  # should not raise
