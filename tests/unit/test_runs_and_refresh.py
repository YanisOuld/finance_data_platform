from datetime import date, datetime

import pytest
from fastapi.testclient import TestClient

import src.api.routes.instruments as instruments_router
import src.api.routes.runs as runs_router
from src.core.database import get_db
from src.main import app


class _Obj:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


@pytest.fixture(autouse=True)
def _override_get_db():
    app.dependency_overrides[get_db] = lambda: iter([None])
    yield
    app.dependency_overrides.pop(get_db, None)


@pytest.fixture(autouse=True)
def _no_side_effects(monkeypatch):
    """Airflow is unconfigured in tests, so triggers fall to in-process
    BackgroundTasks -- TestClient runs those inline, so stub them out. Locks
    default to acquired (Redis unset -> fail open) unless a test overrides."""
    monkeypatch.setattr(instruments_router, "register_ticker", lambda *a, **k: None)
    monkeypatch.setattr(instruments_router, "run_fundamentals_pipeline", lambda *a, **k: None)


@pytest.fixture
def client():
    return TestClient(app)


def _run(**overrides):
    defaults = {
        "run_id": "abc123",
        "dataset": "prices_1d",
        "run_date": date(2026, 1, 2),
        "status": "success",
        "items_total": 1,
        "items_success": 1,
        "items_failed": 0,
        "started_at": datetime(2026, 1, 2, 9, 0, 0),
        "finished_at": datetime(2026, 1, 2, 9, 1, 0),
        "notes": None,
    }
    defaults.update(overrides)
    return _Obj(**defaults)


def test_list_runs_returns_rows_and_total(monkeypatch, client):
    monkeypatch.setattr(
        runs_router, "list_runs", lambda db, dataset=None, status=None, limit=50, offset=0: [_run()]
    )
    monkeypatch.setattr(runs_router, "count_runs", lambda db, dataset=None, status=None: 1)

    resp = client.get("/v1/runs")

    assert resp.status_code == 200
    assert resp.headers["X-Total-Count"] == "1"
    assert resp.json()[0]["run_id"] == "abc123"


def test_get_run_404(monkeypatch, client):
    monkeypatch.setattr(runs_router, "get_run", lambda db, run_id: None)

    resp = client.get("/v1/runs/nope")

    assert resp.status_code == 404


def test_refresh_404_for_unregistered_ticker(monkeypatch, client):
    monkeypatch.setattr(instruments_router, "get_instrument", lambda db, ticker: None)

    resp = client.post("/v1/instruments/NOPE/refresh", json={})

    assert resp.status_code == 404


def test_refresh_triggers_requested_datasets(monkeypatch, client):
    monkeypatch.setattr(instruments_router, "get_instrument", lambda db, ticker: _Obj(is_scheduled=True))

    resp = client.post("/v1/instruments/sofi/refresh", json={"datasets": ["prices"]})

    assert resp.status_code == 202
    body = resp.json()
    assert body["ticker"] == "SOFI"
    assert [j["dataset"] for j in body["triggered"]] == ["prices"]
    assert body["triggered"][0]["executor"] == "in_process"


def test_refresh_409_when_all_datasets_already_in_flight(monkeypatch, client):
    monkeypatch.setattr(instruments_router, "get_instrument", lambda db, ticker: _Obj(is_scheduled=True))
    # Lock already held for every dataset -> nothing new to trigger.
    monkeypatch.setattr(instruments_router, "try_acquire_lock", lambda name, ttl_seconds: False)

    resp = client.post("/v1/instruments/SOFI/refresh", json={})

    assert resp.status_code == 409
