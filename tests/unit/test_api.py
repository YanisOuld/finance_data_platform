from datetime import date

import pytest
from fastapi.testclient import TestClient

import src.api.routers.fundamentals as fundamentals_router
import src.api.routers.instruments as instruments_router
import src.api.routers.prices as prices_router
from src.core.database import get_db
from src.main import app


class _FakeInstrument:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


def _instrument(**overrides):
    defaults = {
        "id": 1,
        "ticker": "SOFI",
        "name": "SoFi Technologies",
        "exchange": "NMS",
        "currency": "USD",
        "timezone": "America/New_York",
        "is_active": True,
        "is_scheduled": True,
    }
    defaults.update(overrides)
    return _FakeInstrument(**defaults)


@pytest.fixture(autouse=True)
def _override_get_db():
    app.dependency_overrides[get_db] = lambda: iter([None])
    yield
    app.dependency_overrides.pop(get_db, None)


@pytest.fixture
def client():
    return TestClient(app)


def test_list_instruments(monkeypatch, client):
    monkeypatch.setattr(
        instruments_router, "list_instruments", lambda db, is_active=None, is_scheduled=None: [_instrument()]
    )

    resp = client.get("/instruments")

    assert resp.status_code == 200
    assert resp.json() == [
        {
            "id": 1,
            "ticker": "SOFI",
            "name": "SoFi Technologies",
            "exchange": "NMS",
            "currency": "USD",
            "timezone": "America/New_York",
            "is_active": True,
            "is_scheduled": True,
        }
    ]


def test_get_instrument_404(monkeypatch, client):
    monkeypatch.setattr(instruments_router, "get_instrument", lambda db, ticker: None)

    resp = client.get("/instruments/NOTREAL")

    assert resp.status_code == 404


def test_get_instrument_found(monkeypatch, client):
    monkeypatch.setattr(instruments_router, "get_instrument", lambda db, ticker: _instrument())

    resp = client.get("/instruments/SOFI")

    assert resp.status_code == 200
    assert resp.json()["ticker"] == "SOFI"


def test_create_instrument_returns_202_and_schedules_backfill(monkeypatch, client):
    monkeypatch.setattr(
        instruments_router,
        "validate_and_upsert_ticker",
        lambda ticker, is_scheduled=True: {
            "id": 1,
            "ticker": ticker,
            "name": "SoFi Technologies",
            "exchange": "NMS",
            "currency": "USD",
            "timezone": "America/New_York",
            "is_active": True,
            "is_scheduled": is_scheduled,
        },
    )
    called_with = {}
    monkeypatch.setattr(
        instruments_router,
        "register_ticker",
        lambda ticker, **kwargs: called_with.update(ticker=ticker, **kwargs),
    )

    resp = client.post("/instruments", json={"ticker": "sofi"})

    assert resp.status_code == 202
    assert resp.json()["ticker"] == "sofi"
    # the background task itself only runs after the response is sent in a
    # real server; TestClient runs it inline, so we can assert it was invoked.
    assert called_with["ticker"] == "sofi"


def test_create_instrument_rejects_invalid_ticker(monkeypatch, client):
    def _raise(ticker, is_scheduled=True):
        raise ValueError(f"'{ticker}' does not look like a real Yahoo Finance ticker")

    monkeypatch.setattr(instruments_router, "validate_and_upsert_ticker", _raise)

    resp = client.post("/instruments", json={"ticker": "NOTATICKER"})

    assert resp.status_code == 422


def test_update_scheduled_404(monkeypatch, client):
    monkeypatch.setattr(instruments_router, "set_scheduled", lambda db, ticker, is_scheduled: None)

    resp = client.patch("/instruments/NOTREAL/scheduled", json={"is_scheduled": False})

    assert resp.status_code == 404


def test_update_scheduled_ok(monkeypatch, client):
    monkeypatch.setattr(
        instruments_router,
        "set_scheduled",
        lambda db, ticker, is_scheduled: _instrument(is_scheduled=is_scheduled),
    )

    resp = client.patch("/instruments/SOFI/scheduled", json={"is_scheduled": False})

    assert resp.status_code == 200
    assert resp.json()["is_scheduled"] is False


def test_get_prices_404_for_unregistered_ticker(monkeypatch, client):
    monkeypatch.setattr(prices_router, "get_instrument", lambda db, ticker: None)

    resp = client.get("/prices/NOTREAL")

    assert resp.status_code == 404


def test_get_prices_returns_rows(monkeypatch, client):
    monkeypatch.setattr(prices_router, "get_instrument", lambda db, ticker: _instrument())
    row = _FakeInstrument(
        symbol="SOFI",
        ts=date(2026, 1, 2),
        open=10.0,
        high=11.0,
        low=9.5,
        close=10.5,
        volume=1000,
        close_returns=0.01,
    )
    monkeypatch.setattr(
        prices_router, "get_prices", lambda db, ticker, start=None, end=None, limit=500: [row]
    )

    resp = client.get("/prices/SOFI")

    assert resp.status_code == 200
    assert resp.json() == [
        {
            "symbol": "SOFI",
            "ts": "2026-01-02",
            "open": 10.0,
            "high": 11.0,
            "low": 9.5,
            "close": 10.5,
            "volume": 1000,
            "close_returns": 0.01,
        }
    ]


def test_get_fundamentals_404_for_unregistered_ticker(monkeypatch, client):
    monkeypatch.setattr(fundamentals_router, "get_instrument", lambda db, ticker: None)

    resp = client.get("/fundamentals/NOTREAL")

    assert resp.status_code == 404


def test_get_fundamentals_returns_rows(monkeypatch, client):
    monkeypatch.setattr(fundamentals_router, "get_instrument", lambda db, ticker: _instrument())
    row = _FakeInstrument(
        ticker="SOFI",
        concept="us-gaap:Revenues",
        unit="USD",
        period_end=date(2025, 12, 31),
        fy=2025,
        fp="FY",
        form="10-K",
        val=1000.0,
    )
    monkeypatch.setattr(
        fundamentals_router, "get_fundamentals", lambda db, ticker, concept=None, limit=500: [row]
    )

    resp = client.get("/fundamentals/SOFI")

    assert resp.status_code == 200
    assert resp.json()[0]["concept"] == "us-gaap:Revenues"
