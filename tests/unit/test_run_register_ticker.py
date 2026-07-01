import pytest

import src.orchestration.pipelines.run_register_ticker as run_register_ticker
from src.orchestration.pipelines.run_register_ticker import fetch_ticker_info, register_ticker


def test_fetch_ticker_info_rejects_empty_info(monkeypatch):
    monkeypatch.setattr(
        run_register_ticker, "ingest_yahoo_info_to_bronze", lambda bucket, symbol: "s3://bucket/key.json.gz"
    )
    monkeypatch.setattr(
        run_register_ticker, "fetch_json_from_bronze", lambda bucket, key: {"meta": {}, "payload": {}}
    )

    with pytest.raises(ValueError, match="does not look like a real Yahoo Finance ticker"):
        fetch_ticker_info("bucket", "NOTATICKER")


def test_fetch_ticker_info_returns_payload_for_real_ticker(monkeypatch):
    info = {"exchange": "NMS", "quoteType": "EQUITY", "timezone": "America/New_York", "currency": "USD"}
    monkeypatch.setattr(
        run_register_ticker, "ingest_yahoo_info_to_bronze", lambda bucket, symbol: "s3://bucket/key.json.gz"
    )
    monkeypatch.setattr(
        run_register_ticker, "fetch_json_from_bronze", lambda bucket, key: {"meta": {}, "payload": info}
    )

    assert fetch_ticker_info("bucket", "SOFI") == info


def test_register_ticker_rejects_blank_ticker():
    with pytest.raises(ValueError, match="ticker is required"):
        register_ticker("   ")


def test_register_ticker_raises_on_missing_metadata_fields(monkeypatch):
    # start_run/finish_run hit Postgres -- stub them out so this stays a pure unit test.
    monkeypatch.setattr(run_register_ticker, "start_run", lambda session, dataset, run_date: "run-id")
    monkeypatch.setattr(run_register_ticker, "finish_run", lambda *a, **k: None)
    monkeypatch.setattr(
        run_register_ticker,
        "fetch_ticker_info",
        lambda bucket, ticker: {"exchange": "NMS"},  # missing quoteType/timezone/currency
    )

    with pytest.raises(ValueError, match="missing required metadata field"):
        register_ticker("SOFI")
