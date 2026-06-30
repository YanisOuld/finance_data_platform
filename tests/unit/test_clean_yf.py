from src.transformers.silver.clean_yf import normalize_history, clean_bronze


def _bronze_envelope(rows, errors=None):
    return {
        "meta": {"source": "yahoo", "dataset": "history", "params": {"start": "2026-01-01", "end": "2026-01-02"}},
        "payload": {"start": "2026-01-01", "end": "2026-01-02", "rows": rows, "errors": errors or []},
    }


def test_normalize_history_matches_actual_bronze_schema():
    raw = _bronze_envelope(
        rows=[
            {
                "ts": "2026-01-06 00:00:00-05:00",
                "open": 10.0,
                "high": 11.0,
                "low": 9.5,
                "close": 10.5,
                "volume": 1000,
                "symbol": "AAPL",
            }
        ]
    )

    records = normalize_history(raw)

    assert records == [
        {
            "symbol": "AAPL",
            "ts": "2026-01-06",
            "open": 10.0,
            "high": 11.0,
            "low": 9.5,
            "close": 10.5,
            "volume": 1000,
        }
    ]


def test_normalize_history_skips_rows_missing_ts_or_symbol():
    raw = _bronze_envelope(rows=[{"ts": None, "symbol": "AAPL"}, {"ts": "2026-01-06", "symbol": None}])
    assert normalize_history(raw) == []


def test_normalize_history_empty_rows_returns_empty_list():
    raw = _bronze_envelope(rows=[], errors=[{"symbol": "ZZZ", "error": "no_rows_returned"}])
    assert normalize_history(raw) == []


def test_clean_bronze_sorts_and_dedupes_per_symbol():
    rows = [
        {"symbol": "MSFT", "ts": "2026-01-07", "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1},
        {"symbol": "AAPL", "ts": "2026-01-06", "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1},
        {"symbol": "AAPL", "ts": "2026-01-06", "open": 2, "high": 2, "low": 2, "close": 2, "volume": 2},
        {"symbol": "AAPL", "ts": "2026-01-07", "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1},
    ]

    df = clean_bronze(rows)

    assert df.height == 3
    assert df["symbol"].to_list() == ["AAPL", "AAPL", "MSFT"]
    # the duplicate (AAPL, 2026-01-06) keeps the *last* occurrence (close=2)
    assert df.filter(df["ts"].dt.strftime("%Y-%m-%d") == "2026-01-06")["close"].to_list() == [2]


def test_clean_bronze_empty_rows_returns_typed_empty_frame():
    df = clean_bronze([])
    assert df.height == 0
    assert set(df.columns) == {"symbol", "ts", "open", "high", "low", "close", "volume"}
