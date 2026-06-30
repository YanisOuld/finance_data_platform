import polars as pl
import pytest

from src.transformers.quality.checks import DataQualityError, check_prices_1d


def _valid_df(**overrides):
    base = {
        "symbol": ["AAPL", "AAPL"],
        "ts": ["2026-01-05", "2026-01-06"],
        "open": [10.0, 11.0],
        "high": [12.0, 13.0],
        "low": [9.0, 10.0],
        "close": [11.0, 12.0],
        "volume": [100, 200],
    }
    base.update(overrides)
    return pl.DataFrame(base)


def test_check_prices_1d_passes_on_clean_data():
    report = check_prices_1d(_valid_df())
    assert report.row_count == 2
    assert report.warnings == []


def test_check_prices_1d_rejects_empty_frame():
    empty = pl.DataFrame(
        schema={
            "symbol": pl.Utf8,
            "ts": pl.Utf8,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "volume": pl.Int64,
        }
    )
    with pytest.raises(DataQualityError):
        check_prices_1d(empty)


def test_check_prices_1d_rejects_duplicate_keys():
    df = _valid_df(symbol=["AAPL", "AAPL"], ts=["2026-01-05", "2026-01-05"])
    with pytest.raises(DataQualityError):
        check_prices_1d(df)


def test_check_prices_1d_rejects_negative_prices():
    df = _valid_df(close=[-1.0, 12.0])
    with pytest.raises(DataQualityError):
        check_prices_1d(df)


def test_check_prices_1d_rejects_too_many_null_closes():
    df = _valid_df(close=[None, None])
    with pytest.raises(DataQualityError):
        check_prices_1d(df)
