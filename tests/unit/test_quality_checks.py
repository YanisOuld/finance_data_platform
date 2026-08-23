import polars as pl
import pytest

from src.transformers.quality.checks import (
    DataQualityError,
    check_fundamentals,
    check_instrument_figi,
    check_macro_series,
    check_prices_1d,
)


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


# --- macro_series -----------------------------------------------------------


def _valid_macro_df(**overrides):
    base = {"series": ["cpi", "cpi"], "ts": ["2026-01-01", "2026-02-01"], "value": [3.1, 3.2]}
    base.update(overrides)
    return pl.DataFrame(base)


def test_check_macro_series_passes_on_clean_data():
    report = check_macro_series(_valid_macro_df())
    assert report.row_count == 2
    assert report.warnings == []


def test_check_macro_series_rejects_empty_frame():
    empty = pl.DataFrame(schema={"series": pl.Utf8, "ts": pl.Utf8, "value": pl.Float64})
    with pytest.raises(DataQualityError):
        check_macro_series(empty)


def test_check_macro_series_rejects_duplicate_keys():
    df = _valid_macro_df(ts=["2026-01-01", "2026-01-01"])
    with pytest.raises(DataQualityError):
        check_macro_series(df)


def test_check_macro_series_rejects_too_many_null_values():
    df = _valid_macro_df(value=[None, None])
    with pytest.raises(DataQualityError):
        check_macro_series(df)


# --- fundamentals -------------------------------------------------------------


def _valid_fundamentals_df(**overrides):
    base = {
        "ticker": ["SOFI", "SOFI"],
        "concept": ["us-gaap:Revenues", "us-gaap:NetIncomeLoss"],
        "unit": ["USD", "USD"],
        "period_end": ["2025-12-31", "2025-12-31"],
        "fp": ["FY", "FY"],
        "form": ["10-K", "10-K"],
        "val": [1000.0, 100.0],
    }
    base.update(overrides)
    return pl.DataFrame(base)


def test_check_fundamentals_passes_on_clean_data():
    report = check_fundamentals(_valid_fundamentals_df())
    assert report.row_count == 2
    assert report.warnings == []


def test_check_fundamentals_rejects_empty_frame():
    empty = pl.DataFrame(
        schema={
            "ticker": pl.Utf8,
            "concept": pl.Utf8,
            "unit": pl.Utf8,
            "period_end": pl.Utf8,
            "fp": pl.Utf8,
            "form": pl.Utf8,
            "val": pl.Float64,
        }
    )
    with pytest.raises(DataQualityError):
        check_fundamentals(empty)


def test_check_fundamentals_rejects_duplicate_keys():
    df = _valid_fundamentals_df(concept=["us-gaap:Revenues", "us-gaap:Revenues"])
    with pytest.raises(DataQualityError):
        check_fundamentals(df)


def test_check_fundamentals_rejects_null_val():
    df = _valid_fundamentals_df(val=[None, 100.0])
    with pytest.raises(DataQualityError):
        check_fundamentals(df)


# --- instrument_figi ----------------------------------------------------------


def _valid_figi_df(**overrides):
    base = {"ticker": ["SOFI", "SOFI"], "figi": ["FIGI1", "FIGI2"]}
    base.update(overrides)
    return pl.DataFrame(base)


def test_check_instrument_figi_passes_on_clean_data():
    report = check_instrument_figi(_valid_figi_df())
    assert report.row_count == 2
    assert report.warnings == []


def test_check_instrument_figi_rejects_empty_frame():
    empty = pl.DataFrame(schema={"ticker": pl.Utf8, "figi": pl.Utf8})
    with pytest.raises(DataQualityError):
        check_instrument_figi(empty)


def test_check_instrument_figi_rejects_duplicate_keys():
    df = _valid_figi_df(figi=["FIGI1", "FIGI1"])
    with pytest.raises(DataQualityError):
        check_instrument_figi(df)
