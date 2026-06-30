import pytest

from src.transformers.silver.clean_fred import clean_bronze_fred, normalize_fred


def _bronze_envelope(observations, series="cpi"):
    return {
        "meta": {"source": "fred", "dataset": "macros", "partitions": {"series": series}},
        "payload": {"observations": observations},
    }


def test_normalize_fred_filters_missing_observations():
    raw = _bronze_envelope(
        observations=[
            {"date": "2026-01-01", "value": "3.1"},
            {"date": "2026-02-01", "value": "."},  # FRED's missing-value marker
            {"date": "2026-03-01", "value": "3.4"},
        ]
    )

    records = normalize_fred(raw)

    assert records == [
        {"series": "cpi", "ts": "2026-01-01", "value": 3.1},
        {"series": "cpi", "ts": "2026-03-01", "value": 3.4},
    ]


def test_normalize_fred_requires_series_in_meta():
    raw = {"meta": {"partitions": {}}, "payload": {"observations": []}}
    with pytest.raises(ValueError):
        normalize_fred(raw)


def test_clean_bronze_fred_types_and_sorts():
    rows = [
        {"series": "cpi", "ts": "2026-03-01", "value": 3.4},
        {"series": "cpi", "ts": "2026-01-01", "value": 3.1},
    ]
    df = clean_bronze_fred(rows)

    assert df.height == 2
    assert df["ts"].to_list()[0].isoformat() == "2026-01-01"
