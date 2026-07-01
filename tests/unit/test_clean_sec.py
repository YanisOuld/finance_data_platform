import pytest

from src.transformers.silver.clean_sec import clean_bronze_sec, normalize_sec


def _bronze_envelope(facts, symbol="SOFI"):
    return {
        "meta": {"source": "sec_edgar", "dataset": "companyfacts", "partitions": {"symbol": symbol}},
        "payload": {"facts": facts},
    }


def test_normalize_sec_flattens_taxonomy_concept_unit():
    raw = _bronze_envelope(
        facts={
            "us-gaap": {
                "Revenues": {
                    "units": {
                        "USD": [
                            {
                                "end": "2025-12-31",
                                "start": "2025-01-01",
                                "val": 1000,
                                "accn": "0001",
                                "fy": 2025,
                                "fp": "FY",
                                "form": "10-K",
                                "filed": "2026-02-01",
                            }
                        ]
                    }
                }
            }
        }
    )

    records = normalize_sec(raw)

    assert records == [
        {
            "ticker": "SOFI",
            "concept": "us-gaap:Revenues",
            "unit": "USD",
            "period_start": "2025-01-01",
            "period_end": "2025-12-31",
            "fy": 2025,
            "fp": "FY",
            "form": "10-K",
            "val": 1000.0,
            "accn": "0001",
            "filed": "2026-02-01",
        }
    ]


def test_normalize_sec_skips_observations_missing_end_or_val():
    raw = _bronze_envelope(
        facts={
            "us-gaap": {
                "Revenues": {
                    "units": {
                        "USD": [
                            {"end": None, "val": 1000},
                            {"end": "2025-12-31", "val": None},
                            {"end": "2025-12-31", "val": 50},
                        ]
                    }
                }
            }
        }
    )

    records = normalize_sec(raw)

    assert len(records) == 1
    assert records[0]["val"] == 50.0


def test_normalize_sec_requires_symbol_in_meta():
    raw = {"meta": {"partitions": {}}, "payload": {"facts": {}}}
    with pytest.raises(ValueError):
        normalize_sec(raw)


def test_clean_bronze_sec_dedups_by_latest_filed():
    rows = [
        {
            "ticker": "SOFI",
            "concept": "us-gaap:Revenues",
            "unit": "USD",
            "period_start": "2025-01-01",
            "period_end": "2025-12-31",
            "fy": 2025,
            "fp": "FY",
            "form": "10-K",
            "val": 1000.0,
            "accn": "0001",
            "filed": "2026-02-01",
        },
        {
            "ticker": "SOFI",
            "concept": "us-gaap:Revenues",
            "unit": "USD",
            "period_start": "2025-01-01",
            "period_end": "2025-12-31",
            "fy": 2025,
            "fp": "FY",
            "form": "10-K",
            "val": 1100.0,  # restated value, filed later
            "accn": "0002",
            "filed": "2026-03-15",
        },
    ]

    df = clean_bronze_sec(rows)

    assert df.height == 1
    assert df["val"].to_list()[0] == 1100.0
    assert df["accn"].to_list()[0] == "0002"


def test_clean_bronze_sec_fills_null_fp_and_form():
    rows = [
        {
            "ticker": "SOFI",
            "concept": "dei:EntityCommonStockSharesOutstanding",
            "unit": "shares",
            "period_start": None,
            "period_end": "2025-12-31",
            "fy": None,
            "fp": None,
            "form": None,
            "val": 12345.0,
            "accn": "0001",
            "filed": None,
        }
    ]

    df = clean_bronze_sec(rows)

    assert df["fp"].to_list()[0] == "UNK"
    assert df["form"].to_list()[0] == "UNK"
