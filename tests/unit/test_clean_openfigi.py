import pytest

from src.transformers.silver.clean_openfigi import clean_bronze_openfigi, normalize_openfigi


def _bronze_envelope(data, symbol="SOFI"):
    return {
        "meta": {"source": "openfigi", "dataset": "mapping", "partitions": {"symbol": symbol}},
        "payload": {"data": data},
    }


def test_normalize_openfigi_flattens_candidates():
    raw = _bronze_envelope(
        data=[
            {
                "figi": "BBG000B9XRY4",
                "compositeFIGI": "BBG000B9XVV8",
                "shareClassFIGI": "BBG001S5N8V8",
                "securityType": "Common Stock",
                "marketSector": "Equity",
                "exchCode": "US",
                "name": "SOFI TECHNOLOGIES INC",
            }
        ]
    )

    records = normalize_openfigi(raw)

    assert records == [
        {
            "ticker": "SOFI",
            "figi": "BBG000B9XRY4",
            "composite_figi": "BBG000B9XVV8",
            "share_class_figi": "BBG001S5N8V8",
            "security_type": "Common Stock",
            "market_sector": "Equity",
            "exch_code": "US",
            "name": "SOFI TECHNOLOGIES INC",
        }
    ]


def test_normalize_openfigi_keeps_multiple_ambiguous_candidates():
    raw = _bronze_envelope(
        data=[
            {"figi": "FIGI1", "exchCode": "US"},
            {"figi": "FIGI2", "exchCode": "LN"},
        ]
    )

    records = normalize_openfigi(raw)

    assert [r["figi"] for r in records] == ["FIGI1", "FIGI2"]


def test_normalize_openfigi_skips_entries_missing_figi():
    raw = _bronze_envelope(data=[{"exchCode": "US"}, {"figi": "FIGI1"}])

    records = normalize_openfigi(raw)

    assert len(records) == 1
    assert records[0]["figi"] == "FIGI1"


def test_normalize_openfigi_requires_symbol_in_meta():
    raw = {"meta": {"partitions": {}}, "payload": {"data": []}}
    with pytest.raises(ValueError):
        normalize_openfigi(raw)


def test_clean_bronze_openfigi_dedups_and_sorts():
    rows = [
        {"ticker": "SOFI", "figi": "FIGI2", "exch_code": "LN"},
        {"ticker": "SOFI", "figi": "FIGI1", "exch_code": "US"},
        {"ticker": "SOFI", "figi": "FIGI1", "exch_code": "US"},
    ]

    df = clean_bronze_openfigi(rows)

    assert df.height == 2
    assert df["figi"].to_list() == ["FIGI1", "FIGI2"]


def test_clean_bronze_openfigi_empty_rows_returns_empty_frame():
    df = clean_bronze_openfigi([])
    assert df.height == 0
    assert set(df.columns) == {
        "ticker",
        "figi",
        "composite_figi",
        "share_class_figi",
        "security_type",
        "market_sector",
        "exch_code",
        "name",
    }
