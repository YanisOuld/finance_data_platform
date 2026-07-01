from __future__ import annotations

import polars as pl

_SCHEMA = {
    "ticker": pl.Utf8,
    "figi": pl.Utf8,
    "composite_figi": pl.Utf8,
    "share_class_figi": pl.Utf8,
    "security_type": pl.Utf8,
    "market_sector": pl.Utf8,
    "exch_code": pl.Utf8,
    "name": pl.Utf8,
}


def normalize_openfigi(raw: dict) -> list[dict]:
    """Normalize an OpenFIGI mapping bronze envelope into flat rows.

    raw["payload"] is the single job result from the OpenFIGI /v3/mapping
    response (fetch_map() already unwraps the one-job-per-request list):
      {"data": [{"figi": ..., "compositeFIGI": ..., "shareClassFIGI": ...,
                 "securityType": ..., "marketSector": ..., "exchCode": ...,
                 "name": ...}, ...]}
    A ticker is often ambiguous across exchanges, so OpenFIGI can return
    several FIGIs for one ticker -- we keep every candidate row rather than
    guessing which listing is "the" instrument; disambiguating against
    universal_instruments.exchange/currency is left to the consumer.

    raw["meta"]["partitions"]["symbol"] carries the ticker we queried for
    (set by ingest_openfigi_financial_to_bronze), since a given result item
    doesn't reliably echo the queried ticker back.
    """
    meta = raw.get("meta") or {}
    ticker = (meta.get("partitions") or {}).get("symbol")
    if not ticker:
        raise ValueError("bronze envelope is missing meta.partitions.symbol")

    payload = raw.get("payload") or {}
    entries = payload.get("data") or []

    out: list[dict] = []
    for entry in entries:
        figi = entry.get("figi")
        if not figi:
            continue
        out.append(
            {
                "ticker": ticker,
                "figi": figi,
                "composite_figi": entry.get("compositeFIGI"),
                "share_class_figi": entry.get("shareClassFIGI"),
                "security_type": entry.get("securityType"),
                "market_sector": entry.get("marketSector"),
                "exch_code": entry.get("exchCode"),
                "name": entry.get("name"),
            }
        )

    return out


def clean_bronze_openfigi(rows: list[dict]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema=_SCHEMA)

    df = pl.DataFrame(rows, schema_overrides=_SCHEMA)
    df = df.unique(subset=["ticker", "figi"], keep="last")
    df = df.sort(["ticker", "figi"])

    return df
