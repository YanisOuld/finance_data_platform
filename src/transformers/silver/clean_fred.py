from __future__ import annotations

import polars as pl


def normalize_fred(raw: dict) -> list[dict]:
    """Normalize a FRED bronze envelope into flat {series, ts, value} rows.

    raw["payload"] is the raw FRED /series/observations response:
      {"observations": [{"date": "2025-01-01", "value": "3.1"}, ...], ...}
    raw["meta"]["partitions"]["series"] carries our internal series code
    (set by ingest_fred_to_bronze), since FRED itself doesn't echo it back.
    """
    meta = raw.get("meta") or {}
    series = (meta.get("partitions") or {}).get("series")
    if not series:
        raise ValueError("bronze envelope is missing meta.partitions.series")

    payload = raw.get("payload") or {}
    observations = payload.get("observations") or []

    out: list[dict] = []
    for obs in observations:
        raw_value = obs.get("value")
        date_str = obs.get("date")
        if not date_str or raw_value is None or raw_value == ".":
            # "." is FRED's convention for a missing observation
            continue
        try:
            value = float(raw_value)
        except ValueError:
            continue
        out.append({"series": series, "ts": date_str, "value": value})

    return out


def clean_bronze_fred(rows: list[dict]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={"series": pl.Utf8, "ts": pl.Date, "value": pl.Float64})

    df = pl.DataFrame(rows)
    df = df.with_columns(
        pl.col("ts").str.strptime(pl.Date, format="%Y-%m-%d").alias("ts"),
        pl.col("series").cast(pl.Utf8),
    )
    df = df.unique(subset=["series", "ts"], keep="last")
    df = df.sort(["series", "ts"])

    return df
