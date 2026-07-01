"""
Fetch the json
"""

import polars as pl

from src.core.config import settings
from src.core.logger import get_logger

from .fetch_bronze import create_bronze_key, fetch_json_from_bronze
from .write_silver import create_silver_key, store_to_s3

logger = get_logger(__name__)


def normalize_info(info: dict):
    res = {
        "exchange": info["exchange"],
        "quote_type": info["quoteType"],
        "timezone": info["timezone"],
        "currency": info["currency"],
    }
    return res


def normalize_history(raw: dict) -> list[dict]:
    """
    Matches the bronze payload written by ingest_yahoo_history_to_bronze (schema_version=2):
    envelope["payload"] = {"start": ..., "end": ..., "rows": [{ts, open, high, low, close, volume, symbol}], "errors": [...]}
    """
    payload = raw.get("payload") or {}

    rows = payload.get("rows") or []
    errors = payload.get("errors") or []
    if errors:
        logger.warning(
            "normalize_history: %s symbol(s) had no bronze rows: %s",
            len(errors),
            [e.get("symbol") for e in errors],
        )

    out: list[dict] = []
    for r in rows:
        ts = r.get("ts")
        symbol = r.get("symbol")
        if ts is None or symbol is None:
            continue
        # ts is a JSON-stringified pandas Timestamp (e.g. "2026-01-06 00:00:00-05:00").
        # We only need calendar-day granularity for daily bars, so keep the date part only.
        date_str = str(ts)[:10]
        out.append(
            {
                "symbol": symbol,
                "ts": date_str,
                "open": r.get("open"),
                "high": r.get("high"),
                "low": r.get("low"),
                "close": r.get("close"),
                "volume": r.get("volume"),
            }
        )

    return out


def clean_bronze(rows: list[dict]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(
            schema={
                "symbol": pl.Utf8,
                "ts": pl.Date,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Int64,
            }
        )

    df = pl.DataFrame(rows)
    df = df.with_columns(
        pl.col("ts").str.strptime(pl.Date, format="%Y-%m-%d").alias("ts"),
        pl.col("symbol").cast(pl.Utf8),
        pl.col("volume").cast(pl.Int64),
    )
    df = df.unique(subset=["symbol", "ts"], keep="last")
    df = df.sort(["symbol", "ts"])

    return df


if __name__ == "__main__":
    key = create_bronze_key(type="history", run_id="20260224180059Z", dt="2026-02-24")
    res = fetch_json_from_bronze(settings.bucket_id, key)
    table = normalize_history(res)
    df = clean_bronze(table)
    silver_key = create_silver_key(type="history", dt="2026-02-24")
    store_to_s3(bucket=settings.bucket_id, df=df, s3_key=silver_key)
    logger.info("%s", silver_key)
