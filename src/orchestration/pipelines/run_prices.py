from __future__ import annotations

from datetime import date, timedelta
from typing import List, Optional, Union
import os

import polars as pl
from dotenv import load_dotenv

load_dotenv()

BUCKET_ID = os.getenv("BUCKET_ID", "")

from src.core.constants import DEFAULT_BACKFILL_START
from src.core.database import SessionLocal
from src.data.crud.ingestion_watermark import get_last_ts, upsert_watermark
from src.ingestion.clients.yahoo_client import ingest_yahoo_history_to_bronze
from src.transformers.silver.fetch_bronze import fetch_json_from_bronze
from src.transformers.silver.clean_yf import normalize_history, clean_bronze
from src.transformers.silver.write_silver import create_silver_key, store_to_s3
from src.transformers.gold.writers.fetch_silver import fetch_parquet_from_silver
from src.transformers.gold.features.returns import add_return
from src.transformers.gold.writers.write_gold import write_gold_price1D
from src.transformers.quality.checks import check_prices_1d

"""
Single source of truth for the Yahoo prices_1d pipeline (Bronze -> Silver -> Gold).

Split into three steps (bronze_ingest / silver_transform / gold_load) so the
Airflow DAG (airflow/dags/price_1d.py) can call each one as its own task for
retries/observability, while run_prices_pipeline() chains them for local/manual
runs. Both call paths now share the exact same code -- previously the DAG
re-implemented this logic inline and had drifted from this module.
"""


def _split_s3_uri(uri: str) -> tuple[str, str]:
    assert uri.startswith("s3://"), f"unexpected uri (expected s3://...): {uri}"
    bucket, key = uri[len("s3://"):].split("/", 1)
    return bucket, key


def resolve_batch_start(symbols: List[str], start: Optional[str]) -> str:
    """Resolve a single start date covering every symbol in the batch.

    If start is not given explicitly, use the earliest "day after last
    watermark" across all requested symbols (falling back to
    DEFAULT_BACKFILL_START for tickers never ingested before). Fetching a
    slightly wider window than strictly needed for some tickers is fine: the
    Gold upsert is idempotent on (symbol, ts).
    """
    if start:
        return start

    with SessionLocal() as session:
        candidates: List[str] = []
        for symbol in symbols:
            last_ts = get_last_ts(session, symbol)
            if last_ts is None:
                candidates.append(DEFAULT_BACKFILL_START)
            else:
                candidates.append((last_ts + timedelta(days=1)).isoformat())

    return min(candidates) if candidates else DEFAULT_BACKFILL_START


def bronze_ingest(symbols: Union[List[str], str], start: str, end: str) -> dict:
    if not BUCKET_ID:
        raise RuntimeError("BUCKET_ID env var is missing")

    symbols_list = [symbols] if isinstance(symbols, str) else list(symbols)

    bronze_uri = ingest_yahoo_history_to_bronze(
        bucket=BUCKET_ID,
        symbols=symbols_list,
        start=start,
        end=end,
    )
    bucket, key = _split_s3_uri(bronze_uri)

    return {
        "bucket": bucket,
        "bronze_s3_path": bronze_uri,
        "bronze_key": key,
        "start": start,
        "end": end,
        "tickers_count": len(symbols_list),
        "symbols": symbols_list,
    }


def silver_transform(bronze_info: dict) -> dict:
    raw = fetch_json_from_bronze(bucket=bronze_info["bucket"], key=bronze_info["bronze_key"])

    records = normalize_history(raw)
    df_silver = clean_bronze(records)

    n_rows = df_silver.height
    if n_rows == 0:
        return {**bronze_info, "silver_s3_path": None, "silver_rows": 0}

    # dt of the silver partition: the batch's start date (matches the daily DAG's ds)
    silver_key = create_silver_key(type="prices_1d", dt=bronze_info["start"])
    silver_s3_path = store_to_s3(bucket=bronze_info["bucket"], df=df_silver, s3_key=silver_key)
    _, key = _split_s3_uri(silver_s3_path)

    return {
        **bronze_info,
        "silver_s3_path": silver_s3_path,
        "silver_key": key,
        "silver_rows": n_rows,
    }


def gold_load(silver_info: dict) -> dict:
    if not silver_info.get("silver_s3_path"):
        return {**silver_info, "gold_rows": 0}

    lazy_df = fetch_parquet_from_silver(bucket=silver_info["bucket"], key=silver_info["silver_key"])
    df = lazy_df.collect()

    check_prices_1d(df)

    df_feat = add_return(df, "close")
    gold_rows = write_gold_price1D(df_feat)

    # advance the watermark per symbol to the max ts we just upserted
    max_ts_per_symbol = (
        df_feat.group_by("symbol").agg(pl.col("ts").max().alias("max_ts")).to_dicts()
    )
    run_id = silver_info["bronze_s3_path"].rsplit("run_id=", 1)[-1].split(".")[0]
    with SessionLocal() as session:
        for row in max_ts_per_symbol:
            upsert_watermark(session, ticker=row["symbol"], last_ts=row["max_ts"], run_id=run_id)
        session.commit()

    return {**silver_info, "gold_rows": gold_rows}


def run_prices_pipeline(symbols: Union[List[str], str], start: Optional[str] = None, end: Optional[str] = None) -> int:
    symbols_list = [symbols] if isinstance(symbols, str) else list(symbols)
    symbols_list = [s.strip().upper() for s in symbols_list if s and s.strip()]
    if not symbols_list:
        raise ValueError("symbols must contain at least one ticker")

    resolved_start = resolve_batch_start(symbols_list, start)
    resolved_end = end or date.today().isoformat()

    bronze_info = bronze_ingest(symbols_list, start=resolved_start, end=resolved_end)
    print(f"bronze data written to: {bronze_info['bronze_s3_path']}")

    silver_info = silver_transform(bronze_info)
    if silver_info["silver_rows"] == 0:
        print("SKIP: no rows after silver transform (market closed / Yahoo returned nothing).")
        return 0
    print(f"silver data written to: {silver_info['silver_s3_path']} (rows={silver_info['silver_rows']})")

    gold_info = gold_load(silver_info)
    print(f"gold upsert completed, rows: {gold_info['gold_rows']}")

    return int(gold_info["gold_rows"])


if __name__ == "__main__":
    stocks = "SOFI"
    rows = run_prices_pipeline(stocks, start="2026-01-01", end="2026-03-01")
    print(f"pipeline finished, gold rows: {rows}")
