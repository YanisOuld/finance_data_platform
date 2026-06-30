"""
Bronze -> Silver -> Gold pipeline for a single FRED macro/FX series, mirroring
src/orchestration/pipelines/run_prices.py. ingestion_watermark rows use
source="fred", dataset=<series code> so each series tracks its own watermark
(series like "cpi" and "usd/cad" have very different observation cadences).
"""

from __future__ import annotations

from datetime import date

from src.core.config import settings
from src.core.constants import DEFAULT_BACKFILL_START, FRED_COLUMN_SERIES
from src.core.database import SessionLocal
from src.data.crud.ingestion_run import finish_run, start_run
from src.data.crud.ingestion_watermark import get_last_ts, upsert_watermark
from src.ingestion.clients.fred_client import ingest_fred_to_bronze
from src.transformers.gold.writers.fetch_silver import fetch_parquet_from_silver
from src.transformers.gold.writers.write_gold_macro import write_gold_macro
from src.transformers.silver.clean_fred import clean_bronze_fred, normalize_fred
from src.transformers.silver.fetch_bronze import fetch_json_from_bronze
from src.transformers.silver.write_silver import create_silver_key, store_to_s3


def _split_s3_uri(uri: str) -> tuple[str, str]:
    assert uri.startswith("s3://"), f"unexpected uri (expected s3://...): {uri}"
    bucket, key = uri[len("s3://") :].split("/", 1)
    return bucket, key


WATERMARK_SOURCE = "fred"
WATERMARK_DATASET = "macro"


def resolve_start(series: str, start: str | None) -> str:
    if start:
        return start

    with SessionLocal() as session:
        last_ts = get_last_ts(session, ticker=series, source=WATERMARK_SOURCE, dataset=WATERMARK_DATASET)

    if last_ts is None:
        return DEFAULT_BACKFILL_START
    return last_ts.isoformat()


def run_macro_pipeline(series: str, start: str | None = None, end: str | None = None) -> int:
    series = series.lower()
    if series not in FRED_COLUMN_SERIES:
        raise ValueError(f"Unknown macro series '{series}'. Known: {sorted(FRED_COLUMN_SERIES)}")

    with SessionLocal() as session:
        tracking_run_id = start_run(session, dataset="macro", run_date=date.today())

    try:
        resolved_start = resolve_start(series, start)
        resolved_end = end or date.today().isoformat()

        # --- BRONZE ----------------------------------------------------
        bronze_uri = ingest_fred_to_bronze(
            settings.bucket_id, macro=series, start=resolved_start, end=resolved_end
        )
        print(f"bronze data written to: {bronze_uri}")
        bucket, bronze_key = _split_s3_uri(bronze_uri)

        # --- SILVER ------------------------------------------------------
        raw = fetch_json_from_bronze(bucket=bucket, key=bronze_key)
        records = normalize_fred(raw)
        df_silver = clean_bronze_fred(records)

        if df_silver.height == 0:
            print(f"SKIP: no observations returned for series={series} ({resolved_start}..{resolved_end}).")
            with SessionLocal() as session:
                finish_run(
                    session,
                    tracking_run_id,
                    status="success",
                    items_total=1,
                    items_success=1,
                    notes="no observations returned",
                )
            return 0

        silver_key = create_silver_key(type=series.replace("/", "-"), dt=resolved_start, vendor="fred")
        silver_path = store_to_s3(bucket=bucket, df=df_silver, s3_key=silver_key)
        print(f"silver data written to: {silver_path}")

        # --- GOLD ----------------------------------------------------------
        _, skey = _split_s3_uri(silver_path)
        lazy_df = fetch_parquet_from_silver(bucket=bucket, key=skey)
        gold_rows = write_gold_macro(lazy_df.collect())
        print(f"gold upsert completed, rows: {gold_rows}")

        bronze_run_id = bronze_uri.rsplit("run_id=", 1)[-1].split(".")[0]
        max_ts = df_silver["ts"].max()
        with SessionLocal() as session:
            upsert_watermark(
                session,
                ticker=series,
                last_ts=max_ts,
                run_id=bronze_run_id,
                source=WATERMARK_SOURCE,
                dataset=WATERMARK_DATASET,
            )
            session.commit()

        with SessionLocal() as session:
            finish_run(session, tracking_run_id, status="success", items_total=1, items_success=1)

        return int(gold_rows)

    except Exception as e:
        with SessionLocal() as session:
            finish_run(
                session, tracking_run_id, status="failed", items_total=1, items_failed=1, notes=str(e)[:500]
            )
        raise


if __name__ == "__main__":
    rows = run_macro_pipeline("cpi", start="2025-01-01", end="2025-12-31")
    print(f"pipeline finished, gold rows: {rows}")
