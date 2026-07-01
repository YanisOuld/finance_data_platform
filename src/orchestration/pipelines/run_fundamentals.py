"""
Bronze -> Silver -> Gold pipeline for SEC EDGAR fundamentals (XBRL
companyfacts), mirroring src/orchestration/pipelines/run_macro.py.
ingestion_watermark rows use source="sec_edgar", dataset="fundamentals" so
each ticker tracks its own watermark.

Note: unlike prices/macro, the SEC companyfacts endpoint always returns a
company's *entire* XBRL history in one response -- start/end aren't used to
filter the request, only kept for observability/params metadata.
"""

from __future__ import annotations

from datetime import date

from src.core.config import settings
from src.core.database import SessionLocal
from src.data.crud.ingestion_run import finish_run, start_run
from src.data.crud.ingestion_watermark import get_last_ts, upsert_watermark
from src.ingestion.clients.sec_edgar_client import ingest_edgar_financial_to_bronze
from src.transformers.gold.writers.fetch_silver import fetch_parquet_from_silver
from src.transformers.gold.writers.write_gold_fundamentals import write_gold_fundamentals
from src.transformers.silver.clean_sec import clean_bronze_sec, normalize_sec
from src.transformers.silver.fetch_bronze import fetch_json_from_bronze
from src.transformers.silver.write_silver import create_silver_key, store_to_s3


def _split_s3_uri(uri: str) -> tuple[str, str]:
    assert uri.startswith("s3://"), f"unexpected uri (expected s3://...): {uri}"
    bucket, key = uri[len("s3://") :].split("/", 1)
    return bucket, key


WATERMARK_SOURCE = "sec_edgar"
WATERMARK_DATASET = "fundamentals"


def resolve_start(ticker: str, start: str | None) -> str:
    if start:
        return start

    with SessionLocal() as session:
        last_ts = get_last_ts(session, ticker=ticker, source=WATERMARK_SOURCE, dataset=WATERMARK_DATASET)

    if last_ts is None:
        return date.today().isoformat()
    return last_ts.isoformat()


def run_fundamentals_pipeline(ticker: str, start: str | None = None, end: str | None = None) -> int:
    ticker = ticker.upper()

    with SessionLocal() as session:
        tracking_run_id = start_run(session, dataset="fundamentals", run_date=date.today())

    try:
        resolved_start = resolve_start(ticker, start)
        resolved_end = end or date.today().isoformat()

        # --- BRONZE ----------------------------------------------------
        bronze_uri = ingest_edgar_financial_to_bronze(
            settings.bucket_id, ticker=ticker, start=resolved_start, end=resolved_end
        )
        print(f"bronze data written to: {bronze_uri}")
        bucket, bronze_key = _split_s3_uri(bronze_uri)

        # --- SILVER ------------------------------------------------------
        raw = fetch_json_from_bronze(bucket=bucket, key=bronze_key)
        records = normalize_sec(raw)
        df_silver = clean_bronze_sec(records)

        if df_silver.height == 0:
            print(f"SKIP: no XBRL facts returned for ticker={ticker}.")
            with SessionLocal() as session:
                finish_run(
                    session,
                    tracking_run_id,
                    status="success",
                    items_total=1,
                    items_success=1,
                    notes="no XBRL facts returned",
                )
            return 0

        silver_key = create_silver_key(type=ticker.lower(), dt=resolved_start, vendor="sec_edgar")
        silver_path = store_to_s3(bucket=bucket, df=df_silver, s3_key=silver_key)
        print(f"silver data written to: {silver_path}")

        # --- GOLD ----------------------------------------------------------
        _, skey = _split_s3_uri(silver_path)
        lazy_df = fetch_parquet_from_silver(bucket=bucket, key=skey)
        gold_rows = write_gold_fundamentals(lazy_df.collect())
        print(f"gold upsert completed, rows: {gold_rows}")

        bronze_run_id = bronze_uri.rsplit("run_id=", 1)[-1].split(".")[0]
        max_ts = df_silver["period_end"].max()
        with SessionLocal() as session:
            upsert_watermark(
                session,
                ticker=ticker,
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
    rows = run_fundamentals_pipeline("SOFI")
    print(f"pipeline finished, gold rows: {rows}")
