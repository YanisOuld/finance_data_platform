"""
Bronze -> Silver -> Gold pipeline for OpenFIGI ticker mapping (GAPS.md 3.5).

Yahoo/SEC/FRED each use their own identifier for the "same" instrument
(ticker, CIK, series code); FIGI is meant to be the cross-source join key.
The bronze OpenFIGI client already existed but nothing stored or used its
output -- this pipeline normalizes the mapping and upserts it into
instrument_figi, keyed on (ticker, figi) since a ticker can resolve to
several candidate listings across exchanges.

instrument_figi.ticker has a FK to universal_instruments.ticker, so a ticker
must already be registered (see run_register_ticker.py) before it can be
mapped here.
"""

from __future__ import annotations

from datetime import date

from src.core.config import settings
from src.core.database import SessionLocal
from src.core.logger import get_logger
from src.data.crud.ingestion_run import finish_run, start_run
from src.ingestion.clients.openfigi_client import ingest_openfigi_financial_to_bronze
from src.transformers.gold.writers.fetch_silver import fetch_parquet_from_silver
from src.transformers.gold.writers.write_gold_figi import write_gold_figi
from src.transformers.silver.clean_openfigi import clean_bronze_openfigi, normalize_openfigi
from src.transformers.silver.fetch_bronze import fetch_json_from_bronze
from src.transformers.silver.write_silver import create_silver_key, store_to_s3

logger = get_logger(__name__)


def _split_s3_uri(uri: str) -> tuple[str, str]:
    assert uri.startswith("s3://"), f"unexpected uri (expected s3://...): {uri}"
    bucket, key = uri[len("s3://") :].split("/", 1)
    return bucket, key


def run_map_figi_pipeline(ticker: str) -> int:
    ticker = ticker.strip().upper()
    if not ticker:
        raise ValueError("ticker is required")

    with SessionLocal() as session:
        tracking_run_id = start_run(session, dataset="figi_mapping", run_date=date.today())

    try:
        # --- BRONZE ----------------------------------------------------
        bronze_uri = ingest_openfigi_financial_to_bronze(settings.bucket_id, symbol=ticker)
        logger.info("bronze data written to: %s", bronze_uri)
        bucket, bronze_key = _split_s3_uri(bronze_uri)

        # --- SILVER ------------------------------------------------------
        raw = fetch_json_from_bronze(bucket=bucket, key=bronze_key)
        records = normalize_openfigi(raw)
        df_silver = clean_bronze_openfigi(records)

        if df_silver.height == 0:
            logger.info("SKIP: no FIGI candidates returned for ticker=%s.", ticker)
            with SessionLocal() as session:
                finish_run(
                    session,
                    tracking_run_id,
                    status="success",
                    items_total=1,
                    items_success=1,
                    notes="no FIGI candidates returned",
                )
            return 0

        silver_key = create_silver_key(type=ticker.lower(), dt=date.today().isoformat(), vendor="openfigi")
        silver_path = store_to_s3(bucket=bucket, df=df_silver, s3_key=silver_key)
        logger.info("silver data written to: %s", silver_path)

        # --- GOLD ----------------------------------------------------------
        _, skey = _split_s3_uri(silver_path)
        lazy_df = fetch_parquet_from_silver(bucket=bucket, key=skey)
        gold_rows = write_gold_figi(lazy_df.collect())
        logger.info("gold upsert completed, rows: %s", gold_rows)

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
    rows = run_map_figi_pipeline("SOFI")
    logger.info("pipeline finished, gold rows: %s", rows)
