from __future__ import annotations

from datetime import datetime, timedelta

import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param

TZ = pendulum.timezone("America/Montreal")

default_args = {
    "owner": "data-pipeline",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="openfigi_mapping_weekly",
    description="OpenFIGI ticker -> FIGI mapping: Bronze -> Silver -> Gold",
    default_args=default_args,
    start_date=datetime(2026, 3, 1, tzinfo=TZ),
    # a ticker's exchange listings change rarely -- weekly is more than
    # enough, and keeps us well under OpenFIGI's rate limit.
    schedule="0 8 * * 1",
    catchup=False,
    max_active_runs=1,
    tags=["openfigi", "mapping", "medallion"],
    params={
        "tickers_override": Param(
            default=None,
            type=["null", "string"],
            description="Comma-separated tickers to run instead of the scheduled universe.",
        ),
    },
) as dag:

    @task
    def get_tickers(**context) -> list:
        override = (context["params"].get("tickers_override") or "").strip()
        if override:
            return [s.strip().upper() for s in override.split(",") if s.strip()]

        from src.core.database import SessionLocal
        from src.data.crud.universal_instruments import get_scheduled_universe

        with SessionLocal() as session:
            return get_scheduled_universe(session)

    @task
    def run_ticker(ticker: str) -> int:
        from src.orchestration.pipelines.run_map_figi import run_map_figi_pipeline

        return run_map_figi_pipeline(ticker)

    tickers = get_tickers()
    run_ticker.expand(ticker=tickers)
