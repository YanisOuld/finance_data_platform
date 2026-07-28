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
    dag_id="sec_fundamentals_weekly",
    description="SEC EDGAR fundamentals (XBRL companyfacts): Bronze -> Silver -> Gold",
    default_args=default_args,
    start_date=datetime(2026, 3, 1, tzinfo=TZ),
    # companyfacts always returns a company's *entire* XBRL history in one
    # response (see run_fundamentals.py), and new filings land quarterly at
    # most -- a weekly run is enough to stay current without hammering SEC.
    schedule="0 7 * * 1",
    catchup=False,
    max_active_runs=1,
    tags=["sec_edgar", "fundamentals", "medallion"],
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
        from src.orchestration.pipelines.run_fundamentals import run_fundamentals_pipeline

        return run_fundamentals_pipeline(ticker)

    tickers = get_tickers()
    run_ticker.expand(ticker=tickers)
