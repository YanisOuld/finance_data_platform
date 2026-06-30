from __future__ import annotations

from datetime import datetime, timedelta

import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from src.core.constants import FRED_COLUMN_SERIES

TZ = pendulum.timezone("America/Montreal")

default_args = {
    "owner": "data-pipeline",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="fred_macro_weekly",
    description="FRED macro/FX series: Bronze -> Silver -> Gold",
    default_args=default_args,
    start_date=datetime(2026, 3, 1, tzinfo=TZ),
    # macro series (CPI, GDP, fed funds...) update at most daily, usually monthly/quarterly;
    # a weekly run is enough to stay current without hammering the FRED API.
    schedule="0 6 * * 1",
    catchup=False,
    max_active_runs=1,
    tags=["fred", "macro", "medallion"],
    params={
        "series_override": Param(
            default=None,
            type=["null", "string"],
            description="Comma-separated series codes to run instead of all known series.",
        ),
    },
) as dag:

    @task
    def get_series_list(**context) -> list:
        override = (context["params"].get("series_override") or "").strip()
        if override:
            return [s.strip().lower() for s in override.split(",") if s.strip()]
        return sorted(FRED_COLUMN_SERIES.keys())

    @task
    def run_series(series: str) -> int:
        from src.orchestration.pipelines.run_macro import run_macro_pipeline

        return run_macro_pipeline(series)

    series_list = get_series_list()
    run_series.expand(series=series_list)
