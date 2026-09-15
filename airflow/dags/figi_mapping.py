from __future__ import annotations

from datetime import datetime, timedelta

import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param

TZ = pendulum.timezone("America/Montreal")

# The project's pipeline code needs SQLAlchemy 2.0, incompatible with Airflow's
# own 1.4 -- so it runs in this isolated interpreter (built in airflow/Dockerfile)
# via ExternalPythonOperator, never in the scheduler/worker interpreter itself.
PIPELINE_PYTHON = "/opt/pipeline-venv/bin/python"

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

    @task.external_python(python=PIPELINE_PYTHON, expect_airflow=False)
    def get_tickers(override: str) -> list:
        # Runs in the pipeline venv. `override` is a rendered template string:
        # None renders as "None", an unset param as "", both meaning "use the
        # scheduled universe from the DB".
        import sys

        sys.path.insert(0, "/opt/project")

        override = (override or "").strip()
        if override and override.lower() != "none":
            return [s.strip().upper() for s in override.split(",") if s.strip()]

        from src.core.database import SessionLocal
        from src.data.crud.universal_instruments import get_scheduled_universe

        with SessionLocal() as session:
            return get_scheduled_universe(session)

    @task.external_python(python=PIPELINE_PYTHON, expect_airflow=False)
    def run_ticker(ticker: str) -> int:
        import sys

        sys.path.insert(0, "/opt/project")

        from src.orchestration.pipelines.run_map_figi import run_map_figi_pipeline

        return run_map_figi_pipeline(ticker)

    tickers = get_tickers(override="{{ params.tickers_override }}")
    run_ticker.expand(ticker=tickers)
