from __future__ import annotations

from datetime import datetime, timedelta
import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param

TZ = pendulum.timezone("America/Montreal")

default_args = {
    "owner": "data-pipeline",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="yf_prices_1d_daily",
    description="Yahoo prices 1D: Bronze → Silver → Gold",
    default_args=default_args,
    start_date=datetime(2026, 3, 1, tzinfo=TZ),
    schedule="5 16 * * 1-5",
    catchup=False,
    max_active_runs=1,
    tags=["yahoo", "prices", "medallion"],
    params={
    "start_dt": Param(
        default=None,
        type=["null", "string"],
        description="Override start_dt (YYYY-MM-DD). Leave empty for normal.",
    ),
    "end_dt": Param(
        default=None,
        type=["null", "string"],
        description="Override end_dt (YYYY-MM-DD). Leave empty for normal.",
    ),
    "limit_tickers": Param(
        default=0,
        type="integer",
        minimum=0,
        description="Optional cap on number of tickers.",
    ),
    "symbols_override": Param(
        default=None,
        type=["null", "string"],
        description="Comma-separated tickers to run instead of DB.",
    ),
},
) as dag:

    @task
    def resolve_dates(**context) -> dict:
        ds: str = context["ds"]
        start_dt = (context["params"].get("start_dt") or "").strip()
        end_dt = (context["params"].get("end_dt") or "").strip()

        # fallback: 1 day
        start = start_dt or ds
        end = end_dt or start
        
        return {"start": str(start), "end": str(end)}

    @task
    def get_scheduled_tickers(**context) -> list:
        symbols_override = (context["params"].get("symbols_override") or "")
        limit_tickers = int(context["params"].get("limit_tickers") or 0)

        override = [s.strip().upper() for s in symbols_override.split(",") if s.strip()]
        if override:
            return override[:limit_tickers] if limit_tickers > 0 else override

        import os
        from sqlalchemy import create_engine, select
        from sqlalchemy.orm import Session
        from src.data.models.universal_instruments import UniversalInstrument

        engine = create_engine(os.environ["GOLD_DATABASE_URL"])
        with Session(engine) as session:
            stmt = select(UniversalInstrument.ticker).where(
                UniversalInstrument.is_active == True,
                UniversalInstrument.is_scheduled == True,
            )
            tickers = list(session.execute(stmt).scalars().all())

        if limit_tickers > 0:
            tickers = tickers[:limit_tickers]

        return tickers

    @task
    def bronze_ingest(dt: dict, tickers: list) -> dict:
        from src.orchestration.pipelines.run_prices import bronze_ingest as _bronze_ingest

        if not tickers:
            raise AirflowSkipException("No tickers scheduled.")

        return _bronze_ingest(symbols=tickers, start=dt["start"], end=dt["end"])

    @task
    def silver_transform(bronze_info: dict) -> dict:
        from src.orchestration.pipelines.run_prices import silver_transform as _silver_transform

        silver_info = _silver_transform(bronze_info)
        if silver_info["silver_rows"] == 0:
            raise AirflowSkipException(
                f"No rows after cleaning for dt={bronze_info['start']} (market closed?)"
            )
        return silver_info

    @task
    def gold_transform(silver_info: dict) -> dict:
        from src.orchestration.pipelines.run_prices import gold_load

        return gold_load(silver_info)

    @task
    def summary(bronze_info: dict, silver_info: dict, gold_info: dict) -> None:
        print("\n" + "=" * 80)
        print("PIPELINE EXECUTION SUMMARY")
        print("=" * 80)
        print(f"Date:    {bronze_info['start']} -> {bronze_info['end']}")
        print(f"Tickers: {bronze_info.get('tickers_count')}")
        print(f"BRONZE:  {bronze_info['bronze_s3_path']}")
        print(f"SILVER:  {silver_info['silver_s3_path']} (rows={silver_info.get('silver_rows')})")
        print(f"GOLD:    {gold_info['gold_rows']} rows upserted")
        print("=" * 80 + "\n")

    dt = resolve_dates()
    tickers = get_scheduled_tickers()
    bronze = bronze_ingest(dt=dt, tickers=tickers)
    silver = silver_transform(bronze)
    gold = gold_transform(silver)
    summary(bronze, silver, gold)