# Finance Data Platform

A batch financial data platform: fetch data from external providers, land it
raw (Bronze), clean/normalize it (Silver), and load serving-ready tables into
Postgres (Gold). Orchestrated with Airflow, storage on S3.

```
external API -> Bronze (S3, raw JSON) -> Silver (S3, Parquet) -> Gold (Postgres)
```

## What's implemented

| Data | Source | Pipeline |
| --- | --- | --- |
| Daily prices (OHLCV) | Yahoo Finance | `src/orchestration/pipelines/run_prices.py` |
| Macro/FX series | FRED | `src/orchestration/pipelines/run_macro.py` |
| Fundamentals (XBRL) | SEC EDGAR | `src/orchestration/pipelines/run_fundamentals.py` |
| Ticker <-> FIGI mapping | OpenFIGI | `src/orchestration/pipelines/run_map_figi.py` |
| Historical backfill | Yahoo Finance | `src/orchestration/pipelines/backfill_prices.py` |
| New ticker onboarding | Yahoo Finance | `src/orchestration/pipelines/run_register_ticker.py` |

Each pipeline follows the same shape: `bronze_ingest -> silver_transform ->
gold_load`, wrapped in an `ingestion_runs` row for observability. See
[docs/STRUCTURE.md](docs/STRUCTURE.md) for where everything lives.

## Setup

```bash
uv sync
cp .env.example .env   # fill in DATABASE_URL, BUCKET_ID, API keys
uv run alembic upgrade head
```

Required env vars (see `src/core/config.py` for the full list):

- `DATABASE_URL` — Postgres connection string
- `BUCKET_ID` — S3 bucket for Bronze/Silver
- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` — S3 credentials
- `FRED_API_KEY`, `OPENFIGI_API_KEY` — provider API keys

## Running a pipeline

```bash
uv run python -m src.orchestration.pipelines.run_prices          # one ticker/day
uv run python -m src.orchestration.pipelines.backfill_prices --symbols AAPL,MSFT --start 2015-01-01
uv run python -m src.orchestration.pipelines.run_register_ticker AAPL
```

Airflow DAGs (`airflow/dags/`) run `run_prices_pipeline()` /
`run_macro_pipeline()` daily against every ticker/series flagged
`is_active` + `is_scheduled` in `universal_instruments` / `FRED_COLUMN_SERIES`.

## Tests

```bash
uv run pytest tests/unit
```

## Not yet built

- A FastAPI layer to serve the Gold tables to a frontend (`src/main.py` is
  currently an empty app).
- Using `instrument_figi` to reconcile the same instrument across Yahoo/SEC/FRED.
