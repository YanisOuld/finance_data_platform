# Project Structure

```
src/
  core/                     shared utilities
    config.py                 Settings (env vars), single source of truth
    database.py                SQLAlchemy engine/session/Base
    logger.py                   structured logging (level, timestamp, module)
    constants.py                 DEFAULT_BACKFILL_START, FRED_COLUMN_SERIES
    bucket_utils.py                S3 client factory

  ingestion/                fetch external data, write Bronze
    clients/
      yahoo_client.py          prices (history) + .info metadata
      fred_client.py            macro/FX series
      sec_edgar_client.py        XBRL companyfacts (fundamentals)
      openfigi_client.py          ticker -> FIGI mapping
    writers/
      write_bronze.py            write_bronze_to_s3(): the one Bronze envelope writer

  transformers/
    silver/                  Bronze JSON -> flat rows -> Parquet
      clean_yf.py, clean_fred.py, clean_sec.py, clean_openfigi.py
      fetch_bronze.py, write_silver.py
    gold/
      features/returns.py       e.g. close_returns
      writers/                  write_gold*.py: upsert Silver Parquet into Postgres
    quality/
      checks.py                 sanity checks run before the Gold upsert

  data/
    models/                  one SQLAlchemy model per Postgres table
    crud/                    upsert/query functions per table (ON CONFLICT upserts)

  orchestration/pipelines/  bronze_ingest -> silver_transform -> gold_load,
                            wrapped with ingestion_runs tracking
    run_prices.py, run_macro.py, run_fundamentals.py, run_map_figi.py
    run_register_ticker.py, backfill_prices.py

alembic/versions/         one migration per table, linear history
airflow/dags/             daily DAGs calling the pipelines above
tests/unit/               one test file per silver transform / pipeline helper
```

## Postgres tables (Gold)

| Table | What | Key |
| --- | --- | --- |
| `universal_instruments` | registered tickers + `is_active`/`is_scheduled` flags | `ticker` |
| `prices_1d` | daily OHLCV | `(symbol, ts)` |
| `macro_series` | FRED observations | `(series, ts)` |
| `fundamentals` | XBRL facts (revenue, EPS, ...) | `(ticker, concept, unit, period_end, fp, form)` |
| `instrument_figi` | OpenFIGI candidates for a ticker | `(ticker, figi)`, FK to `universal_instruments` |
| `ingestion_watermarks` | last processed timestamp per `(source, dataset, ticker)` | `(source, dataset, ticker)` |
| `ingestion_runs` | one row per pipeline execution, for observability | `run_id` |

## Adding a new source

Every vertical (prices, macro, fundamentals, ...) follows the same recipe:

1. `ingestion/clients/<vendor>_client.py` — fetch + `write_bronze_to_s3(...)`
2. `transformers/silver/clean_<vendor>.py` — `normalize_*()` (flatten to rows) + `clean_bronze_*()` (type/dedupe, Polars DataFrame)
3. `data/models/<table>.py` + `data/crud/<table>.py` + an Alembic migration
4. `transformers/gold/writers/write_gold_<table>.py` — upsert the Silver Parquet into Postgres
5. `orchestration/pipelines/run_<name>.py` — chain the three steps, wrap in `start_run`/`finish_run`

Copy an existing pipeline (`run_macro.py` is the shortest) rather than
starting from scratch.
