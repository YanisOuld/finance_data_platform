# Finance Data Platform

A batch financial data platform: fetch data from external providers, land it
raw (Bronze), clean/normalize it (Silver), load serving-ready tables into
Postgres (Gold), and serve them through a FastAPI. Orchestrated with Airflow,
storage on S3.

```
external API -> Bronze (S3, raw JSON) -> Silver (S3, Parquet) -> Gold (Postgres) -> API
```

## What's implemented

| Data | Source | Pipeline | Airflow DAG |
| --- | --- | --- | --- |
| Daily prices (OHLCV) | Yahoo Finance | `run_prices.py` | `yf_prices_1d_daily` |
| Macro/FX series | FRED | `run_macro.py` | `fred_macro_weekly` |
| Fundamentals (XBRL) | SEC EDGAR | `run_fundamentals.py` | `sec_fundamentals_weekly` |
| Ticker <-> FIGI mapping | OpenFIGI | `run_map_figi.py` | `openfigi_mapping_weekly` |
| Historical backfill | Yahoo Finance | `backfill_prices.py` | on-demand only |
| New ticker onboarding | Yahoo Finance | `run_register_ticker.py` | on-demand only |

Pipelines live in `src/orchestration/pipelines/`. Each follows the same
shape (`bronze_ingest -> silver_transform -> gold_load`), runs a quality
check (`src/transformers/quality/checks.py`) before the Gold upsert, and is
wrapped in an `ingestion_runs` row for observability. See
[docs/STRUCTURE.md](docs/STRUCTURE.md) for where everything lives.

## API

`src/main.py` — FastAPI, served via `uvicorn` (see `Dockerfile`).

| Route | What |
| --- | --- |
| `GET /health` | DB connectivity check, no auth (for Docker/LB healthchecks) |
| `GET /instruments`, `GET /instruments/{ticker}` | registered tickers |
| `POST /instruments` | register a ticker + kick off an initial backfill |
| `PATCH /instruments/{ticker}/scheduled` | toggle the daily auto-ETL on/off |
| `GET /instruments/{ticker}/figi` | OpenFIGI candidates for a ticker |
| `GET /prices/{ticker}` | daily OHLCV, filterable by `start`/`end` |
| `GET /fundamentals/{ticker}` | XBRL facts, filterable by `concept` |
| `GET /macro/{series}` | FRED series, filterable by `start`/`end` |

All routes except `/health` require an `X-API-Key` header matching `API_KEY`
(see `src/api/deps.py`) whenever `ENV != local`. List endpoints support
`limit`/`offset` and return the total row count in an `X-Total-Count` header,
and are cached in Redis for a short TTL (fails open if Redis is unreachable).

## Internal UI

A React + Vite app (`frontend/`) served at `/app` — add/toggle tickers and
browse prices/fundamentals/macro/FIGI tables. It's same-origin with the API
(no CORS involved) and reads an API key from a browser field if one is
configured. Meant as an internal admin tool, not a public-facing app — the
key is visible in the page's JS if you set one.

```bash
cd frontend
npm install
npm run dev        # dev server on :5173, proxies API calls to :8000
npm run build       # -> frontend/dist, served by FastAPI at /app
```

`src/main.py` mounts `frontend/dist` as static files; the API still boots
(with a logged warning) if the frontend hasn't been built. The Dockerfile
builds it automatically in a Node stage before the Python image — no Node
needed at runtime.

## Setup

```bash
uv sync
cp .env.example .env   # fill in DATABASE_URL, BUCKET_ID, API keys
uv run alembic upgrade head
cd frontend && npm install && npm run build && cd ..
uv run uvicorn src.main:app --reload
```

Then open http://localhost:8000/app/ for the internal UI, or http://localhost:8000/docs for the API's interactive docs.

Required env vars (see `src/core/config.py` for the full list):

- `DATABASE_URL` — Postgres connection string
- `BUCKET_ID` — S3 bucket for Bronze/Silver
- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` — S3 credentials
- `FRED_API_KEY`, `OPENFIGI_API_KEY` — provider API keys
- `API_KEY` — required once `ENV != local` (the API fails closed without it)
- `REDIS_URL` — optional, enables response caching

`docker compose up` builds and runs the API (`backend`), Redis, and the full
Airflow stack; migrations run automatically on container start.

## Running a pipeline

```bash
uv run python -m src.orchestration.pipelines.run_prices          # one ticker/day
uv run python -m src.orchestration.pipelines.backfill_prices --symbols AAPL,MSFT --start 2015-01-01
uv run python -m src.orchestration.pipelines.run_register_ticker AAPL
```

Airflow DAGs (`airflow/dags/`) run the scheduled pipelines against every
ticker/series flagged `is_active` + `is_scheduled` in `universal_instruments`
(prices/fundamentals/figi) or `FRED_COLUMN_SERIES` (macro).

## Tests

```bash
uv run pytest tests/unit
```

## Not yet built

- Using `instrument_figi` to reconcile the same instrument across Yahoo/SEC/FRED.
- Integration tests against a real Postgres (current tests mock the DB layer).
