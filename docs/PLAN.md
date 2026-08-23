# Roadmap

Architecture and setup live in [README.md](../README.md) and
[STRUCTURE.md](STRUCTURE.md) — this file only tracks what's left.

## Done

- Bronze/Silver/Gold for prices (Yahoo), macro/FX (FRED), fundamentals (SEC EDGAR)
- OpenFIGI mapping stored in `instrument_figi`
- Historical backfill (`backfill_prices.py`), chunked by ticker batch + year
- New ticker onboarding (`run_register_ticker.py`) with an `is_scheduled`
  on/off switch for the daily ETL
- Structured logging (`src/core/logger.py`)
- Per-run observability via `ingestion_runs`
- Weekly Airflow DAGs for fundamentals and FIGI mapping (previously only
  prices/macro ran automatically)
- API layer (`src/main.py`): `/instruments`, `/prices`, `/fundamentals`,
  `/macro`, `/instruments/{ticker}/figi`, `/health` -- API-key auth, CORS,
  a global exception handler, offset pagination with `X-Total-Count`, and
  Redis response caching (fails open if Redis is down)
- Retry/backoff on every ingestion client (`src/core/retry.py`) --
  previously only `yahoo_client.py` had one
- Quality checks (`transformers/quality/checks.py`) now cover all four Gold
  tables, not just `prices_1d`
- Migrations run automatically on container start (`Dockerfile`)

## Not done

- **Cross-source reconciliation**: `instrument_figi` is populated but nothing
  uses it yet to join Yahoo/SEC/FRED data for the same instrument.
- **Frontend**: none.
- **Integration tests**: everything is unit-tested against mocks; nothing
  exercises the CRUD/upsert layer against a real Postgres.
- **DAGs unverified live**: `fundamentals`/`figi_mapping` DAGs pass
  `py_compile`/ruff but were never loaded inside an actual Airflow instance
  (not installed in the dev venv).
