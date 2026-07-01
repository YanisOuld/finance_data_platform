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

## Not done

- **API layer**: `src/main.py` is an empty FastAPI app. Nothing serves the
  Gold tables to a frontend yet.
- **Cross-source reconciliation**: `instrument_figi` is populated but nothing
  uses it yet to join Yahoo/SEC/FRED data for the same instrument.
- **Data quality**: `transformers/quality/checks.py` only covers `prices_1d`.
- **Frontend**: none.
