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
- **Fixed a real bug found via live end-to-end testing**: `store_to_s3()` /
  `fetch_parquet_from_silver()` used polars' native `write_parquet`/
  `scan_parquet` against `s3://...` paths, which doesn't reliably pick up AWS
  credentials in this environment (confirmed broken on both Windows and the
  Linux Docker image) -- every pipeline's Silver step was silently unable to
  write until this was caught. Now goes through an explicit boto3 client
  (same pattern as the Bronze layer), with a regression test
  (`test_silver_s3_roundtrip.py`).
- Live-verified end to end (real Postgres, real S3, real external APIs):
  fundamentals (SEC), macro (FRED), and FIGI mapping (OpenFIGI) all landed
  real data in Gold. Prices (Yahoo) verified only via unit tests + a
  clean bronze/skip path -- Yahoo rate-limited us during testing before any
  real rows could land; see "Not done" below.
- Yahoo request throttling (`_throttle_yahoo_calls` in `yahoo_client.py`):
  minimum 2s between any two outbound Yahoo calls, on top of the existing
  per-call backoff, to reduce (not eliminate) 429s.
- Fixed `/macro/{series}` 404'ing on any FRED series with a slash in its
  code (`usd/cad`, `usd/eur`, ...) -- was a plain path param, needed
  `{series:path}`.
- Internal admin UI: React + Vite app (`frontend/`), built to `frontend/dist`
  and served at `/app` by FastAPI's StaticFiles -- add/toggle tickers, browse
  prices/fundamentals/macro/FIGI tables. Dockerfile is now a two-stage build
  (Node builds the frontend, the Python image only gets the static output --
  no Node at runtime). Live-verified in a real headless Chromium (Playwright)
  against the actual built Docker image + real seeded Postgres data: page
  renders, ticker selection loads real prices, tab switching works, zero
  console errors.
  - First cut used React via CDN + Babel standalone (no build step) -- caught
    a real bug there too: Babel's default "automatic" JSX runtime emits an ES
    module `import`, which a plain classic `<script>` can't execute. Moot now
    that there's a real Vite build, but the same class of "silent failure
    until actually run" lesson applies broadly here.

## Not done

- **Cross-source reconciliation**: `instrument_figi` is populated but nothing
  uses it yet to join Yahoo/SEC/FRED data for the same instrument.
- **Integration tests**: everything is unit-tested against mocks; nothing in
  CI exercises the CRUD/upsert layer against a real Postgres (this was
  checked manually once, against a throwaway Docker Postgres, not codified).
- **DAGs unverified live**: `fundamentals`/`figi_mapping` DAGs pass
  `py_compile`/ruff but were never loaded inside an actual Airflow instance
  (not installed in the dev venv).
- **Prices never confirmed with real data landing in Gold**: Yahoo
  rate-limited every attempt during live testing. Retry/throttling exists,
  but nobody has watched a real `prices_1d` row appear from a real
  `run_prices_pipeline()`/`backfill_prices()` call yet.
- **UI is unauthenticated by anything other than the shared API key**, and
  that key is visible in the page's JS if one is set -- fine for personal/
  internal use, not for anything public-facing.
- **Vite dev-server-only esbuild advisory** (moderate, GHSA-67mh-4wv8-2f99):
  fixing it means a breaking Vite 8 major bump. Doesn't affect the production
  build FastAPI serves (dev server isn't used there), left as-is for now.
