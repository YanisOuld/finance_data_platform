# Data Flow

## Daily scheduled ingestion

```mermaid
flowchart LR
  SCHED[Airflow daily DAG] --> Q[Query universal_instruments<br/>where is_active AND is_scheduled]
  Q --> W[Per ticker: resolve start<br/>from ingestion_watermarks]
  W --> F[Fetch Yahoo 1d]
  F --> B[Bronze: S3 JSON]
  B --> T[Silver: normalize + type + dedupe]
  T --> S[Silver: S3 Parquet]
  S --> G[Gold: UPSERT prices_1d]
  G --> U[Update ingestion_watermarks]
  G --> R[Log ingestion_runs]
```

Same shape for `run_macro.py` (FRED) and `run_fundamentals.py` (SEC EDGAR),
just swapping the source and the Gold table.

## Registering a new ticker

```mermaid
flowchart TB
  U["register_ticker(ticker)"] --> V[Fetch + validate Yahoo .info]
  V -->|ok| M[Upsert universal_instruments]
  M --> BF["backfill_prices(ticker)"]
  BF --> B[Bronze] --> S[Silver] --> P[Gold: prices_1d]
```

`is_scheduled=False` registers a ticker without enrolling it in the daily
DAG; `set_scheduled()` flips it later.

## Key tables

```mermaid
erDiagram
  universal_instruments {
    int id PK
    text ticker UK
    text name
    text exchange
    text currency
    text timezone
    boolean is_active
    boolean is_scheduled
  }

  prices_1d {
    text symbol PK
    date ts PK
    double open
    double high
    double low
    double close
    bigint volume
  }

  instrument_figi {
    text ticker PK
    text figi PK
    text composite_figi
    text exch_code
  }

  ingestion_runs {
    text run_id PK
    text dataset
    text status
    int items_total
    int items_success
    int items_failed
  }

  universal_instruments ||--o{ prices_1d : "symbol = ticker"
  universal_instruments ||--o{ instrument_figi : "ticker"
```
