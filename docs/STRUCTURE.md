# Finance Data Platform – Project Structure

## 1. Purpose of This Document

This document explains the structure of the `finance-data-platform` repository.

The goal of this project is to build a complete batch-based financial data platform that:

- Fetches external financial data
- Stores raw datasets safely
- Cleans and normalizes data
- Computes financial metrics
- Loads serving-ready datasets into Postgres

This repository contains only the data platform.
It does not include the user-facing API or frontend.

---

# 2. High-Level Responsibility

The Data Platform is responsible for:

1. Fetching data from external providers
2. Writing raw data to S3 (Bronze)
3. Cleaning and normalizing data (Silver)
4. Computing financial metrics (Gold)
5. Loading Gold datasets into Postgres
6. Orchestrating execution via Airflow

The output of this system is a set of tables in the `gold` schema in Postgres.

---

# 3. Repository Structure

```

## finance-data-platform/

-- README.md
-- ARCHITECTURE.md
-- DATA_MODEL.md
-- PIPELINE_DESIGN.md
-- requirements.txt
-- .env.example
---------------

-- dags/
-- src/
-- docs/
-- scripts/
-- tests/

```

---

# 4. Root-Level Files

## README.md
Project overview, setup instructions, and architecture summary.

## ARCHITECTURE.md
Detailed explanation of system architecture and infrastructure.

## DATA_MODEL.md
Defines the serving-layer tables in Postgres and their relationships.

## PIPELINE_DESIGN.md
Describes the batch execution flow and DAG structure.

## requirements.txt / pyproject.toml
Python dependencies and package configuration.

## .env.example
Environment variable template for local and production execution.

---

# 5. dags/

Contains Airflow DAG definitions.

## finance_daily_dag.py
Defines the daily pipeline:
- Fetch
- Bronze
- Silver
- Gold
- Load to Postgres

## finance_backfill_dag.py
Defines the backfill pipeline for historical reprocessing.

---

# 6. src/dp/

The core Data Platform package.

This is the main system.

```

src/
-- dp/

```

Everything under `dp/` belongs to the Data Platform.

---

# 7. dp/core/

Contains shared utilities used across the entire system.

## config.py
Loads environment variables and global configuration.

## logging.py
Centralized logging configuration.

## constants.py
Defines reusable constants such as dataset names.

## s3_paths.py
Standardizes S3 path construction for Bronze, Silver, and Gold.

## time_utils.py
Date and partition helpers.

## hashing.py
Utility for deduplication and idempotency.

## retry.py
Retry wrappers for API calls.

## rate_limit.py
Rate-limiting helpers for API safety.

Purpose:
Ensure consistency and avoid duplication across modules.

---

# 8. dp/ingestion/

Responsible for fetching external data and writing Bronze.

## clients/

Each file represents a connection to an external data provider.

Examples:
- yahoo_client.py
- sec_edgar_client.py
- fx_client.py
- openfigi_client.py

Purpose:
Encapsulate API logic and external communication.

---

## jobs/

Executable ingestion tasks.

Examples:
- fetch_prices.py
- fetch_financials.py
- fetch_fx.py
- fetch_identifiers.py

Purpose:
Call clients and write raw payloads to Bronze.

---

## writers/

## bronze_writer.py
Responsible for writing raw JSON payloads into S3 Bronze using standardized partitioning.

Purpose:
Ensure all Bronze writes follow the same structure.

---

# 9. dp/transforms/

Responsible for Silver and Gold processing.

```

transforms/
-- silver/
-- gold/
-- quality/

```

---

## silver/

Transforms raw Bronze data into normalized tables.

Examples:
- normalize_prices.py
- normalize_financials.py
- normalize_fx.py
- normalize_identifiers.py

Purpose:
Clean, type, and harmonize data.

---

## gold/

Computes serving-ready metrics.

Examples:
- build_returns.py
- build_volatility.py
- build_ratios.py

Purpose:
Prepare business-ready datasets.

---

## quality/

Implements data validation.

## checks.py
Validates schema, row counts, duplicates, null thresholds.

## quarantine.py
Isolates corrupted or invalid data.

Purpose:
Ensure reliability before loading to Postgres.

---

# 10. dp/loaders/

Responsible for loading Gold datasets into Postgres.

## postgres/

## engine.py
Creates database connection.

## upsert.py
Handles batch UPSERT logic.

## load_gold_to_postgres.py
Reads Gold Parquet from S3 and loads into Postgres `gold` schema.

Purpose:
Provide serving-ready structured data.

---

# 11. dp/warehouse/

Abstracts storage logic and schema definitions.

## schemas/

Defines canonical dataset schemas.

Examples:
- prices_schema.py
- financials_schema.py
- fx_schema.py
- identifiers_schema.py

Purpose:
Guarantee structural consistency.

---

## io/

Abstracts S3 interactions.

## s3_json.py
Handles Bronze JSON I/O.

## s3_parquet.py
Handles Silver and Gold Parquet I/O.

Purpose:
Centralize storage logic.

---

# 12. dp/orchestration/

Handles execution logic.

## runners/

Local execution without Airflow.

- run_daily.py
- run_backfill.py

## airflow/

Defines Airflow task factories and reusable logic.

Purpose:
Ensure flexible orchestration.

---

# 13. scripts/

Operational utilities.

- init_postgres.sql
- bootstrap_s3_paths.py
- local_run_daily.sh

Purpose:
Assist with setup and manual execution.

---

# 14. tests/

## unit/
Test isolated components.

## integration/
Test full pipeline flows.

Purpose:
Ensure reliability and regression safety.

---

# 15. Architectural Principles

- Strict separation of concerns
- Append-only Bronze
- Partitioned Silver and Gold
- Idempotent loading
- Serving schema isolation
- Cost-controlled AWS design
- Scalable structure

---

# 16. Final Summary

The Data Platform is a complete batch processing system composed of:

- Ingestion
- Transformation
- Quality control
- Serving-layer loading
- Orchestration

It produces structured financial datasets in Postgres.

It does not serve users directly.

The Application Layer consumes the resulting Postgres tables.
