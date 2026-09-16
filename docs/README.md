# Docs

Minimal documentation for the Finance Data Platform. Setup, API reference, and
env vars live in the [root README](../README.md); these pages cover the internals.

| Page | What it covers |
| --- | --- |
| [PROCESSES.md](PROCESSES.md) | Each pipeline explained in order (Bronze → Silver → Gold), plus backfill, ticker onboarding, FIGI, serving, and orchestration. |
| [STRUCTURE.md](STRUCTURE.md) | Where the code lives, the Postgres (Gold) tables, and the recipe for adding a new data source. |
| [dataflow/structure.md](dataflow/structure.md) | Mermaid diagrams: daily ingestion flow, ticker registration, and the key-table ER model. |
