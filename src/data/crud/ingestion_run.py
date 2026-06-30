from __future__ import annotations

import uuid
from datetime import UTC, date, datetime

from sqlalchemy.orm import Session

from src.data.models.ingestion_run import IngestionRun


def start_run(session: Session, dataset: str, run_date: date) -> str:
    """Create a 'running' row and commit immediately, so the row exists in
    Postgres even if the rest of the pipeline crashes before finish_run().
    """
    run_id = uuid.uuid4().hex
    run = IngestionRun(run_id=run_id, dataset=dataset, run_date=run_date, status="running")
    session.add(run)
    session.commit()
    return run_id


def finish_run(
    session: Session,
    run_id: str,
    *,
    status: str,
    items_total: int = 0,
    items_success: int = 0,
    items_failed: int = 0,
    notes: str | None = None,
) -> None:
    run = session.get(IngestionRun, run_id)
    if run is None:
        raise ValueError(f"No ingestion_runs row with run_id={run_id!r} (start_run was never called?)")

    run.status = status
    run.items_total = items_total
    run.items_success = items_success
    run.items_failed = items_failed
    run.notes = notes
    run.finished_at = datetime.now(UTC)
    session.commit()
