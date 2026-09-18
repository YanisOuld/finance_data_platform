from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from sqlalchemy.orm import Session

from src.api.deps import require_read
from src.api.schemas import RunResponse
from src.core.database import get_db
from src.data.crud.ingestion_run import count_runs, get_run, list_runs

# Exposes the ingestion_runs table so a caller that triggered a register/refresh
# can observe whether the backfill is running / succeeded / failed, instead of
# polling the data endpoints and guessing. Read-only.
router = APIRouter(prefix="/runs", tags=["runs"], dependencies=[Depends(require_read)])

DbSession = Annotated[Session, Depends(get_db)]


@router.get("", response_model=list[RunResponse])
def list_runs_route(
    db: DbSession,
    response: Response,
    dataset: str | None = Query(
        default=None, description="Filter by dataset, e.g. prices_1d, register_ticker."
    ),
    status: str | None = Query(default=None, description="Filter by status: running, success, failed."),
    limit: int = Query(default=50, le=1000),
    offset: int = Query(default=0, ge=0),
):
    """Recent pipeline executions, newest first. The total (ignoring pagination)
    is returned in the X-Total-Count header.

    Note: rows are keyed by dataset, not ticker -- correlate a specific ticker's
    backfill by dataset + started_at window for now.
    """
    rows = list_runs(db, dataset=dataset, status=status, limit=limit, offset=offset)
    response.headers["X-Total-Count"] = str(count_runs(db, dataset=dataset, status=status))
    return rows


@router.get("/{run_id}", response_model=RunResponse)
def get_run_route(run_id: str, db: DbSession):
    run = get_run(db, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"run '{run_id}' not found")
    return run
