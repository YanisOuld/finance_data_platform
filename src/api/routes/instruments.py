from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from sqlalchemy.orm import Session

from src.api.schemas import InstrumentCreate, InstrumentResponse, ScheduledUpdate
from src.core.database import get_db
from src.core.logger import get_logger
from src.data.crud.universal_instruments import get_instrument, list_instruments, set_scheduled
from src.orchestration.pipelines.run_register_ticker import register_ticker, validate_and_upsert_ticker

logger = get_logger(__name__)

router = APIRouter(prefix="/instruments", tags=["instruments"])

DbSession = Annotated[Session, Depends(get_db)]


@router.get("", response_model=list[InstrumentResponse])
def list_instruments_route(
    db: DbSession,
    is_active: bool | None = None,
    is_scheduled: bool | None = None,
):
    return list_instruments(db, is_active=is_active, is_scheduled=is_scheduled)


@router.get("/{ticker}", response_model=InstrumentResponse)
def get_instrument_route(ticker: str, db: DbSession):
    instrument = get_instrument(db, ticker)
    if instrument is None:
        raise HTTPException(status_code=404, detail=f"'{ticker}' is not registered")
    return instrument


@router.post("", response_model=InstrumentResponse, status_code=202)
def create_instrument_route(body: InstrumentCreate, background_tasks: BackgroundTasks):
    """Registers a ticker synchronously (fetch + validate Yahoo .info, upsert
    universal_instruments -- a couple seconds), then hands the slow part off:
    the initial multi-year price backfill can take minutes, so it runs as a
    background task via register_ticker() rather than blocking the request.
    """
    try:
        instrument = validate_and_upsert_ticker(body.ticker, is_scheduled=body.is_scheduled)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e

    background_tasks.add_task(
        register_ticker,
        body.ticker,
        is_scheduled=body.is_scheduled,
        backfill_start=body.backfill_start,
        backfill_end=body.backfill_end,
    )
    return instrument


@router.patch("/{ticker}/scheduled", response_model=InstrumentResponse)
def update_scheduled_route(ticker: str, body: ScheduledUpdate, db: DbSession):
    instrument = set_scheduled(db, ticker, body.is_scheduled)
    if instrument is None:
        raise HTTPException(status_code=404, detail=f"'{ticker}' is not registered")
    return instrument
