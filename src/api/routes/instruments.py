from __future__ import annotations

from datetime import date
from typing import Annotated

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, Response
from sqlalchemy.orm import Session

from src.api.deps import require_api_key
from src.api.schemas import FigiResponse, InstrumentCreate, InstrumentResponse, ScheduledUpdate
from src.core.cache import cache_get_json, cache_set_json
from src.core.database import get_db
from src.core.logger import get_logger
from src.data.crud.instrument_figi import get_figi_mappings
from src.data.crud.universal_instruments import (
    count_instruments,
    get_instrument,
    list_instruments,
    set_scheduled,
)
from src.orchestration.airflow_client import trigger_dag_run
from src.orchestration.pipelines.run_fundamentals import run_fundamentals_pipeline
from src.orchestration.pipelines.run_register_ticker import register_ticker, validate_and_upsert_ticker

logger = get_logger(__name__)

router = APIRouter(prefix="/instruments", tags=["instruments"], dependencies=[Depends(require_api_key)])

DbSession = Annotated[Session, Depends(get_db)]

# The daily prices DAG doubles as the backfill mechanism: passing symbols_override
# + start_dt/end_dt in its run conf makes it ingest that ticker's history for the
# given range (see airflow/dags/price_1d.py).
_PRICES_DAG_ID = "yf_prices_1d_daily"

# The fundamentals DAG likewise backfills on demand via tickers_override. No date
# range: SEC companyfacts returns a company's entire XBRL history in one response
# (see airflow/dags/fundamentals.py).
_FUNDAMENTALS_DAG_ID = "sec_fundamentals_weekly"


@router.get("", response_model=list[InstrumentResponse])
def list_instruments_route(
    db: DbSession,
    response: Response,
    is_active: bool | None = None,
    is_scheduled: bool | None = None,
    limit: int | None = Query(default=None, le=5000),
    offset: int = Query(default=0, ge=0),
):
    """List the instrument universe. Unpaginated by default (`limit` unset) so
    existing callers keep getting the full list; pass `limit`/`offset` to page.
    The total (ignoring pagination) is returned in the X-Total-Count header."""
    rows = list_instruments(db, is_active=is_active, is_scheduled=is_scheduled, limit=limit, offset=offset)
    response.headers["X-Total-Count"] = str(
        count_instruments(db, is_active=is_active, is_scheduled=is_scheduled)
    )
    return rows


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
    the initial backfill of both price history and SEC fundamentals can take
    minutes.

    Each backfill is handed to Airflow via its REST API (a durable executor that
    survives an API restart) -- the prices DAG and the fundamentals DAG. For any
    DAG that Airflow won't accept (not configured or unreachable), it falls back
    to running that backfill in an in-process BackgroundTask -- lost on a uvicorn
    restart, but better than leaving the ticker with no history.
    """
    try:
        instrument = validate_and_upsert_ticker(body.ticker, is_scheduled=body.is_scheduled)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e

    ticker = body.ticker.strip().upper()

    # --- Prices backfill ---------------------------------------------------
    # The DAG resolves a missing end_dt to a single day, so pin it to today to
    # backfill the full [backfill_start, today] range.
    backfill_end = body.backfill_end or date.today().isoformat()
    prices_triggered = trigger_dag_run(
        _PRICES_DAG_ID,
        conf={
            "symbols_override": ticker,
            "start_dt": body.backfill_start,
            "end_dt": backfill_end,
        },
    )
    if not prices_triggered:
        logger.info("Airflow unavailable; running price backfill in-process for %s", ticker)
        background_tasks.add_task(
            register_ticker,
            body.ticker,
            is_scheduled=body.is_scheduled,
            backfill_start=body.backfill_start,
            backfill_end=body.backfill_end,
        )

    # --- Fundamentals backfill --------------------------------------------
    fundamentals_triggered = trigger_dag_run(
        _FUNDAMENTALS_DAG_ID,
        conf={"tickers_override": ticker},
    )
    if not fundamentals_triggered:
        logger.info("Airflow unavailable; running fundamentals backfill in-process for %s", ticker)
        background_tasks.add_task(run_fundamentals_pipeline, ticker)

    return instrument


@router.patch("/{ticker}/scheduled", response_model=InstrumentResponse)
def update_scheduled_route(ticker: str, body: ScheduledUpdate, db: DbSession):
    instrument = set_scheduled(db, ticker, body.is_scheduled)
    if instrument is None:
        raise HTTPException(status_code=404, detail=f"'{ticker}' is not registered")
    return instrument


@router.get("/{ticker}/figi", response_model=list[FigiResponse])
def get_instrument_figi_route(ticker: str, db: DbSession):
    """A ticker can resolve to several FIGI candidates across exchanges (see
    instrument_figi's docstring) -- this returns every one on file, not a
    single "the" FIGI.
    """
    ticker = ticker.upper()
    cache_key = f"figi:{ticker}"

    cached = cache_get_json(cache_key)
    if cached is not None:
        return cached

    if get_instrument(db, ticker) is None:
        raise HTTPException(status_code=404, detail=f"'{ticker}' is not registered")

    rows = get_figi_mappings(db, ticker)
    payload = [FigiResponse.model_validate(r).model_dump(mode="json") for r in rows]
    cache_set_json(cache_key, payload, ttl_seconds=300)

    return rows
