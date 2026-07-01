from __future__ import annotations

from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from src.api.schemas import PriceResponse
from src.core.database import get_db
from src.data.crud.prices_1d import get_prices
from src.data.crud.universal_instruments import get_instrument

router = APIRouter(prefix="/prices", tags=["prices"])

DbSession = Annotated[Session, Depends(get_db)]


@router.get("/{ticker}", response_model=list[PriceResponse])
def get_prices_route(
    ticker: str,
    db: DbSession,
    start: date | None = None,
    end: date | None = None,
    limit: int = Query(default=500, le=5000),
):
    if get_instrument(db, ticker) is None:
        raise HTTPException(status_code=404, detail=f"'{ticker}' is not registered")
    return get_prices(db, ticker, start=start, end=end, limit=limit)
