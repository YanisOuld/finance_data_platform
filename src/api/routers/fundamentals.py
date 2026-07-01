from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from src.api.schemas import FundamentalResponse
from src.core.database import get_db
from src.data.crud.fundamentals import get_fundamentals
from src.data.crud.universal_instruments import get_instrument

router = APIRouter(prefix="/fundamentals", tags=["fundamentals"])

DbSession = Annotated[Session, Depends(get_db)]


@router.get("/{ticker}", response_model=list[FundamentalResponse])
def get_fundamentals_route(
    ticker: str,
    db: DbSession,
    concept: str | None = None,
    limit: int = Query(default=500, le=5000),
):
    if get_instrument(db, ticker) is None:
        raise HTTPException(status_code=404, detail=f"'{ticker}' is not registered")
    return get_fundamentals(db, ticker, concept=concept, limit=limit)
