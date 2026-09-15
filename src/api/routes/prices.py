from __future__ import annotations

from datetime import date
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from sqlalchemy.orm import Session

from src.api.deps import require_api_key
from src.api.schemas import PriceResponse
from src.core.cache import cache_get_json, cache_set_json
from src.core.database import get_db
from src.data.crud.prices_1d import count_prices, get_prices
from src.data.crud.universal_instruments import get_instrument

router = APIRouter(prefix="/prices", tags=["prices"], dependencies=[Depends(require_api_key)])

DbSession = Annotated[Session, Depends(get_db)]

_CACHE_TTL_SECONDS = 30


@router.get("/{ticker}", response_model=list[PriceResponse])
def get_prices_route(
    ticker: str,
    db: DbSession,
    response: Response,
    start: date | None = None,
    end: date | None = None,
    limit: int = Query(default=500, le=5000),
    offset: int = Query(default=0, ge=0),
    sort_by: Literal["ts", "open", "high", "low", "close", "volume", "close_returns"] = "ts",
    order: Literal["asc", "desc"] = "desc",
):
    ticker = ticker.upper()
    cache_key = f"prices:{ticker}:{start}:{end}:{limit}:{offset}:{sort_by}:{order}"

    cached = cache_get_json(cache_key)
    if cached is not None:
        response.headers["X-Total-Count"] = str(cached["total"])
        response.headers["X-Cache"] = "HIT"
        return cached["rows"]

    if get_instrument(db, ticker) is None:
        raise HTTPException(status_code=404, detail=f"'{ticker}' is not registered")

    rows = get_prices(
        db, ticker, start=start, end=end, limit=limit, offset=offset, sort_by=sort_by, order=order
    )
    total = count_prices(db, ticker, start=start, end=end)

    payload = [PriceResponse.model_validate(r).model_dump(mode="json") for r in rows]
    cache_set_json(cache_key, {"rows": payload, "total": total}, ttl_seconds=_CACHE_TTL_SECONDS)

    response.headers["X-Total-Count"] = str(total)
    response.headers["X-Cache"] = "MISS"
    return rows
