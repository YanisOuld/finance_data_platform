from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from sqlalchemy.orm import Session

from src.api.deps import require_api_key
from src.api.schemas import FundamentalResponse
from src.core.cache import cache_get_json, cache_set_json
from src.core.database import get_db
from src.data.crud.fundamentals import count_fundamentals, get_fundamentals
from src.data.crud.universal_instruments import get_instrument

router = APIRouter(prefix="/fundamentals", tags=["fundamentals"], dependencies=[Depends(require_api_key)])

DbSession = Annotated[Session, Depends(get_db)]

_CACHE_TTL_SECONDS = 300  # fundamentals refresh weekly at most (see run_fundamentals.py)


@router.get("/{ticker}", response_model=list[FundamentalResponse])
def get_fundamentals_route(
    ticker: str,
    db: DbSession,
    response: Response,
    concept: str | None = None,
    limit: int = Query(default=500, le=5000),
    offset: int = Query(default=0, ge=0),
):
    ticker = ticker.upper()
    cache_key = f"fundamentals:{ticker}:{concept}:{limit}:{offset}"

    cached = cache_get_json(cache_key)
    if cached is not None:
        response.headers["X-Total-Count"] = str(cached["total"])
        response.headers["X-Cache"] = "HIT"
        return cached["rows"]

    if get_instrument(db, ticker) is None:
        raise HTTPException(status_code=404, detail=f"'{ticker}' is not registered")

    rows = get_fundamentals(db, ticker, concept=concept, limit=limit, offset=offset)
    total = count_fundamentals(db, ticker, concept=concept)

    payload = [FundamentalResponse.model_validate(r).model_dump(mode="json") for r in rows]
    cache_set_json(cache_key, {"rows": payload, "total": total}, ttl_seconds=_CACHE_TTL_SECONDS)

    response.headers["X-Total-Count"] = str(total)
    response.headers["X-Cache"] = "MISS"
    return rows
