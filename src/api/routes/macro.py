from __future__ import annotations

from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from sqlalchemy.orm import Session

from src.api.deps import require_api_key
from src.api.schemas import MacroSeriesResponse
from src.core.cache import cache_get_json, cache_set_json
from src.core.constants import FRED_COLUMN_SERIES
from src.core.database import get_db
from src.data.crud.macro_series import count_macro_series, get_macro_series

router = APIRouter(prefix="/macro", tags=["macro"], dependencies=[Depends(require_api_key)])

DbSession = Annotated[Session, Depends(get_db)]

_CACHE_TTL_SECONDS = 300  # macro series refresh weekly at most (see run_macro.py)


@router.get("/{series}", response_model=list[MacroSeriesResponse])
def get_macro_series_route(
    series: str,
    db: DbSession,
    response: Response,
    start: date | None = None,
    end: date | None = None,
    limit: int = Query(default=500, le=5000),
    offset: int = Query(default=0, ge=0),
):
    series = series.lower()
    if series not in FRED_COLUMN_SERIES:
        raise HTTPException(
            status_code=404, detail=f"Unknown macro series '{series}'. Known: {sorted(FRED_COLUMN_SERIES)}"
        )

    cache_key = f"macro:{series}:{start}:{end}:{limit}:{offset}"

    cached = cache_get_json(cache_key)
    if cached is not None:
        response.headers["X-Total-Count"] = str(cached["total"])
        response.headers["X-Cache"] = "HIT"
        return cached["rows"]

    rows = get_macro_series(db, series, start=start, end=end, limit=limit, offset=offset)
    total = count_macro_series(db, series, start=start, end=end)

    payload = [MacroSeriesResponse.model_validate(r).model_dump(mode="json") for r in rows]
    cache_set_json(cache_key, {"rows": payload, "total": total}, ttl_seconds=_CACHE_TTL_SECONDS)

    response.headers["X-Total-Count"] = str(total)
    response.headers["X-Cache"] = "MISS"
    return rows
