# upsert data

from datetime import date

import sqlalchemy as sa
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from src.data.models.prices_1d import Price1D


def upsert_prices_1d(session: Session, rows: list[dict]) -> int:
    """
    rows: list of dicts with keys matching columns (symbol, ts, open, ...)
    """
    stmt = insert(Price1D).values(rows)

    # columns to update if conflict
    update_cols = {
        "open": stmt.excluded.open,
        "high": stmt.excluded.high,
        "low": stmt.excluded.low,
        "close": stmt.excluded.close,
        "volume": stmt.excluded.volume,
        "dividends": stmt.excluded.dividends,
        "stock_split": stmt.excluded.stock_split,
        "close_returns": stmt.excluded.close_returns,
        "run_id": stmt.excluded.run_id,
        "ingested_at": sa.func.now(),
    }

    stmt = stmt.on_conflict_do_update(
        index_elements=[Price1D.symbol, Price1D.ts],
        set_=update_cols,
    )

    result = session.execute(stmt)
    return result.rowcount or 0


def get_prices(
    session: Session,
    symbol: str,
    *,
    start: date | None = None,
    end: date | None = None,
    limit: int = 500,
) -> list[Price1D]:
    stmt = select(Price1D).where(Price1D.symbol == symbol.upper())
    if start is not None:
        stmt = stmt.where(Price1D.ts >= start)
    if end is not None:
        stmt = stmt.where(Price1D.ts <= end)
    stmt = stmt.order_by(Price1D.ts.asc()).limit(limit)
    return list(session.execute(stmt).scalars().all())
