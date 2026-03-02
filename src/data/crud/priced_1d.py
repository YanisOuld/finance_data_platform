# upsert data

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from data.models.prices_1d import Price1D

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