from datetime import date

import sqlalchemy as sa
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from src.data.models.macro_series import MacroSeries


def upsert_macro_series(session: Session, rows: list[dict]) -> int:
    """
    rows: list of dicts with keys matching columns (series, ts, value, ...)
    """
    if not rows:
        return 0

    stmt = insert(MacroSeries).values(rows)

    update_cols = {
        "value": stmt.excluded.value,
        "ingested_at": sa.func.now(),
    }
    if "run_id" in rows[0]:
        update_cols["run_id"] = stmt.excluded.run_id

    stmt = stmt.on_conflict_do_update(
        index_elements=[MacroSeries.series, MacroSeries.ts],
        set_=update_cols,
    )

    result = session.execute(stmt)
    return result.rowcount or 0


def get_macro_series(
    session: Session,
    series: str,
    *,
    start: date | None = None,
    end: date | None = None,
    limit: int = 500,
    offset: int = 0,
) -> list[MacroSeries]:
    stmt = select(MacroSeries).where(MacroSeries.series == series.lower())
    if start is not None:
        stmt = stmt.where(MacroSeries.ts >= start)
    if end is not None:
        stmt = stmt.where(MacroSeries.ts <= end)
    stmt = stmt.order_by(MacroSeries.ts.asc()).offset(offset).limit(limit)
    return list(session.execute(stmt).scalars().all())


def count_macro_series(
    session: Session, series: str, *, start: date | None = None, end: date | None = None
) -> int:
    stmt = select(sa.func.count()).select_from(MacroSeries).where(MacroSeries.series == series.lower())
    if start is not None:
        stmt = stmt.where(MacroSeries.ts >= start)
    if end is not None:
        stmt = stmt.where(MacroSeries.ts <= end)
    return session.execute(stmt).scalar_one()
