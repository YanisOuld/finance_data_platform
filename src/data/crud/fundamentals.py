import sqlalchemy as sa
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from src.data.models.fundamentals import Fundamental


def upsert_fundamentals(session: Session, rows: list[dict]) -> int:
    """
    rows: list of dicts with keys matching columns (ticker, concept, unit,
    period_end, fp, form, fy, period_start, val, accn, filed, ...)
    """
    if not rows:
        return 0

    stmt = insert(Fundamental).values(rows)

    update_cols = {
        "fy": stmt.excluded.fy,
        "period_start": stmt.excluded.period_start,
        "val": stmt.excluded.val,
        "accn": stmt.excluded.accn,
        "filed": stmt.excluded.filed,
        "ingested_at": sa.func.now(),
    }
    if "run_id" in rows[0]:
        update_cols["run_id"] = stmt.excluded.run_id

    stmt = stmt.on_conflict_do_update(
        index_elements=[
            Fundamental.ticker,
            Fundamental.concept,
            Fundamental.unit,
            Fundamental.period_end,
            Fundamental.fp,
            Fundamental.form,
        ],
        set_=update_cols,
    )

    result = session.execute(stmt)
    return result.rowcount or 0


def get_fundamentals(
    session: Session, ticker: str, *, concept: str | None = None, limit: int = 500
) -> list[Fundamental]:
    stmt = select(Fundamental).where(Fundamental.ticker == ticker.upper())
    if concept is not None:
        stmt = stmt.where(Fundamental.concept == concept)
    stmt = stmt.order_by(Fundamental.period_end.desc()).limit(limit)
    return list(session.execute(stmt).scalars().all())
