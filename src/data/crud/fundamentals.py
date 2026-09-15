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


# Columns the API is allowed to sort by (keyed by the client-facing string so an
# arbitrary value can never reach the ORM). Unknown keys fall back to period_end.
SORTABLE_COLUMNS = {
    "period_end": Fundamental.period_end,
    "concept": Fundamental.concept,
    "val": Fundamental.val,
    "fy": Fundamental.fy,
    "form": Fundamental.form,
    "fp": Fundamental.fp,
}


def _apply_filters(stmt, ticker, concept, concepts, form):
    stmt = stmt.where(Fundamental.ticker == ticker.upper())
    if concept is not None:
        stmt = stmt.where(Fundamental.concept == concept)
    if concepts:
        stmt = stmt.where(Fundamental.concept.in_(concepts))
    if form is not None:
        stmt = stmt.where(Fundamental.form == form)
    return stmt


def get_fundamentals(
    session: Session,
    ticker: str,
    *,
    concept: str | None = None,
    concepts: list[str] | None = None,
    form: str | None = None,
    limit: int = 500,
    offset: int = 0,
    sort_by: str = "period_end",
    order: str = "desc",
) -> list[Fundamental]:
    stmt = _apply_filters(select(Fundamental), ticker, concept, concepts, form)

    col = SORTABLE_COLUMNS.get(sort_by, Fundamental.period_end)
    col = col.desc() if order == "desc" else col.asc()
    # Secondary key on concept keeps pagination stable when many rows share the
    # primary sort value (e.g. dozens of concepts with the same period_end).
    stmt = stmt.order_by(col, Fundamental.concept.asc()).offset(offset).limit(limit)
    return list(session.execute(stmt).scalars().all())


def count_fundamentals(
    session: Session,
    ticker: str,
    *,
    concept: str | None = None,
    concepts: list[str] | None = None,
    form: str | None = None,
) -> int:
    stmt = _apply_filters(select(sa.func.count()).select_from(Fundamental), ticker, concept, concepts, form)
    return session.execute(stmt).scalar_one()


def list_concepts(session: Session, ticker: str) -> list[str]:
    """Distinct XBRL concept tags on file for a ticker -- powers the explorer's
    concept autocomplete so the user picks from what actually exists instead of
    guessing exact us-gaap:/dei: tag names."""
    stmt = (
        select(Fundamental.concept)
        .where(Fundamental.ticker == ticker.upper())
        .distinct()
        .order_by(Fundamental.concept.asc())
    )
    return list(session.execute(stmt).scalars().all())
