import sqlalchemy as sa
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
