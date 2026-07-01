import sqlalchemy as sa
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from src.data.models.instrument_figi import InstrumentFigi


def upsert_instrument_figi(session: Session, rows: list[dict]) -> int:
    """
    rows: list of dicts with keys matching columns (ticker, figi,
    composite_figi, share_class_figi, security_type, market_sector,
    exch_code, name, ...)
    """
    if not rows:
        return 0

    stmt = insert(InstrumentFigi).values(rows)

    update_cols = {
        "composite_figi": stmt.excluded.composite_figi,
        "share_class_figi": stmt.excluded.share_class_figi,
        "security_type": stmt.excluded.security_type,
        "market_sector": stmt.excluded.market_sector,
        "exch_code": stmt.excluded.exch_code,
        "name": stmt.excluded.name,
        "ingested_at": sa.func.now(),
    }
    if "run_id" in rows[0]:
        update_cols["run_id"] = stmt.excluded.run_id

    stmt = stmt.on_conflict_do_update(
        index_elements=[InstrumentFigi.ticker, InstrumentFigi.figi],
        set_=update_cols,
    )

    result = session.execute(stmt)
    return result.rowcount or 0


def get_figi_mappings(session: Session, ticker: str) -> list[InstrumentFigi]:
    """All FIGI candidates on file for a ticker (a ticker can be ambiguous
    across exchanges, so this may return more than one row).
    """
    stmt = select(InstrumentFigi).where(InstrumentFigi.ticker == ticker.upper())
    return list(session.execute(stmt).scalars().all())
