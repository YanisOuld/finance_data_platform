import polars as pl

from src.core.database import SessionLocal
from src.data.crud.fundamentals import upsert_fundamentals


def write_gold_fundamentals(df: pl.DataFrame) -> int:
    """Upsert a fundamentals dataframe (ticker, concept, unit, period_end, ...) into Postgres."""
    with SessionLocal() as session:
        rows = df.to_dicts()
        upsert_fundamentals(session, rows)
        session.commit()
    return len(rows)
