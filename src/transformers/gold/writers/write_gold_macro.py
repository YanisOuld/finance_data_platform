import polars as pl

from src.data.crud.macro_series import upsert_macro_series
from src.core.database import SessionLocal


def write_gold_macro(df: pl.DataFrame) -> int:
    """Upsert a macro_series dataframe (series, ts, value, ...) into Postgres."""
    with SessionLocal() as session:
        rows = df.to_dicts()
        upsert_macro_series(session, rows)
        session.commit()
    return len(rows)
