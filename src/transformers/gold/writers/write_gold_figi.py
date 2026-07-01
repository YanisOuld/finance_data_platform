import polars as pl

from src.core.database import SessionLocal
from src.data.crud.instrument_figi import upsert_instrument_figi


def write_gold_figi(df: pl.DataFrame) -> int:
    """Upsert an instrument_figi dataframe (ticker, figi, ...) into Postgres."""
    with SessionLocal() as session:
        rows = df.to_dicts()
        upsert_instrument_figi(session, rows)
        session.commit()
    return len(rows)
