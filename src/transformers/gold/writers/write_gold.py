import polars as pl

from src.core.database import SessionLocal
from src.data.crud.prices_1d import upsert_prices_1d


def write_gold_price1D(df: pl.DataFrame):
    """Upsert a PA price_1d dataframe into the gold/Postgres table.

    Returns the number of rows processed so callers can verify success.
    """
    with SessionLocal() as session:
        rows = df.to_dicts()
        upsert_prices_1d(session, rows)
        session.commit()
    return len(rows)
