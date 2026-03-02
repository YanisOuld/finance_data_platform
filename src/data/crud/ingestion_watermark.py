import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select
from sqlalchemy.orm import Session
from datetime import date

from src.data.models.ingestion_watermark import IngestionWatermark


def get_last_ts(session: Session, ticker: str) -> date | None:
    stmt = select(IngestionWatermark.last_ts).where(
        IngestionWatermark.source == "yahoo",
        IngestionWatermark.dataset == "prices_1d",
        IngestionWatermark.ticker == ticker,
    )

    result = session.execute(stmt).scalar_one_or_none()
    return result


import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert

def upsert_watermark(
    session: Session,
    ticker: str,
    last_ts: date,
    run_id: str,
):
    stmt = insert(IngestionWatermark).values(
        source="yahoo",
        dataset="prices_1d",
        ticker=ticker,
        last_ts=last_ts,
        last_run_id=run_id,
    )

    stmt = stmt.on_conflict_do_update(
        index_elements=["source", "dataset", "ticker"],
        set_={
            "last_ts": stmt.excluded.last_ts,
            "last_run_id": stmt.excluded.last_run_id,
            "updated_at": sa.func.now(),
        },
    )

    session.execute(stmt)