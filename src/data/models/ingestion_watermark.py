from src.core.database import Base

from datetime import datetime, date
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func
from sqlalchemy import Date, DateTime, String


class IngestionWatermark(Base):
    __tablename__ = "ingestion_watermarks"

    source: Mapped[str] = mapped_column(String, primary_key=True)
    dataset: Mapped[str] = mapped_column(String, primary_key=True)
    ticker: Mapped[str] = mapped_column(String, primary_key=True)

    last_ts: Mapped[date | None] = mapped_column(Date, nullable=True)
    last_run_id: Mapped[str | None] = mapped_column(String, nullable=True)

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )