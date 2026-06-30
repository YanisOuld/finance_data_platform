from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from sqlalchemy import Date, DateTime, Float, String, text
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from src.core.database import Base


class MacroSeries(Base):
    __tablename__ = "macro_series"

    series: Mapped[str] = mapped_column(String, primary_key=True)
    ts: Mapped[date] = mapped_column(Date, primary_key=True)

    value: Mapped[Optional[float]] = mapped_column(Float)

    source: Mapped[str] = mapped_column(String, nullable=False, server_default=text("'fred'"))
    run_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
