from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import BigInteger, Date, DateTime, Float, String, text
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from src.core.database import Base


class Fundamental(Base):
    __tablename__ = "fundamentals"

    ticker: Mapped[str] = mapped_column(String, primary_key=True)
    concept: Mapped[str] = mapped_column(String, primary_key=True)
    unit: Mapped[str] = mapped_column(String, primary_key=True)
    period_end: Mapped[date] = mapped_column(Date, primary_key=True)
    fp: Mapped[str] = mapped_column(String, primary_key=True)
    form: Mapped[str] = mapped_column(String, primary_key=True)

    fy: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    period_start: Mapped[date | None] = mapped_column(Date, nullable=True)
    val: Mapped[float] = mapped_column(Float, nullable=False)
    accn: Mapped[str | None] = mapped_column(String, nullable=True)
    filed: Mapped[date | None] = mapped_column(Date, nullable=True)

    source: Mapped[str] = mapped_column(String, nullable=False, server_default=text("'sec_edgar'"))
    run_id: Mapped[str | None] = mapped_column(String, nullable=True)

    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
