# src/db/models/prices_1d.py
from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from sqlalchemy import Date, DateTime, Float, BigInteger, String, text
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from src.core.database import Base  # ton declarative base


class Price1D(Base):
    __tablename__ = "prices_1d"

    symbol: Mapped[str] = mapped_column(String, primary_key=True)
    ts: Mapped[date] = mapped_column(Date, primary_key=True)

    open: Mapped[Optional[float]] = mapped_column(Float)
    high: Mapped[Optional[float]] = mapped_column(Float)
    low: Mapped[Optional[float]] = mapped_column(Float)
    close: Mapped[Optional[float]] = mapped_column(Float)

    volume: Mapped[Optional[int]] = mapped_column(BigInteger)
    dividends: Mapped[Optional[float]] = mapped_column(Float)
    stock_split: Mapped[Optional[float]] = mapped_column(Float)
    close_returns: Mapped[Optional[float]] = mapped_column(Float)

    source: Mapped[str] = mapped_column(String, nullable=False, server_default=text("'yahoo'"))
    run_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )