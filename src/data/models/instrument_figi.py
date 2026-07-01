from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, String, text
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from src.core.database import Base


class InstrumentFigi(Base):
    """OpenFIGI candidate mapping(s) for a ticker.

    A ticker can be ambiguous across exchanges, so OpenFIGI may return
    several FIGIs for the same ticker -- (ticker, figi) is the primary key
    rather than ticker alone, so every candidate listing is kept instead of
    silently overwriting one with another.
    """

    __tablename__ = "instrument_figi"

    ticker: Mapped[str] = mapped_column(String, ForeignKey("universal_instruments.ticker"), primary_key=True)
    figi: Mapped[str] = mapped_column(String, primary_key=True)

    composite_figi: Mapped[str | None] = mapped_column(String, nullable=True)
    share_class_figi: Mapped[str | None] = mapped_column(String, nullable=True)
    security_type: Mapped[str | None] = mapped_column(String, nullable=True)
    market_sector: Mapped[str | None] = mapped_column(String, nullable=True)
    exch_code: Mapped[str | None] = mapped_column(String, nullable=True)
    name: Mapped[str | None] = mapped_column(String, nullable=True)

    source: Mapped[str] = mapped_column(String, nullable=False, server_default=text("'openfigi'"))
    run_id: Mapped[str | None] = mapped_column(String, nullable=True)

    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
