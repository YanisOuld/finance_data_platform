from core.database import Base

from datetime import date, datetime

from sqlalchemy.orm import Mapped, mapped_column, func
from sqlalchemy import Integer, String, Date, Datetime, Numeric

class Price_1d(Base):
	__tablename__ = "prices_1d"

	id: Mapped[int] = mapped_column(Integer, primary_key=True)
	ts: Mapped[date] = mapped_column(Date, nullable=False)
	ticker: Mapped[str] = mapped_column(String, nullable=False)
	source: Mapped[str] = mapped_column(String, nullable=True)
	open: Mapped[float] = mapped_column(Numeric, nullable=False)
	close: Mapped[float] = mapped_column(Numeric, nullable=False)
	high: Mapped[float] = mapped_column(Numeric, nullable=False)
	low: Mapped[float] = mapped_column(Numeric, nullable=False)
	volume: Mapped[float] = mapped_column(Integer)
	stock_splits: Mapped[float] = mapped_column(Numeric, default=0.0)
	dividends: Mapped[float] = mapped_column(Numeric, default=0.0)

	created_at: Mapped[datetime] = mapped_column(Datetime, server_default=func.now)
	modified_at: Mapped[datetime] = mapped_column(Datetime, onupdate=func.now)
