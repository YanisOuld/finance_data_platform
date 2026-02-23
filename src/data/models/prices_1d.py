from core.database import Base

from datetime import datetime

from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import Integer, String, DateTime, Numeric

class Prices_1d(Base):
	__tablename__ = "prices_1d"

	id: Mapped[int] = mapped_column(Integer, primary_key=True)
	ts: Mapped[datetime] = mapped_column(DateTime, nullable=False)
	ticker: Mapped[str] = mapped_column(String, nullable=False)
	open: Mapped[float] = mapped_column(Numeric, nullable=False)
	close: Mapped[float] = mapped_column(Numeric, nullable=False)
	high: Mapped[float] = mapped_column(Numeric, nullable=False)
	low: Mapped[float] = mapped_column(Numeric, nullable=False)
	volume: Mapped[float] = mapped_column(Integer)
	stock_splits: Mapped[float] = mapped_column(Numeric, default=0.0)
	dividends: Mapped[float] = mapped_column(Numeric, default=0.0)