from core.database import Base

from datetime import datetime, date

from sqlalchemy import Integer, String, DateTime, Date
from sqlalchemy.orm import Mapped, mapped_column, func

class IngestionRun(Base):
	id: Mapped[int] = mapped_column(Integer, primary_key=True)
	ts: Mapped[date] = mapped_column(Date)
	dataset: Mapped[str] = mapped_column(String)

	status: Mapped[str] = mapped_column(String) # Mettre un enum
	tickers_total: Mapped[int] = mapped_column(Integer, default=0)
	tickers_success: Mapped[int] = mapped_column(Integer)
	tickers_failed: Mapped[int] = mapped_column(Integer)
	
	created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now)
	finished_at: Mapped[datetime] = mapped_column(DateTime)