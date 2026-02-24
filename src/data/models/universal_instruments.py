from core.database import Base

from datetime import datetime

from sqlalchemy.orm import Mapped, mapped_column, func
from sqlalchemy import Boolean, Datetime, Integer, String

class UniversalInstrument(Base):
	__tablename__ = "universal_instruments"

	id: Mapped[int] = mapped_column(Integer, primary_key=True)
	ticker: Mapped[str] = mapped_column(String, nullable=False)
	name: Mapped[str]= mapped_column(String, nullable=False)
	exchange: Mapped[str] = mapped_column(String, nullable=True)
	currency: Mapped[str] = mapped_column(String, nullable=False)
	timezone: Mapped[str] = mapped_column(String, nullable=False)
	is_active: Mapped[bool] = mapped_column(Boolean, default=False)
	is_scheduled: Mapped[bool] = mapped_column(Boolean, default=False)

	created_at: Mapped[datetime] = mapped_column(Datetime, default=func.now)
	updated_at: Mapped[datetime] = mapped_column(Datetime, onupdate=func.now)

