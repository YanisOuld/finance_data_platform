from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Integer, String, text
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from src.core.database import Base


class ApiKey(Base):
    """A managed API key presented in the X-API-Key header. Only the token's
    SHA-256 hash is stored; `prefix` is a visible fragment for recognition."""

    __tablename__ = "api_keys"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    label: Mapped[str] = mapped_column(String, nullable=False)
    prefix: Mapped[str] = mapped_column(String, nullable=False)
    key_hash: Mapped[str] = mapped_column(String, nullable=False, unique=True, index=True)

    # Comma-separated grants from {"read", "write"}. Read serves Gold data;
    # write authorizes ingestion triggers (register / refresh). New keys default
    # to read-only -- see src/data/crud/api_key.py.
    scopes: Mapped[str] = mapped_column(String, nullable=False, server_default=text("'read'"))

    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    last_used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
