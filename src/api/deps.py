from __future__ import annotations

import secrets
from typing import Annotated

from fastapi import Depends, Header, HTTPException
from sqlalchemy.orm import Session

from src.core.config import settings
from src.core.database import get_db
from src.data.crud.api_key import get_active_by_hash, hash_api_key, touch_last_used

DbSession = Annotated[Session, Depends(get_db)]


def _matches_admin_key(x_api_key: str | None) -> bool:
    """Constant-time check of the header against the env master key."""
    if not settings.api_key or x_api_key is None:
        return False
    return secrets.compare_digest(x_api_key, settings.api_key)


def require_api_key(
    db: DbSession,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> None:
    """Allow the request if it presents the env master key or an active DB key.
    Open only when environment="local" with no master key; else fail closed."""
    if not settings.api_key:
        if settings.environment != "local":
            raise HTTPException(status_code=500, detail="API_KEY is not configured on the server")
        return

    if _matches_admin_key(x_api_key):
        return

    if x_api_key:
        row = get_active_by_hash(db, hash_api_key(x_api_key))
        if row is not None:
            touch_last_used(db, row.id)
            return

    raise HTTPException(status_code=401, detail="Invalid or missing API key")


def require_admin_key(
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> None:
    """Key-management guard: only the env master key qualifies, not DB keys."""
    if not settings.api_key:
        if settings.environment != "local":
            raise HTTPException(status_code=500, detail="API_KEY is not configured on the server")
        return

    if not _matches_admin_key(x_api_key):
        raise HTTPException(status_code=401, detail="Admin API key required")
