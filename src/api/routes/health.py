from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from src.core.database import get_db

router = APIRouter(tags=["health"])

DbSession = Annotated[Session, Depends(get_db)]


@router.get("/health")
def health(db: DbSession):
    """No API key required -- Docker/load-balancer health checks need to hit
    this without a secret. Confirms the DB is actually reachable, not just
    that the process is alive.
    """
    try:
        db.execute(text("SELECT 1"))
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"database unavailable: {e}") from e
    return {"status": "ok"}
