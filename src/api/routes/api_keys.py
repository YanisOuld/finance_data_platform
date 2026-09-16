from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from src.api.deps import require_admin_key
from src.api.schemas import ApiKeyCreate, ApiKeyCreated, ApiKeyInfo
from src.core.database import get_db
from src.data.crud.api_key import create_api_key, list_api_keys, revoke_api_key

# Admin-only: gated on the env master key.
router = APIRouter(prefix="/admin/api-keys", tags=["api-keys"], dependencies=[Depends(require_admin_key)])

DbSession = Annotated[Session, Depends(get_db)]


@router.get("", response_model=list[ApiKeyInfo])
def list_api_keys_route(db: DbSession):
    return list_api_keys(db)


@router.post("", response_model=ApiKeyCreated, status_code=201)
def create_api_key_route(body: ApiKeyCreate, db: DbSession):
    """Create a key; the response carries the plaintext token once."""
    row, token = create_api_key(db, body.label)
    return ApiKeyCreated(
        id=row.id,
        label=row.label,
        prefix=row.prefix,
        is_active=row.is_active,
        created_at=row.created_at,
        last_used_at=row.last_used_at,
        key=token,
    )


@router.delete("/{key_id}", response_model=ApiKeyInfo)
def revoke_api_key_route(key_id: int, db: DbSession):
    """Deactivate a key (kept as a row, stops authenticating)."""
    row = revoke_api_key(db, key_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"API key {key_id} not found")
    return row
