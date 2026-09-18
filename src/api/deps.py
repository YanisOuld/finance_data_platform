from __future__ import annotations

import secrets
from collections.abc import Callable
from dataclasses import dataclass
from typing import Annotated

from fastapi import Depends, Header, HTTPException
from sqlalchemy.orm import Session

from src.core.config import settings
from src.core.database import get_db
from src.core.ratelimit import enforce_rate_limit
from src.data.crud.api_key import get_active_by_hash, hash_api_key, parse_scopes, touch_last_used

DbSession = Annotated[Session, Depends(get_db)]

_ALL_SCOPES = frozenset({"read", "write"})


@dataclass(frozen=True)
class Principal:
    """The authenticated caller. `identity` keys the rate-limit buckets;
    `is_admin` (env master key or local mode) holds every scope and is exempt
    from rate limiting."""

    identity: str
    scopes: frozenset[str]
    is_admin: bool


def _matches_admin_key(x_api_key: str | None) -> bool:
    """Constant-time check of the header against the env master key."""
    if not settings.api_key or x_api_key is None:
        return False
    return secrets.compare_digest(x_api_key, settings.api_key)


def authenticate(
    db: DbSession,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> Principal:
    """Resolve the caller to a Principal or raise 401. The env master key (and
    local mode with no key configured) authenticates as admin -- full scopes,
    no rate limit. A DB-managed key authenticates with its own scopes and burns
    the general per-key rate-limit bucket. Open only when environment="local"
    with no master key; else fail closed."""
    if not settings.api_key:
        if settings.environment != "local":
            raise HTTPException(status_code=500, detail="API_KEY is not configured on the server")
        return Principal(identity="local", scopes=_ALL_SCOPES, is_admin=True)

    if _matches_admin_key(x_api_key):
        return Principal(identity="admin", scopes=_ALL_SCOPES, is_admin=True)

    if x_api_key:
        row = get_active_by_hash(db, hash_api_key(x_api_key))
        if row is not None:
            touch_last_used(db, row.id)
            enforce_rate_limit(row.key_hash, "req", settings.rate_limit_per_minute, 60)
            return Principal(identity=row.key_hash, scopes=parse_scopes(row.scopes), is_admin=False)

    raise HTTPException(status_code=401, detail="Invalid or missing API key")


CurrentPrincipal = Annotated[Principal, Depends(authenticate)]


def require_scope(scope: str) -> Callable[[Principal], Principal]:
    """Build a dependency that authenticates, then 403s unless the caller holds
    `scope`. Used at router level for read-only routers."""

    def dependency(principal: CurrentPrincipal) -> Principal:
        if scope not in principal.scopes:
            raise HTTPException(status_code=403, detail=f"This API key lacks the '{scope}' scope")
        return principal

    return dependency


require_read = require_scope("read")


def require_write(principal: CurrentPrincipal) -> Principal:
    """Write/ingestion guard: needs the 'write' scope and additionally burns the
    stricter write rate-limit bucket, so a flood of register/refresh triggers
    can't hammer the upstream providers (Yahoo/SEC). Admin is exempt from the
    write limit but still needs no scope check."""
    if "write" not in principal.scopes:
        raise HTTPException(status_code=403, detail="This API key lacks the 'write' scope")
    if not principal.is_admin:
        enforce_rate_limit(principal.identity, "write", settings.rate_limit_write_per_minute, 60)
    return principal


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
