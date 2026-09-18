"""CRUD + token helpers for managed API keys. Only the SHA-256 hash is stored;
the plaintext is returned once at creation."""

from __future__ import annotations

import hashlib
import secrets
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.data.models.api_key import ApiKey

_TOKEN_PREFIX = "fdp_live_"
_PREFIX_VISIBLE_CHARS = len(_TOKEN_PREFIX) + 6  # e.g. "fdp_live_Ab12Cd"

VALID_SCOPES = ("read", "write")
DEFAULT_SCOPES = "read"


def hash_api_key(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def parse_scopes(raw: str | None) -> frozenset[str]:
    """Split a stored comma-separated scopes string into a set, dropping blanks
    and anything not in VALID_SCOPES."""
    if not raw:
        return frozenset()
    return frozenset(s.strip() for s in raw.split(",") if s.strip() in VALID_SCOPES)


def normalize_scopes(raw: str | None) -> str:
    """Validate + canonicalize scopes for storage. Falls back to DEFAULT_SCOPES
    when nothing valid is supplied; raises on an unknown scope so a typo is a
    400 at creation rather than a silently powerless key."""
    if raw is None:
        return DEFAULT_SCOPES
    requested = [s.strip().lower() for s in raw.split(",") if s.strip()]
    if not requested:
        return DEFAULT_SCOPES
    unknown = [s for s in requested if s not in VALID_SCOPES]
    if unknown:
        raise ValueError(f"Unknown scope(s) {unknown}; valid scopes are {list(VALID_SCOPES)}")
    # De-dupe while keeping VALID_SCOPES order for a stable stored value.
    return ",".join(s for s in VALID_SCOPES if s in requested)


def generate_token() -> str:
    """A URL-safe token like 'fdp_live_<43 random chars>'."""
    return f"{_TOKEN_PREFIX}{secrets.token_urlsafe(32)}"


def create_api_key(session: Session, label: str, scopes: str | None = None) -> tuple[ApiKey, str]:
    """Create a key and return (row, plaintext_token) -- the plaintext is shown once."""
    token = generate_token()
    row = ApiKey(
        label=label.strip() or "unnamed",
        prefix=token[:_PREFIX_VISIBLE_CHARS],
        key_hash=hash_api_key(token),
        scopes=normalize_scopes(scopes),
        is_active=True,
    )
    session.add(row)
    session.commit()
    session.refresh(row)
    return row, token


def list_api_keys(session: Session) -> list[ApiKey]:
    return list(session.execute(select(ApiKey).order_by(ApiKey.created_at.desc())).scalars().all())


def get_active_by_hash(session: Session, key_hash: str) -> ApiKey | None:
    stmt = select(ApiKey).where(ApiKey.key_hash == key_hash, ApiKey.is_active.is_(True))
    return session.execute(stmt).scalars().first()


def revoke_api_key(session: Session, key_id: int) -> ApiKey | None:
    row = session.get(ApiKey, key_id)
    if row is None:
        return None
    row.is_active = False
    session.commit()
    session.refresh(row)
    return row


def touch_last_used(session: Session, key_id: int) -> None:
    """Best-effort last-used stamp; never let a bookkeeping write break auth."""
    try:
        row = session.get(ApiKey, key_id)
        if row is not None:
            row.last_used_at = datetime.now(UTC)
            session.commit()
    except Exception:
        session.rollback()
