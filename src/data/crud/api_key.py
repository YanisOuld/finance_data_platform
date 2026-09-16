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


def hash_api_key(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def generate_token() -> str:
    """A URL-safe token like 'fdp_live_<43 random chars>'."""
    return f"{_TOKEN_PREFIX}{secrets.token_urlsafe(32)}"


def create_api_key(session: Session, label: str) -> tuple[ApiKey, str]:
    """Create a key and return (row, plaintext_token) -- the plaintext is shown once."""
    token = generate_token()
    row = ApiKey(
        label=label.strip() or "unnamed",
        prefix=token[:_PREFIX_VISIBLE_CHARS],
        key_hash=hash_api_key(token),
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
