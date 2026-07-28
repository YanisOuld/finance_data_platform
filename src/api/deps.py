from __future__ import annotations

from fastapi import Header, HTTPException

from src.core.config import settings


def require_api_key(x_api_key: str | None = Header(default=None, alias="X-API-Key")) -> None:
    """Guards every route except /health with a shared-secret header.

    - environment != "local" and API_KEY unset: fail closed (500) -- a
      deployed environment must not silently run wide open.
    - environment == "local" and API_KEY unset: no enforcement, so local dev
      and the existing test suite don't need a fake key just to hit the API.
    - API_KEY set (any environment): always enforced.
    """
    if not settings.api_key:
        if settings.environment != "local":
            raise HTTPException(status_code=500, detail="API_KEY is not configured on the server")
        return

    if x_api_key != settings.api_key:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")
