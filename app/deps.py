import secrets
from typing import Annotated

from fastapi import Depends, Header, HTTPException

from app.config import Settings, get_settings


def require_api_key(
    x_api_key: Annotated[str | None, Header()] = None,
    settings: Settings = Depends(get_settings),
) -> None:
    if x_api_key is None or not secrets.compare_digest(x_api_key, settings.api_key):
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


def require_idempotency_key(
    idempotency_key: Annotated[str | None, Header()] = None,
) -> str:
    if idempotency_key is None or not idempotency_key.strip():
        raise HTTPException(status_code=400, detail="Idempotency-Key header is required")
    if len(idempotency_key.strip()) > 255:
        raise HTTPException(status_code=400, detail="Idempotency-Key is too long")
    return idempotency_key.strip()
