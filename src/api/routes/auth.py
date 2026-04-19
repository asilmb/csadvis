"""Steam authentication endpoint.

POST /auth/steam — encrypt credentials via SteamCredentialManager and wake
                   the worker if it is waiting in PAUSED_AUTH state.

Encryption happens immediately after Pydantic validation, before any Redis
interaction, so plain-text values never leave the local request scope.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, field_validator

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/auth", tags=["auth"])


class SteamAuthRequest(BaseModel):
    steamLoginSecure: str
    session_id: str

    @field_validator("steamLoginSecure", "session_id")
    @classmethod
    def _not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("field must not be empty")
        return v.strip()


@router.post("/steam")
def set_steam_auth(req: SteamAuthRequest) -> dict:
    """
    Encrypt both tokens via SteamCredentialManager.set_credentials() and
    persist to Redis atomically.  Signals the worker to exit PAUSED_AUTH.

    Encryption occurs immediately after validation — req.steamLoginSecure and
    req.session_id exist only in this function's local scope.
    """
    try:
        from infra.steam_credentials import _get_manager
        _get_manager().set_credentials(req.steamLoginSecure, req.session_id)
    except Exception as exc:
        logger.error("auth: credential storage failed — %s", exc)
        raise HTTPException(status_code=500, detail=f"Credential storage error: {exc}")

    # Wake the worker immediately (it polls every 2 s anyway as a fallback).
    try:
        from infra.work_queue import signal_auth_ready
        signal_auth_ready()
    except Exception as exc:
        logger.warning("auth: could not signal worker — %s", exc)

    return {"ok": True, "message": "Credentials saved and worker notified."}
