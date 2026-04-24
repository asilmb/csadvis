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
    steamLoginSecure: str | None = None
    session_id: str | None = None

    @field_validator("steamLoginSecure", "session_id", mode="before")
    @classmethod
    def _strip_empty(cls, v: object) -> object:
        if isinstance(v, str):
            v = v.strip()
            return v if v else None
        return v


@router.post("/steam")
def set_steam_auth(req: SteamAuthRequest) -> dict:
    """
    Update only the credentials that were provided — each field is saved
    independently.  At least one field must be non-empty.
    """
    if not req.steamLoginSecure and not req.session_id:
        raise HTTPException(status_code=422, detail="Заполни хотя бы одно поле.")
    try:
        from infra.steam_credentials import set_login_secure, set_session_id
        if req.steamLoginSecure:
            set_login_secure(req.steamLoginSecure)
        if req.session_id:
            set_session_id(req.session_id)
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
