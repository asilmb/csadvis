"""
Auth Bridge — Tampermonkey Integration (PV-06).

POST /api/v1/auth/update_session
    Receives a fresh steamLoginSecure (and optional sessionid) from the
    Tampermonkey browser extension, persists it, and instantly unblocks
    all paused Celery tasks.

Flow
----
1.  Validate: cookie value must be non-empty.
2.  Update runtime os.environ + pydantic settings (no restart needed).
3.  Persist to .env so the next cold-start picks up the new cookie.
4.  Delete STEALTH_BLOCK_EXPIRES Redis key — workers resume immediately.
5.  Return {"ok": true, "unblocked": true}.
"""

from __future__ import annotations

import logging
import os

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from config import settings, _ENV_FILE

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/auth", tags=["auth"])


# ─── Schema ───────────────────────────────────────────────────────────────────


class UpdateSessionRequest(BaseModel):
    steamLoginSecure: str
    sessionid: str = ""
    note: str = ""  # optional free-text label (e.g. "Tampermonkey auto-update")


class UpdateSessionResponse(BaseModel):
    ok: bool
    unblocked: bool
    note: str = ""


# ─── Endpoint ─────────────────────────────────────────────────────────────────


@router.post("/update_session", response_model=UpdateSessionResponse)
def update_session(req: UpdateSessionRequest) -> UpdateSessionResponse:
    """
    Accept a fresh Steam session cookie from Tampermonkey and immediately
    unblock all stalled Celery workers.

    Security: the cookie value is never written to logs — only a masked
    confirmation and the ``note`` field are logged.
    """
    cookie = req.steamLoginSecure.strip()
    if not cookie:
        raise HTTPException(status_code=400, detail="steamLoginSecure cannot be empty")

    logger.info(
        "Auth bridge: new session received — note=%r (cookie masked)",
        req.note or "(none)",
    )

    # ── 1. Update runtime environment ────────────────────────────────────────
    os.environ["STEAM_LOGIN_SECURE"] = cookie
    settings.steam_login_secure = cookie  # pydantic-settings v2 is not frozen

    if req.sessionid.strip():
        os.environ["STEAM_SESSION_ID"] = req.sessionid.strip()
        settings.steam_session_id = req.sessionid.strip()

    # ── 2. Persist to .env for next restart ──────────────────────────────────
    try:
        from dotenv import set_key
        set_key(str(_ENV_FILE), "STEAM_LOGIN_SECURE", cookie)
        if req.sessionid.strip():
            set_key(str(_ENV_FILE), "STEAM_SESSION_ID", req.sessionid.strip())
    except Exception as exc:
        logger.warning("Auth bridge: could not persist to .env: %s", exc)

    # ── 3. Remove STEALTH_BLOCK_EXPIRES from Redis ────────────────────────────
    unblocked = _clear_stealth_block()

    logger.info("Auth bridge: session update complete — unblocked=%s", unblocked)
    return UpdateSessionResponse(ok=True, unblocked=unblocked, note=req.note)


# ─── Helpers ──────────────────────────────────────────────────────────────────


def _clear_stealth_block() -> bool:
    """Delete STEALTH_BLOCK_EXPIRES from Redis. Returns True if key existed."""
    try:
        from scheduler.tasks import _redis, _STEALTH_KEY
        deleted = _redis().delete(_STEALTH_KEY)
        if deleted:
            logger.info("Auth bridge: STEALTH_BLOCK_EXPIRES cleared — workers unblocked")
        return bool(deleted)
    except Exception as exc:
        logger.warning("Auth bridge: could not clear stealth block: %s", exc)
        return False
