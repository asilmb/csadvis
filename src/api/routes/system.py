"""
System management endpoints (PV-43).

POST /system/update-cookie  — hot-swap steamLoginSecure, validate, reset FAILED tasks
GET  /system/cookie-status  — return current cookie status from DB
"""

from __future__ import annotations

import logging
import os

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from config import settings, _ENV_FILE

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/system", tags=["system"])


class UpdateCookieRequest(BaseModel):
    value: str
    session_note: str = ""   # optional description saved to SystemSettings.last_auth_note
    sessionid: str = ""      # Steam sessionid cookie — required for /myhistory (PV-52)


class CookieStatusResponse(BaseModel):
    status: str  # VALID | EXPIRED | UNKNOWN


@router.get("/cookie-status", response_model=CookieStatusResponse)
def cookie_status_endpoint() -> CookieStatusResponse:
    from src.domain.connection import SessionLocal
    from src.domain.sql_repositories import get_cookie_status
    with SessionLocal() as db:
        status = get_cookie_status(db)
    return CookieStatusResponse(status=status)


@router.post("/update-cookie")
def update_cookie_endpoint(req: UpdateCookieRequest) -> dict:
    value = req.value.strip()
    if not value:
        raise HTTPException(status_code=400, detail="Cookie value cannot be empty")

    # SECURITY: cookie value is NEVER logged — only a masked confirmation
    logger.info("Cookie update requested — applying new value (masked: ***)")

    # 1. Update runtime settings (in-memory) — no restart needed
    os.environ["STEAM_LOGIN_SECURE"] = value
    settings.steam_login_secure = value  # pydantic-settings v2 not frozen

    if req.sessionid.strip():
        sid = req.sessionid.strip()
        os.environ["STEAM_SESSION_ID"] = sid
        settings.steam_session_id = sid

    # 2. Persist to .env for next restart
    try:
        from dotenv import set_key
        set_key(str(_ENV_FILE), "STEAM_LOGIN_SECURE", value)
        if req.sessionid.strip():
            set_key(str(_ENV_FILE), "STEAM_SESSION_ID", req.sessionid.strip())
        logger.info("Cookie(s) persisted to .env (masked)")
    except Exception as exc:
        logger.warning("Could not persist cookie to .env: %s", exc)

    # 3. Validate cookie by attempting a wallet sync (sets cookie_status=VALID on success)
    from scrapper.steam_sync import sync_wallet
    result = sync_wallet()

    if not result.ok:
        logger.warning("Cookie validation failed: error_code=%s", result.error_code)
        return {"ok": False, "error": result.error_code or "VALIDATION_FAILED"}

    # 5. Save session note if provided (never logs the cookie itself)
    if req.session_note.strip():
        try:
            from src.domain.connection import SessionLocal as _SL
            from src.domain.models import SystemSettings
            from datetime import UTC, datetime
            with _SL() as _db:
                row = _db.get(SystemSettings, "last_auth_note")
                now = datetime.now(UTC).replace(tzinfo=None)
                if row is None:
                    row = SystemSettings(key="last_auth_note", value=req.session_note.strip())
                    _db.add(row)
                else:
                    row.value = req.session_note.strip()
                    row.updated_at = now
                _db.commit()
        except Exception as exc:
            logger.warning("Could not save session note: %s", exc)

    # 6. Reset all FAILED + PAUSED_AUTH tasks to PENDING + release workers
    from src.domain.connection import SessionLocal
    from sqlalchemy import text
    with SessionLocal() as db:
        reset_count = db.execute(
            text("UPDATE task_queue SET status='PENDING', retries=0 WHERE status IN ('FAILED', 'PAUSED_AUTH')")
        ).rowcount
        # Release workers stuck in BUSY — they will pick up PENDING tasks naturally
        worker_count = db.execute(
            text("UPDATE worker_registry SET status='IDLE', current_task_id=NULL WHERE status='BUSY'")
        ).rowcount
        db.commit()

    logger.info(
        "Cookie hot-swap successful — %d FAILED tasks reset to PENDING, %d workers released",
        reset_count, worker_count,
    )
    return {"ok": True, "reset_tasks": reset_count, "workers_released": worker_count}
