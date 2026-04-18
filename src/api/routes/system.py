"""
System management endpoints (PV-43).

POST /system/update-cookie  — hot-swap steamLoginSecure, validate
GET  /system/cookie-status  — return current cookie status from DB
GET  /system/queue-status   — return in-process work queue state
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/system", tags=["system"])


class UpdateCookieRequest(BaseModel):
    value: str
    session_note: str = ""
    sessionid: str = ""


class CancelTaskRequest(BaseModel):
    job_type: str | None = None  # None = drain entire queue


class CookieStatusResponse(BaseModel):
    status: str  # VALID | EXPIRED | UNKNOWN


@router.get("/cookie-status", response_model=CookieStatusResponse)
def cookie_status_endpoint() -> CookieStatusResponse:
    from src.domain.connection import SessionLocal
    from src.domain.sql_repositories import get_cookie_status
    with SessionLocal() as db:
        status = get_cookie_status(db)
    return CookieStatusResponse(status=status)


@router.get("/queue-status")
def queue_status_endpoint() -> dict:
    """Return in-process work queue state for the System Status dashboard."""
    try:
        from infra.work_queue import get_worker_state
        return get_worker_state()
    except Exception as exc:
        logger.warning("queue_status: %s", exc)
        return {"busy": False, "current_type": "", "last_job_at": None, "last_error": str(exc), "restarts": 0, "queue_size": 0}


@router.post("/cancel-task")
async def cancel_task_endpoint(req: CancelTaskRequest) -> dict:
    """
    Drain queued jobs matching req.job_type from the in-process work queue.
    Passing job_type=None drains everything.  The currently-running job is
    NOT interrupted (asyncio.Queue provides no preemption).
    """
    try:
        import asyncio as _asyncio
        from infra.work_queue import get_queue
        q = get_queue()
        removed = 0
        kept: list[dict] = []
        while not q.empty():
            try:
                job = q.get_nowait()
                if req.job_type is None or job.get("type") == req.job_type:
                    q.task_done()   # consumed (discarded) — balances the get
                    removed += 1
                else:
                    kept.append(job)
            except _asyncio.QueueEmpty:
                break
        for job in kept:
            try:
                q.put_nowait(job)
            except _asyncio.QueueFull:
                pass
        logger.info("cancel_task: removed=%d job_type=%r", removed, req.job_type)
        return {"ok": True, "removed": removed}
    except Exception as exc:
        logger.warning("cancel_task: %s", exc)
        return {"ok": False, "removed": 0, "error": str(exc)}


@router.post("/update-cookie")
def update_cookie_endpoint(req: UpdateCookieRequest) -> dict:
    value = req.value.strip()
    if not value:
        raise HTTPException(status_code=400, detail="Cookie value cannot be empty")

    logger.info("Cookie update requested — applying new value (masked: ***)")

    from infra.steam_credentials import set_login_secure, set_session_id
    set_login_secure(value)
    if req.sessionid.strip():
        set_session_id(req.sessionid.strip())

    from scrapper.steam_sync import sync_wallet
    result = sync_wallet()

    if not result.ok:
        logger.warning("Cookie validation failed: error_code=%s", result.error_code)
        return {"ok": False, "error": result.error_code or "VALIDATION_FAILED"}

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

    try:
        from infra.redis_client import get_redis as _get_redis
        _get_redis().delete("STEALTH_BLOCK_EXPIRES")
    except Exception as exc:
        logger.warning("Could not clear stealth block: %s", exc)

    logger.info("Cookie hot-swap successful")
    return {"ok": True, "reset_tasks": 0, "workers_released": 0}
