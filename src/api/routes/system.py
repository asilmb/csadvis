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

        from infra.work_queue import _queue_shadow, enqueue, get_queue
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
        _queue_shadow.clear()
        for job in kept:
            try:
                enqueue(job)
            except _asyncio.QueueFull:
                pass
        logger.info("cancel_task: removed=%d job_type=%r", removed, req.job_type)
        return {"ok": True, "removed": removed}
    except Exception as exc:
        logger.warning("cancel_task: %s", exc)
        return {"ok": False, "removed": 0, "error": str(exc)}


@router.get("/last-ping")
def last_ping_endpoint() -> dict:
    """Return the last ping-steam result stored in Redis."""
    import json

    from infra.redis_client import get_redis
    raw = get_redis().get("cs2:system:last_ping")
    if not raw:
        return {"status": None, "pinged_at": None}
    try:
        return json.loads(raw)
    except Exception:
        return {"status": None, "pinged_at": None}


@router.post("/ping-steam")
async def ping_steam_endpoint() -> dict:
    """
    Test Steam connectivity: validate token + detect rate-limit block.
    Persists result to Redis (cs2:system:last_ping) for UI display.
    """
    import json
    import time
    from datetime import UTC, datetime

    from infra.redis_client import get_redis
    from infra.steam_credentials import auth_credentials_exist

    redis = get_redis()
    now = datetime.now(UTC).strftime("%d.%m.%Y %H:%M")

    def _save(result: dict) -> dict:
        redis.set("cs2:system:last_ping", json.dumps(result), ex=86400)
        return result

    if not auth_credentials_exist():
        return _save({"status": "no_credentials", "pinged_at": now})

    # Check block before making any request — avoids consuming a request slot
    block_raw = redis.get("STEALTH_BLOCK_EXPIRES")
    if block_raw:
        ttl = redis.ttl("STEALTH_BLOCK_EXPIRES")
        remaining_s = max(0, ttl) if ttl > 0 else 0
        blocked_until = (
            datetime.fromtimestamp(time.time() + remaining_s, tz=UTC).strftime("%H:%M UTC")
            if remaining_s > 0 else "скоро"
        )
        return _save({"status": "blocked", "pinged_at": now, "blocked_until": blocked_until, "remaining_s": remaining_s})

    try:
        from scrapper.steam.client import SteamMarketClient
        async with SteamMarketClient() as client:
            data = await client.fetch_price_overview("CS20 Case")

        block_raw2 = redis.get("STEALTH_BLOCK_EXPIRES")
        if block_raw2 or not data:
            ttl = redis.ttl("STEALTH_BLOCK_EXPIRES")
            remaining_s = max(0, ttl) if ttl > 0 else 0
            blocked_until = (
                datetime.fromtimestamp(time.time() + remaining_s, tz=UTC).strftime("%H:%M UTC")
                if remaining_s > 0 else "неизвестно"
            )
            return _save({"status": "blocked", "pinged_at": now, "blocked_until": blocked_until, "remaining_s": remaining_s})
        return _save({"status": "ok", "pinged_at": now})

    except RuntimeError as exc:
        msg = str(exc)
        if "exist" in msg.lower() or "credential" in msg.lower():
            return _save({"status": "no_credentials", "pinged_at": now})
        return _save({"status": "error", "pinged_at": now, "detail": msg[:120]})
    except Exception as exc:
        return _save({"status": "error", "pinged_at": now, "detail": str(exc)[:120]})


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
            from datetime import UTC, datetime

            from src.domain.connection import SessionLocal as _SL
            from src.domain.models import SystemSettings
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
