"""
Scrape session endpoints.

GET  /scrape-sessions          — list all saved sessions
POST /scrape-sessions/{id}/resume — enqueue resume job
DELETE /scrape-sessions/{id}   — delete session

GET  /scrape-sessions/cooldown — active cooldown info
"""

from __future__ import annotations

import asyncio
import logging

from fastapi import APIRouter, HTTPException

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/scrape-sessions", tags=["scrape-sessions"])


@router.get("/cooldown")
def get_cooldown() -> dict:
    from infra.scrape_guard import get_active_cooldown
    until = get_active_cooldown()
    if until:
        return {"active": True, "cooldown_until": until.isoformat(), "cooldown_until_fmt": until.strftime("%H:%M UTC")}
    return {"active": False, "cooldown_until": None, "cooldown_until_fmt": None}


@router.get("/")
def list_sessions() -> list[dict]:
    from infra.scrape_guard import list_sessions
    return list_sessions()


@router.post("/{session_id}/resume")
def resume_session(session_id: int) -> dict:
    from infra.scrape_guard import delete_session, list_sessions, remaining_ids
    sessions = {s["id"]: s for s in list_sessions()}
    if session_id not in sessions:
        raise HTTPException(status_code=404, detail="Session not found")
    sess = sessions[session_id]
    ids = remaining_ids(session_id)
    if not ids:
        delete_session(session_id)
        return {"ok": False, "message": "Нет оставшихся элементов — сессия удалена."}

    try:
        from infra.work_queue import enqueue, is_job_type_active
        job_type = sess["job_type"]
        if is_job_type_active(job_type):
            return {"ok": False, "message": f"{job_type} уже выполняется — дождись завершения."}
        if job_type == "price_poll":
            enqueue({"type": "price_poll", "container_ids": ids, "session_id": session_id})
        else:
            # backfill_history: ids are container names
            enqueue({"type": "backfill_history", "names": ids, "session_id": session_id})
        return {"ok": True, "message": f"Продолжение {job_type} ({len(ids)} элементов) поставлено в очередь."}
    except asyncio.QueueFull:
        return {"ok": False, "message": "Очередь заполнена — попробуй позже."}


@router.delete("/{session_id}")
def remove_session(session_id: int) -> dict:
    from infra.scrape_guard import delete_session
    delete_session(session_id)
    return {"ok": True}
