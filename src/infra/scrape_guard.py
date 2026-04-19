"""
Scrape guard — cooldown enforcement and session snapshots.

Cooldown
--------
When Steam returns 429, call record_429(triggered_by).
Before any Steam scraping job, call check_cooldown() — raises ScrapeBlocked if active.

Snapshots
---------
create_session(job_type, ids) → session_id
tick_session(session_id)      → increments processed_count by 1
finish_session(session_id)    → deletes the session (job completed)
list_sessions()               → list of dicts for UI
delete_session(session_id)    → manual delete
remaining_ids(session_id)     → list of unprocessed IDs
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta

logger = logging.getLogger(__name__)

_COOLDOWN_HOURS = 6


class ScrapeBlocked(Exception):
    def __init__(self, cooldown_until: datetime):
        self.cooldown_until = cooldown_until
        super().__init__(f"Steam rate-limited until {cooldown_until.strftime('%H:%M')} UTC")


# ── Cooldown ──────────────────────────────────────────────────────────────────

def record_429(triggered_by: str = "") -> datetime:
    """Record a 429 event. Returns cooldown_until."""
    from src.domain.connection import SessionLocal
    from src.domain.models import RateLimitLog
    until = datetime.now(UTC).replace(tzinfo=None) + timedelta(hours=_COOLDOWN_HOURS)
    with SessionLocal() as db:
        db.add(RateLimitLog(triggered_by=triggered_by, cooldown_until=until))
        db.commit()
    logger.warning("scrape_guard: 429 recorded — cooldown until %s UTC", until.strftime("%H:%M"))
    return until


def get_active_cooldown() -> datetime | None:
    """Return cooldown_until if there is an active block, else None."""
    from src.domain.connection import SessionLocal
    from src.domain.models import RateLimitLog
    now = datetime.now(UTC).replace(tzinfo=None)
    with SessionLocal() as db:
        row = (
            db.query(RateLimitLog)
            .filter(RateLimitLog.cooldown_until > now)
            .order_by(RateLimitLog.cooldown_until.desc())
            .first()
        )
        return row.cooldown_until if row else None


def check_cooldown() -> None:
    """Raise ScrapeBlocked if Steam is currently rate-limited."""
    until = get_active_cooldown()
    if until:
        raise ScrapeBlocked(until)


# ── Scrape sessions ───────────────────────────────────────────────────────────

def create_session(job_type: str, ids: list[str]) -> int:
    """Persist a new scrape session. Returns session id."""
    from src.domain.connection import SessionLocal
    from src.domain.models import ScrapeSession
    with SessionLocal() as db:
        sess = ScrapeSession(
            job_type=job_type,
            all_ids=json.dumps(ids),
            total_count=len(ids),
            processed_count=0,
        )
        db.add(sess)
        db.commit()
        db.refresh(sess)
        return sess.id


def tick_session(session_id: int) -> None:
    """Increment processed_count by 1."""
    from src.domain.connection import SessionLocal
    from src.domain.models import ScrapeSession
    with SessionLocal() as db:
        sess = db.get(ScrapeSession, session_id)
        if sess:
            sess.processed_count += 1
            sess.updated_at = datetime.now(UTC).replace(tzinfo=None)
            db.commit()


def finish_session(session_id: int) -> None:
    """Delete session on successful completion."""
    from src.domain.connection import SessionLocal
    from src.domain.models import ScrapeSession
    with SessionLocal() as db:
        sess = db.get(ScrapeSession, session_id)
        if sess:
            db.delete(sess)
            db.commit()


def remaining_ids(session_id: int) -> list[str]:
    """Return unprocessed IDs for resuming."""
    from src.domain.connection import SessionLocal
    from src.domain.models import ScrapeSession
    with SessionLocal() as db:
        sess = db.get(ScrapeSession, session_id)
        if not sess:
            return []
        try:
            all_ids: list[str] = json.loads(sess.all_ids)
        except (ValueError, TypeError):
            logger.error("scrape_guard: session %d has corrupt all_ids — deleting", session_id)
            db.delete(sess)
            db.commit()
            return []
        return all_ids[sess.processed_count:]


def list_sessions() -> list[dict]:
    from src.domain.connection import SessionLocal
    from src.domain.models import ScrapeSession
    with SessionLocal() as db:
        rows = db.query(ScrapeSession).order_by(ScrapeSession.created_at.desc()).all()
        return [
            {
                "id": r.id,
                "job_type": r.job_type,
                "total_count": r.total_count,
                "processed_count": r.processed_count,
                "remaining_count": r.total_count - r.processed_count,
                "created_at": r.created_at.strftime("%d.%m %H:%M"),
                "updated_at": r.updated_at.strftime("%d.%m %H:%M"),
            }
            for r in rows
        ]


def delete_session(session_id: int) -> None:
    from src.domain.connection import SessionLocal
    from src.domain.models import ScrapeSession
    with SessionLocal() as db:
        sess = db.get(ScrapeSession, session_id)
        if sess:
            db.delete(sess)
            db.commit()
