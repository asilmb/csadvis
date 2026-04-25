"""
Scrape guard — cooldown enforcement.

When Steam returns 429, call record_429(triggered_by).
Before any Steam scraping job, call check_cooldown() — raises ScrapeBlocked if active.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

logger = logging.getLogger(__name__)

_COOLDOWN_HOURS = 6


class ScrapeBlocked(Exception):
    def __init__(self, cooldown_until: datetime):
        self.cooldown_until = cooldown_until
        super().__init__(f"Steam rate-limited until {cooldown_until.strftime('%H:%M')} UTC")


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
