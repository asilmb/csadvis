"""
Tracks the last date the scraper ran so it fires at most once per calendar day.
State is stored in Redis under key cs2:scraper:last_parsed.
"""

from __future__ import annotations

import logging
from datetime import date

from infra.redis_client import get_redis

logger = logging.getLogger(__name__)

_KEY = "cs2:scraper:last_parsed"


def needs_run() -> bool:
    """Return True if the scraper has not run today."""
    try:
        last = get_redis().get(_KEY)
    except Exception as exc:
        logger.warning("scraper_state: redis unavailable (%s) — allowing run", exc)
        return True

    today = date.today().isoformat()
    if last == today:
        logger.info("Scraper: already ran today (%s), skipping.", today)
        return False
    return True


def mark_done() -> None:
    """Record today as the last successful run date."""
    today = date.today().isoformat()
    try:
        get_redis().set(_KEY, today)
    except Exception as exc:
        logger.warning("scraper_state: could not persist to Redis: %s", exc)
    logger.info("Scraper: state saved — last_parsed=%s", today)


def get_state() -> dict:
    """Return the raw scraper state dict (public API)."""
    try:
        last = get_redis().get(_KEY)
        return {"last_parsed": last} if last else {}
    except Exception:
        return {}
