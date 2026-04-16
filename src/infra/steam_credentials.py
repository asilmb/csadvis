"""Redis-backed Steam credential store.

Credentials are set via the dashboard cookie form and read by all scrapers.
No .env persistence — credentials live in Redis only.
"""
from __future__ import annotations

import logging

from infra.redis_client import get_redis

logger = logging.getLogger(__name__)

_LOGIN_SECURE_KEY = "steam:login_secure"
_SESSION_ID_KEY   = "steam:session_id"


def get_login_secure() -> str:
    """Read steamLoginSecure from Redis. Returns '' if not set or Redis unavailable."""
    try:
        return get_redis().get(_LOGIN_SECURE_KEY) or ""
    except Exception as exc:
        logger.debug("steam_credentials: could not read login_secure — %s", exc)
        return ""


def set_login_secure(value: str) -> None:
    """Persist steamLoginSecure to Redis."""
    get_redis().set(_LOGIN_SECURE_KEY, value.strip())


def get_session_id() -> str:
    """Read sessionid from Redis. Returns '' if not set or Redis unavailable."""
    try:
        return get_redis().get(_SESSION_ID_KEY) or ""
    except Exception as exc:
        logger.debug("steam_credentials: could not read session_id — %s", exc)
        return ""


def set_session_id(value: str) -> None:
    """Persist sessionid to Redis."""
    get_redis().set(_SESSION_ID_KEY, value.strip())
