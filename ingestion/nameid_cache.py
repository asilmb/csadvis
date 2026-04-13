"""
item_nameid cache — persists Steam Market listing IDs to avoid repeated HTML fetches.

item_nameid is a stable integer that identifies a specific item on the Steam Community
Market. It is required by the itemordershistogram API endpoint.

Storage: Redis Hash cs2:nameid:cache {market_hash_name: item_nameid}.
"""

from __future__ import annotations

import logging

from infra.redis_client import get_redis

logger = logging.getLogger(__name__)

_KEY = "cs2:nameid:cache"


def load_nameid_cache() -> dict[str, int]:
    """Load the nameid cache from Redis. Returns {} on failure."""
    try:
        data = get_redis().hgetall(_KEY)
        return {k: int(v) for k, v in data.items() if v is not None}
    except Exception as exc:
        logger.warning("nameid_cache: load failed (%s) — starting fresh", exc)
        return {}


def save_nameid_cache(cache: dict[str, int]) -> None:
    """Persist the nameid cache to Redis atomically (delete + hmset in pipeline)."""
    if not cache:
        return
    try:
        r = get_redis()
        pipe = r.pipeline()
        pipe.delete(_KEY)
        pipe.hset(_KEY, mapping={k: str(v) for k, v in cache.items()})
        pipe.execute()
        logger.debug("nameid_cache: saved %d entries", len(cache))
    except Exception as exc:
        logger.warning("nameid_cache: save failed (%s)", exc)
