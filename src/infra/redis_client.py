"""Shared Redis client factory."""
import os

import redis

_URL = os.getenv("REDIS_URL", os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0"))

# One pool per process — Celery workers are multi-process, so each gets its own pool.
# max_connections=10 is generous for typical single-worker concurrency.
_pool = redis.ConnectionPool.from_url(_URL, decode_responses=True, max_connections=10)


def get_redis() -> redis.Redis:
    """Return a Redis client backed by the shared connection pool."""
    return redis.Redis(connection_pool=_pool)
