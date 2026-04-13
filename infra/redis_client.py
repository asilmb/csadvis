"""Shared Redis client factory."""
import os

import redis

_URL = os.getenv("REDIS_URL", os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0"))


def get_redis() -> redis.Redis:
    """Return a short-lived Redis client. Call once per operation."""
    return redis.from_url(_URL, decode_responses=True)
