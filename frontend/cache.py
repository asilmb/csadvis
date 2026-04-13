"""Redis-backed flask-caching instance for the CS2 Market Analytics Dash app (PV-10).

Usage
-----
1. Import ``cache`` and decorate functions with ``@cache.memoize(timeout=N)``.
2. Call ``init_cache(app.server)`` once inside ``create_dash_app()`` after the
   Dash app is constructed so the Flask server is available.
"""

import os

from flask_caching import Cache

cache = Cache()

_REDIS_URL = os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0")

_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_REDIS_URL": _REDIS_URL,
    "CACHE_DEFAULT_TIMEOUT": 300,
}


def init_cache(flask_server) -> None:
    """Bind the Redis cache to the Flask server backing the Dash app."""
    cache.init_app(flask_server, config=_CACHE_CONFIG)
