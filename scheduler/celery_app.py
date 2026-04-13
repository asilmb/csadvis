"""
Celery application factory (PV-04).

Broker  : Redis  redis://redis:6379/0
Backend : PostgreSQL via SQLAlchemy  db+postgresql://...

Settings enforced:
  task_expires             = 28800   (8 h — stale results auto-purge)
  worker_prefetch_multiplier = 1     (one task at a time per worker slot)
  task_acks_late           = True    (ack only after the task finishes)
"""

from __future__ import annotations

import os

import structlog
from celery import Celery

logger = structlog.get_logger()

# ─── Connection URLs ──────────────────────────────────────────────────────────

BROKER_URL: str = os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0")

_pg_user = os.getenv("POSTGRES_USER", "cs2user")
_pg_pass = os.getenv("POSTGRES_PASSWORD", "cs2pass")
_pg_host = os.getenv("DB_HOST", "db")
_pg_port = os.getenv("DB_PORT", "5432")
_pg_db   = os.getenv("POSTGRES_DB", "cs2")

RESULT_BACKEND: str = os.getenv(
    "CELERY_RESULT_BACKEND",
    f"db+postgresql://{_pg_user}:{_pg_pass}@{_pg_host}:{_pg_port}/{_pg_db}",
)

# ─── App ─────────────────────────────────────────────────────────────────────

app = Celery("cs2_worker", broker=BROKER_URL, backend=RESULT_BACKEND)

app.conf.update(
    # Reliability
    task_expires=28800,
    worker_prefetch_multiplier=1,
    task_acks_late=True,

    # Serialisation
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],

    # Time
    timezone="UTC",
    enable_utc=True,

    # Autodiscover
    include=["scheduler.tasks", "scraper.runner"],
)
