"""
EventLog writer (PV-17).

Provides a single public function:

    log_event(level, module, message)

Each call opens its own short-lived SessionLocal session, inserts one
EventLog row, commits, and closes.  This guarantees no long-held
connections and eliminates deadlock risk under concurrent workers.

DB errors are swallowed (non-fatal) — the operation is best-effort.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# Column width guards — match EventLog model constraints
_MAX_LEVEL = 20
_MAX_MODULE = 100
_MAX_MESSAGE = 1000


def log_event(level: str, module: str, message: str) -> None:
    """
    Write a single row to the EventLog table.

    Parameters
    ----------
    level:    Severity string — "DEBUG" / "INFO" / "WARNING" / "ERROR" / "CRITICAL".
    module:   Originating module name (e.g. "signal_handler").
    message:  Human-readable description of the event (truncated to 1000 chars).

    Thread-safety: each call opens and closes its own session — safe to call
    from multiple worker threads simultaneously without locking.
    """
    try:
        from database.connection import SessionLocal
        from database.models import EventLog

        with SessionLocal() as db:
            db.add(
                EventLog(
                    level=level.upper()[:_MAX_LEVEL],
                    module=module[:_MAX_MODULE],
                    message=message[:_MAX_MESSAGE],
                )
            )
            db.commit()
    except Exception as exc:
        logger.debug("event_logger: DB write failed (non-fatal): %s", exc)
