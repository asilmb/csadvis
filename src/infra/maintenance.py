"""
Database Garbage Collector (PV-26).

MaintenanceService.run_all() performs two steps in order:
  1. cleanup_event_log()  — delete EventLog rows older than 7 days, but
       retain ERROR/CRITICAL rows within the last 48 h so that recent
       auth failures and exceptions remain visible for debugging.
  2. vacuum()             — VACUUM on a dedicated connection, outside any
       transaction.

Run manually via `cs2 db cleanup`.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import text

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CleanupResult:
    """Immutable summary returned by run_all()."""

    events_deleted: int


class MaintenanceService:
    """Stateless garbage-collector — safe to instantiate per-call."""

    _EVENT_RETENTION_D: int = 7
    _ERROR_PROTECT_H: int = 48

    # ── public API ─────────────────────────────────────────────────────────────

    def cleanup_event_log(
        self,
        older_than_days: int = _EVENT_RETENTION_D,
        protect_errors_hours: int = _ERROR_PROTECT_H,
    ) -> int:
        """
        Delete EventLog rows older than older_than_days.

        Protection rule: ERROR and CRITICAL rows whose timestamp falls within
        the last protect_errors_hours are always retained, regardless of the
        retention window.  This preserves recent auth-error alerts (PV-17)
        and exceptions for operator review.

        Delete condition (both must be true):
          1. row.timestamp < now − older_than_days
          2. level is not ERROR/CRITICAL  OR  row.timestamp < now − protect_errors_hours

        Returns the number of deleted rows.
        """
        from sqlalchemy import or_

        from src.domain.connection import SessionLocal
        from src.domain.models import EventLog

        now = datetime.now(UTC).replace(tzinfo=None)
        old_cutoff = now - timedelta(days=older_than_days)
        recent_cutoff = now - timedelta(hours=protect_errors_hours)

        with SessionLocal() as db:
            deleted = (
                db.query(EventLog)
                .filter(
                    EventLog.timestamp < old_cutoff,
                    or_(
                        EventLog.level.notin_(["ERROR", "CRITICAL"]),
                        EventLog.timestamp < recent_cutoff,
                    ),
                )
                .delete(synchronize_session=False)
            )
            db.commit()

        logger.info(
            "maintenance: deleted %d event log row(s) older than %dd "
            "(protecting ERROR/CRITICAL within last %dh)",
            deleted,
            older_than_days,
            protect_errors_hours,
        )
        return deleted

    def vacuum(self) -> None:
        """
        Run VACUUM ANALYZE on a dedicated engine connection.

        VACUUM must be issued outside a transaction — PostgreSQL forbids it
        inside an explicit transaction block. Using AUTOCOMMIT isolation level
        ensures SQLAlchemy does not wrap the statement in an implicit transaction.
        """
        from src.domain.connection import engine

        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text("VACUUM ANALYZE"))

        logger.info("maintenance: VACUUM complete")

    def run_all(
        self,
        event_retention_d: int = _EVENT_RETENTION_D,
        protect_errors_h: int = _ERROR_PROTECT_H,
    ) -> CleanupResult:
        """
        Full maintenance cycle: event log cleanup → VACUUM.

        Returns a CleanupResult with the total rows deleted.
        """
        events = self.cleanup_event_log(
            older_than_days=event_retention_d,
            protect_errors_hours=protect_errors_h,
        )
        self.vacuum()
        return CleanupResult(events_deleted=events)
