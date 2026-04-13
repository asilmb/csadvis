"""
Database Garbage Collector (PV-26).

MaintenanceService.run_all() performs three steps in order:
  1. cleanup_task_queue() — delete COMPLETED/FAILED tasks older than 24 h.
  2. cleanup_event_log()  — delete EventLog rows older than 7 days, but
       retain ERROR/CRITICAL rows within the last 48 h so that recent
       auth failures and exceptions remain visible for debugging.
  3. vacuum()             — VACUUM on a dedicated connection, outside any
       transaction (SQLite forbids VACUUM inside a transaction).

Designed to run periodically via a "db_maintenance" TaskQueue entry or
manually via `cs2 db cleanup`.
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

    tasks_deleted: int
    events_deleted: int


class MaintenanceService:
    """Stateless garbage-collector — safe to instantiate per-call."""

    # Defaults used by run_all(); individual methods accept override args for testing.
    _TASK_RETENTION_H: int = 24
    _EVENT_RETENTION_D: int = 7
    _ERROR_PROTECT_H: int = 48

    # ── public API ─────────────────────────────────────────────────────────────

    def cleanup_task_queue(self, older_than_hours: int = _TASK_RETENTION_H) -> int:
        """
        Delete COMPLETED and FAILED tasks created more than older_than_hours ago.

        PENDING / PROCESSING / RETRY tasks are never touched — they are live
        work that must not be lost.

        Returns the number of deleted rows.
        """
        from database.connection import SessionLocal
        from database.models import TaskQueue, TaskStatus

        cutoff = datetime.now(UTC).replace(tzinfo=None) - timedelta(hours=older_than_hours)

        with SessionLocal() as db:
            deleted = (
                db.query(TaskQueue)
                .filter(
                    TaskQueue.status.in_([TaskStatus.COMPLETED, TaskStatus.FAILED]),
                    TaskQueue.created_at < cutoff,
                )
                .delete(synchronize_session=False)
            )
            db.commit()

        logger.info(
            "maintenance: deleted %d terminal task(s) older than %dh",
            deleted,
            older_than_hours,
        )
        return deleted

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

        from database.connection import SessionLocal
        from database.models import EventLog

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
        from database.connection import engine

        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text("VACUUM ANALYZE"))

        logger.info("maintenance: VACUUM complete")

    def run_all(
        self,
        task_retention_h: int = _TASK_RETENTION_H,
        event_retention_d: int = _EVENT_RETENTION_D,
        protect_errors_h: int = _ERROR_PROTECT_H,
    ) -> CleanupResult:
        """
        Full maintenance cycle: task cleanup → event log cleanup → VACUUM.

        Parameters are passed through to the individual methods so callers
        (tests, CLI) can override the defaults without subclassing.

        Returns a CleanupResult with the total rows deleted.
        """
        tasks = self.cleanup_task_queue(older_than_hours=task_retention_h)
        events = self.cleanup_event_log(
            older_than_days=event_retention_d,
            protect_errors_hours=protect_errors_h,
        )
        self.vacuum()
        return CleanupResult(tasks_deleted=tasks, events_deleted=events)
