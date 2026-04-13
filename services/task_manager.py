"""
TaskQueueService — application-layer facade over SqlAlchemyTaskQueueRepository.

Owns the Session lifecycle so callers never touch SQLAlchemy directly.
Returns TaskDTO objects (immutable dataclasses) — no ORM models leak out.

Usage:
    from services.task_manager import TaskQueueService

    svc = TaskQueueService()
    task = svc.enqueue("price_poll", priority=1, payload={"container_id": "abc"})
    task = svc.pick_task()          # → TaskDTO | None
    svc.complete(task.id)
    svc.fail(task.id, max_retries=3)
    n = svc.reclaim_stuck_tasks()   # → int (reclaimed count)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

# Re-export TaskDTO so callers import from a single place.
from database.repositories import SqlAlchemyTaskQueueRepository, TaskDTO


@dataclass
class SystemHealth:
    """Snapshot of system health for the UI Status tab."""

    cookie_set: bool
    tokens: float | None               # current token bucket level (0–15); None if unavailable
    token_level: str                    # "HIGH" / "MED" / "LOW"
    circuit_open: bool                  # True when circuit breaker is active
    active_workers: int
    workers: list[dict] = field(default_factory=list)    # WorkerRegistry rows as dicts
    recent_tasks: list[dict] = field(default_factory=list) # last 15 tasks (any status)
    failed_count: int = 0
    pending_count: int = 0
    timestamp: str = ""

__all__ = [
    "TASK_TTL", "WORKER_STUCK_THRESHOLD_S", "WATCHDOG_MAX_RETRIES",
    "TaskDTO", "TaskQueueService", "SystemHealth",
]

# ─── Watchdog constants ───────────────────────────────────────────────────────

#: Seconds of heartbeat silence before a worker is considered stuck.
WORKER_STUCK_THRESHOLD_S: int = 90

#: Maximum times watchdog may reclaim the same task before permanently failing it.
WATCHDOG_MAX_RETRIES: int = 3

#: Maximum allowed processing time (seconds) per task type before watchdog reclaims.
#: If a task type is not listed here the stuck-worker threshold (90s) is used.
TASK_TTL: dict[str, int] = {
    "update_balance": 60,
    "sync_inventory": 300,
    "price_poll": 600,               # 39 containers × 4s delay + overhead ≈ 10 min
    "sync_transactions": 300,        # 10 pages × ~5s/page + overhead
    "backfill_history": 2700,        # 45 min
    "market_validation": 120,        # 3 items × HIGH priority ≤ 2 min
    "db_maintenance": 300,           # VACUUM on large DB can take up to 5 min
    "run_market_sync": 600,          # 10 min max for Steam Market search pages
    "run_inventory_sync": 120,       # inventory fetch + reconcile
}


class TaskQueueService:
    """
    Stateless service — safe to instantiate per-call or as a singleton.
    Each public method opens its own Session and commits on success.
    """

    def enqueue(
        self,
        task_type: str,
        priority: int,
        payload: dict | None = None,
    ) -> TaskDTO | None:
        """
        Enqueue a task.  Returns None when an identical active task already exists.

        Parameters
        ----------
        task_type:  Logical task name, e.g. "price_poll", "backfill_history".
        priority:   1=HIGH, 2=MEDIUM, 3=LOW.
        payload:    Arbitrary JSON-serialisable dict of task parameters.
        """
        from database.connection import SessionLocal

        with SessionLocal() as db:
            repo = SqlAlchemyTaskQueueRepository(db)
            dto = repo.enqueue(task_type, priority, payload)
            db.commit()
            return dto

    def pick_task(self) -> TaskDTO | None:
        """
        Claim and return the next eligible task (PENDING or RETRY, highest priority).
        Returns None when the queue is empty.  Claimed task moves to PROCESSING.
        """
        from database.connection import SessionLocal

        with SessionLocal() as db:
            repo = SqlAlchemyTaskQueueRepository(db)
            dto = repo.pick_task()
            db.commit()
            return dto

    def complete(self, task_id: str) -> None:
        """Mark a PROCESSING task as COMPLETED."""
        from database.connection import SessionLocal

        with SessionLocal() as db:
            repo = SqlAlchemyTaskQueueRepository(db)
            repo.complete(task_id)
            db.commit()

    def fail(self, task_id: str, max_retries: int = 3, error_msg: str | None = None) -> str:
        """
        Record a failure.  Returns the new status string ("RETRY" or "FAILED").

        When retries < max_retries the task is re-queued (RETRY).
        At max_retries it is permanently marked FAILED.
        """
        from database.connection import SessionLocal

        with SessionLocal() as db:
            repo = SqlAlchemyTaskQueueRepository(db)
            new_status = repo.fail(task_id, max_retries, error_msg=error_msg)
            db.commit()
            return new_status

    def requeue_pending(self, task_id: str) -> None:
        """Reset task to PENDING without incrementing retry counter (network backoff path)."""
        from database.connection import SessionLocal

        with SessionLocal() as db:
            repo = SqlAlchemyTaskQueueRepository(db)
            repo.requeue_pending(task_id)
            db.commit()

    def create_processing(
        self, task_type: str, payload: dict | None = None, priority: int = 3
    ) -> TaskDTO:
        """
        Create a task directly in PROCESSING state for subsystems that own their
        own execution (e.g. scraper).  Returns the created TaskDTO.
        """
        from database.connection import SessionLocal

        with SessionLocal() as db:
            repo = SqlAlchemyTaskQueueRepository(db)
            dto = repo.create_processing(task_type, payload, priority)
            db.commit()
            return dto

    def update_task_progress(self, task_id: str, progress: dict) -> None:
        """Merge progress dict into task payload for live UI visibility (best-effort)."""
        try:
            from database.connection import SessionLocal
            with SessionLocal() as db:
                repo = SqlAlchemyTaskQueueRepository(db)
                repo.update_task_progress(task_id, progress)
                db.commit()
        except Exception:
            pass  # Non-fatal — progress display is best-effort

    def reclaim_stuck_tasks(self) -> int:
        """
        Watchdog: reclaim PROCESSING tasks that are no longer making progress.

        Two reclaim paths:

        Path 1 — Stale-heartbeat workers:
            Worker.last_heartbeat > WORKER_STUCK_THRESHOLD_S AND task TTL elapsed.
            Worker is marked STUCK, current_task_id cleared.

        Path 2 — Orphaned tasks:
            PROCESSING task whose task_id is not referenced by any WorkerRegistry row.
            Happens when a worker thread dies without cleaning up.  TTL guard still
            applies so short-lived legitimate tasks are not touched.

        In both paths the task is reset to PENDING (retries incremented) UNLESS
        retries >= WATCHDOG_MAX_RETRIES, in which case it is permanently FAILED to
        prevent an infinite reclaim→fail loop.

        Returns the number of tasks reclaimed (PENDING or FAILED).
        """
        from database.connection import SessionLocal
        from database.models import TaskQueue, TaskStatus, WorkerRegistry

        now = datetime.now(UTC).replace(tzinfo=None)
        stuck_cutoff = now - timedelta(seconds=WORKER_STUCK_THRESHOLD_S)

        with SessionLocal() as db:
            reclaimed = 0

            # ── Path 1: workers with stale heartbeat ──────────────────────────
            stuck_workers = (
                db.query(WorkerRegistry)
                .filter(
                    WorkerRegistry.last_heartbeat < stuck_cutoff,
                    WorkerRegistry.current_task_id.isnot(None),
                )
                .all()
            )

            for worker in stuck_workers:
                task = (
                    db.query(TaskQueue)
                    .filter(
                        TaskQueue.id == worker.current_task_id,
                        TaskQueue.status == TaskStatus.PROCESSING,
                    )
                    .first()
                )
                if task is None:
                    # Already completed/failed via another path — clear the worker lock.
                    worker.status = "STUCK"
                    worker.current_task_id = None
                    continue

                ttl = TASK_TTL.get(str(task.type), WORKER_STUCK_THRESHOLD_S)
                if (now - task.created_at).total_seconds() < ttl:
                    continue  # TTL not elapsed — too early to reclaim

                worker.status = "STUCK"
                worker.current_task_id = None
                reclaimed += self._reclaim_task(task, now)

            # ── Path 2: orphaned PROCESSING tasks (no worker owns them) ───────
            owned_ids = {
                w.current_task_id
                for w in db.query(WorkerRegistry).all()
                if w.current_task_id is not None
            }
            for task in db.query(TaskQueue).filter(TaskQueue.status == TaskStatus.PROCESSING).all():
                if task.id in owned_ids:
                    continue  # actively owned
                ttl = TASK_TTL.get(str(task.type), WORKER_STUCK_THRESHOLD_S)
                if (now - task.created_at).total_seconds() < ttl:
                    continue  # TTL not elapsed — may be a brand-new task

                reclaimed += self._reclaim_task(task, now)

            db.commit()
            return reclaimed

    @staticmethod
    def _reclaim_task(task, now: datetime) -> int:
        """
        Apply watchdog retry logic to a single TaskQueue ORM row (already loaded).
        Mutates in-place; caller must commit.  Returns 1 (always reclaimed).
        """
        from database.models import TaskStatus

        new_retries = (task.retries or 0) + 1
        task.retries = new_retries
        task.updated_at = now
        if new_retries >= WATCHDOG_MAX_RETRIES:
            task.status = TaskStatus.FAILED
            task.error_message = (
                f"watchdog: permanently failed after {new_retries} reclaim attempts"
            )
        else:
            task.status = TaskStatus.PENDING
        return 1

    def pause_auth(self, task_id: str) -> None:
        """Mark a PROCESSING task as PAUSED_AUTH (auth-loop detected)."""
        from database.connection import SessionLocal

        with SessionLocal() as db:
            repo = SqlAlchemyTaskQueueRepository(db)
            repo.pause_auth(task_id)
            db.commit()

    def has_paused_auth_tasks(self) -> bool:
        """Return True if any task is currently PAUSED_AUTH."""
        from database.connection import SessionLocal

        with SessionLocal() as db:
            repo = SqlAlchemyTaskQueueRepository(db)
            return repo.has_paused_auth_tasks()

    def flush_failed(self) -> int:
        """Delete all FAILED tasks from the queue. Returns count deleted."""
        from database.connection import SessionLocal
        from database.models import TaskQueue, TaskStatus

        with SessionLocal() as db:
            count = (
                db.query(TaskQueue)
                .filter(TaskQueue.status == TaskStatus.FAILED)
                .delete(synchronize_session=False)
            )
            db.commit()
            return count

    def reset_stuck_workers(self) -> int:
        """
        Force all workers with STUCK/DEAD status back to IDLE and clear their task lock.
        Returns count of workers reset.
        """
        from database.connection import SessionLocal
        from database.models import WorkerRegistry

        with SessionLocal() as db:
            workers = (
                db.query(WorkerRegistry)
                .filter(WorkerRegistry.status.in_(["STUCK", "DEAD"]))
                .all()
            )
            for w in workers:
                w.status = "IDLE"
                w.current_task_id = None
            db.commit()
            return len(workers)

    def get_system_health(self) -> "SystemHealth":
        """Return a health snapshot for the System Status dashboard tab."""
        from database.connection import SessionLocal
        from database.models import TaskQueue, TaskStatus, WorkerRegistry

        now = datetime.now(UTC).replace(tzinfo=None)

        worker_rows: list[dict] = []
        processing_rows: list[dict] = []
        failed_count = 0
        pending_count = 0
        active_workers = 0

        try:
            with SessionLocal() as db:
                workers = db.query(WorkerRegistry).all()
                worker_rows = [
                    {
                        "name": w.name,
                        "status": w.status,
                        "heartbeat_age_s": (
                            int((now - w.last_heartbeat).total_seconds())
                            if w.last_heartbeat else None
                        ),
                        "current_task_id": (w.current_task_id or "")[:8] if w.current_task_id else "—",
                    }
                    for w in workers
                ]

                try:
                    from sqlalchemy import case, func as _func
                    recent = (
                        db.query(TaskQueue)
                        .order_by(
                            # PROCESSING rows first, then all others by created_at DESC
                            case(
                                (TaskQueue.status == TaskStatus.PROCESSING, 0),
                                else_=1,
                            ).asc(),
                            TaskQueue.created_at.desc(),
                        )
                        .limit(15)
                        .all()
                    )
                    processing_rows = [
                        {
                            "id": t.id[:8],
                            "type": t.type,
                            "priority": t.priority,
                            "status": str(t.status),
                            "age_s": int((now - t.created_at).total_seconds()) if t.created_at else 0,
                            "completed_at": t.completed_at.strftime("%H:%M:%S") if t.completed_at else None,
                            "error_message": t.error_message or "",
                            "progress": (t.payload or {}).get("_progress") if t.payload else None,
                        }
                        for t in recent
                    ]
                except Exception:
                    processing_rows = []  # migration not yet applied — columns missing

                failed_count = (
                    db.query(TaskQueue).filter(TaskQueue.status == TaskStatus.FAILED).count()
                )
                pending_count = (
                    db.query(TaskQueue).filter(TaskQueue.status == TaskStatus.PENDING).count()
                )
                active_workers = sum(1 for w in workers if w.status in ("IDLE", "BUSY"))
        except Exception:
            pass  # DB unavailable — return zeroed health snapshot

        from config import settings

        return SystemHealth(
            cookie_set=bool(settings.steam_login_secure),
            tokens=None,
            token_level="N/A",
            circuit_open=False,
            active_workers=active_workers,
            workers=worker_rows,
            recent_tasks=processing_rows,
            failed_count=failed_count,
            pending_count=pending_count,
            timestamp=now.strftime("%H:%M:%S"),
        )
