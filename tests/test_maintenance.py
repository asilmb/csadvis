"""
Unit tests for PV-26: services/maintenance.py

Covers:
  MaintenanceService.cleanup_task_queue():
    - deletes COMPLETED tasks older than retention window
    - deletes FAILED tasks older than retention window
    - retains PENDING tasks regardless of age
    - retains PROCESSING tasks regardless of age
    - retains COMPLETED/FAILED tasks within the retention window
    - returns correct deleted count

  MaintenanceService.cleanup_event_log():
    - deletes INFO rows older than retention window
    - deletes WARNING rows older than retention window
    - retains rows within the retention window (any level)
    - retains ERROR rows within protect_errors_hours, even if older than retention window
    - retains CRITICAL rows within protect_errors_hours
    - deletes ERROR rows older than both retention window AND protect_errors_hours
    - returns correct deleted count

  MaintenanceService.vacuum():
    - executes without raising (mocked engine.connect)

  MaintenanceService.run_all():
    - calls cleanup_task_queue, cleanup_event_log, vacuum in order
    - returns CleanupResult with correct counts
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, call, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.domain.models import Base, EventLog, TaskQueue, TaskStatus


# ─── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture()
def engine():
    e = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(e)
    yield e
    e.dispose()


@pytest.fixture()
def db(engine):
    with Session(engine) as s:
        yield s


def _now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _ago(hours: float = 0, days: float = 0) -> datetime:
    return _now() - timedelta(hours=hours, days=days)


def _make_cm(engine):
    TestSession = sessionmaker(bind=engine)

    class _CM:
        def __enter__(self):
            self._s = TestSession()
            return self._s

        def __exit__(self, *_):
            try:
                self._s.commit()
            except Exception:
                self._s.rollback()
            finally:
                self._s.close()

    return _CM


def _add_task(db: Session, status: TaskStatus, created_at: datetime) -> TaskQueue:
    row = TaskQueue(type="test_task", priority=2, status=status, created_at=created_at)
    db.add(row)
    db.flush()
    return row


def _add_event(db: Session, level: str, ts: datetime) -> EventLog:
    row = EventLog(level=level, module="test", message="msg", timestamp=ts)
    db.add(row)
    db.flush()
    return row


def _count_tasks(engine) -> int:
    TestSession = sessionmaker(bind=engine)
    with TestSession() as s:
        return s.query(TaskQueue).count()


def _count_events(engine) -> int:
    TestSession = sessionmaker(bind=engine)
    with TestSession() as s:
        return s.query(EventLog).count()


# ─── cleanup_task_queue ───────────────────────────────────────────────────────


class TestCleanupTaskQueue:
    def _run(self, engine, **kwargs) -> int:
        from infra.maintenance import MaintenanceService

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("domain.connection.SessionLocal", _make_cm(engine), raising=False)
            return MaintenanceService().cleanup_task_queue(**kwargs)

    def test_deletes_completed_old_tasks(self, engine, db):
        _add_task(db, TaskStatus.COMPLETED, _ago(hours=30))
        db.commit()
        deleted = self._run(engine, older_than_hours=24)
        assert deleted == 1
        assert _count_tasks(engine) == 0

    def test_deletes_failed_old_tasks(self, engine, db):
        _add_task(db, TaskStatus.FAILED, _ago(hours=48))
        db.commit()
        deleted = self._run(engine, older_than_hours=24)
        assert deleted == 1
        assert _count_tasks(engine) == 0

    def test_retains_pending_regardless_of_age(self, engine, db):
        _add_task(db, TaskStatus.PENDING, _ago(hours=100))
        db.commit()
        deleted = self._run(engine, older_than_hours=24)
        assert deleted == 0
        assert _count_tasks(engine) == 1

    def test_retains_processing_regardless_of_age(self, engine, db):
        _add_task(db, TaskStatus.PROCESSING, _ago(hours=100))
        db.commit()
        deleted = self._run(engine, older_than_hours=24)
        assert deleted == 0
        assert _count_tasks(engine) == 1

    def test_retains_retry_regardless_of_age(self, engine, db):
        _add_task(db, TaskStatus.RETRY, _ago(hours=100))
        db.commit()
        deleted = self._run(engine, older_than_hours=24)
        assert deleted == 0
        assert _count_tasks(engine) == 1

    def test_retains_fresh_completed_task(self, engine, db):
        """COMPLETED task created 1h ago — within 24h retention → not deleted."""
        _add_task(db, TaskStatus.COMPLETED, _ago(hours=1))
        db.commit()
        deleted = self._run(engine, older_than_hours=24)
        assert deleted == 0
        assert _count_tasks(engine) == 1

    def test_mixed_tasks_only_old_terminal_deleted(self, engine, db):
        _add_task(db, TaskStatus.COMPLETED, _ago(hours=30))   # → deleted
        _add_task(db, TaskStatus.FAILED, _ago(hours=36))       # → deleted
        _add_task(db, TaskStatus.COMPLETED, _ago(hours=2))     # → kept (fresh)
        _add_task(db, TaskStatus.PENDING, _ago(hours=100))     # → kept (live)
        db.commit()
        deleted = self._run(engine, older_than_hours=24)
        assert deleted == 2
        assert _count_tasks(engine) == 2

    def test_returns_zero_when_nothing_to_delete(self, engine, db):
        db.commit()
        deleted = self._run(engine, older_than_hours=24)
        assert deleted == 0


# ─── cleanup_event_log ────────────────────────────────────────────────────────


class TestCleanupEventLog:
    def _run(self, engine, **kwargs) -> int:
        from infra.maintenance import MaintenanceService

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("domain.connection.SessionLocal", _make_cm(engine), raising=False)
            return MaintenanceService().cleanup_event_log(**kwargs)

    def test_deletes_old_info_rows(self, engine, db):
        _add_event(db, "INFO", _ago(days=10))
        db.commit()
        deleted = self._run(engine, older_than_days=7, protect_errors_hours=48)
        assert deleted == 1
        assert _count_events(engine) == 0

    def test_deletes_old_warning_rows(self, engine, db):
        _add_event(db, "WARNING", _ago(days=8))
        db.commit()
        deleted = self._run(engine, older_than_days=7, protect_errors_hours=48)
        assert deleted == 1

    def test_retains_rows_within_retention_window(self, engine, db):
        _add_event(db, "INFO", _ago(days=3))
        db.commit()
        deleted = self._run(engine, older_than_days=7, protect_errors_hours=48)
        assert deleted == 0
        assert _count_events(engine) == 1

    def test_retains_recent_error_even_if_old_enough(self, engine, db):
        """
        ERROR row is 10 days old (beyond retention) but within 48h protect window.
        → Must NOT be deleted.
        Wait — 10 days old CANNOT be within 48h. This case is impossible with
        default params. Test with older_than_days=1 to create the conflict.
        """
        # Row is 2 days old: older than 1-day retention, but within 48h protect window.
        _add_event(db, "ERROR", _ago(hours=30))  # 30h old
        db.commit()
        # older_than_days=1 (24h), protect_errors_hours=48 → protect window wins
        deleted = self._run(engine, older_than_days=1, protect_errors_hours=48)
        assert deleted == 0
        assert _count_events(engine) == 1

    def test_retains_recent_critical_in_protect_window(self, engine, db):
        _add_event(db, "CRITICAL", _ago(hours=12))
        db.commit()
        deleted = self._run(engine, older_than_days=1, protect_errors_hours=48)
        assert deleted == 0

    def test_deletes_old_error_beyond_protect_window(self, engine, db):
        """ERROR row older than both retention AND protect window → deleted."""
        _add_event(db, "ERROR", _ago(days=10))  # 10 days old, well beyond 48h
        db.commit()
        deleted = self._run(engine, older_than_days=7, protect_errors_hours=48)
        assert deleted == 1

    def test_deletes_old_critical_beyond_protect_window(self, engine, db):
        _add_event(db, "CRITICAL", _ago(days=15))
        db.commit()
        deleted = self._run(engine, older_than_days=7, protect_errors_hours=48)
        assert deleted == 1

    def test_mixed_events_correct_selection(self, engine, db):
        """
        Setup:
          - INFO  10d old  → deleted (old, non-critical)
          - WARNING 8d old → deleted (old, non-critical)
          - ERROR  30h old → kept   (non-critical-level, within 48h protect)
               with older_than_days=1: 30h > 24h → qualifies for deletion by age,
               but 30h < 48h and ERROR → protected
          - INFO   6h old  → kept   (within 7d retention)
          - ERROR 10d old  → deleted (old AND beyond protect window)
        """
        _add_event(db, "INFO", _ago(days=10))     # deleted
        _add_event(db, "WARNING", _ago(days=8))   # deleted
        _add_event(db, "ERROR", _ago(hours=30))   # kept  (protect window, use 1d retention)
        _add_event(db, "INFO", _ago(hours=6))     # kept  (fresh)
        _add_event(db, "ERROR", _ago(days=10))    # deleted
        db.commit()
        # Use older_than_days=1 so the 30h-old ERROR falls in the "age-qualifies" zone
        deleted = self._run(engine, older_than_days=1, protect_errors_hours=48)
        # INFO(10d)=yes, WARNING(8d)=yes, ERROR(10d)=yes → 3 deleted
        # ERROR(30h) and INFO(6h) → both within 1d? No: 30h > 24h, 6h < 24h
        # ERROR(30h): age qualifies (30h > 24h), but level=ERROR and 30h < 48h → protected
        # INFO(6h): age does NOT qualify (6h < 24h) → not in query → kept
        assert deleted == 3
        assert _count_events(engine) == 2

    def test_returns_zero_when_nothing_to_delete(self, engine, db):
        db.commit()
        deleted = self._run(engine, older_than_days=7, protect_errors_hours=48)
        assert deleted == 0


# ─── vacuum ───────────────────────────────────────────────────────────────────


class TestVacuum:
    def _mock_conn(self):
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        # vacuum() calls engine.connect().execution_options(...) — make the chain
        # return the same mock so __enter__ / execute / commit land on mock_conn.
        mock_conn.execution_options.return_value = mock_conn
        return mock_conn

    def test_vacuum_executes_without_error(self):
        """VACUUM statement is issued via engine.connect()."""
        from infra.maintenance import MaintenanceService

        mock_conn = self._mock_conn()

        with patch("domain.connection.engine") as mock_engine:
            mock_engine.connect.return_value = mock_conn
            MaintenanceService().vacuum()

        mock_conn.execute.assert_called_once()
        sql_text = str(mock_conn.execute.call_args.args[0])
        assert "VACUUM" in sql_text.upper()

    def test_vacuum_uses_autocommit_isolation(self):
        """vacuum() must request AUTOCOMMIT so PostgreSQL never wraps it in a transaction."""
        from infra.maintenance import MaintenanceService

        mock_conn = self._mock_conn()

        with patch("domain.connection.engine") as mock_engine:
            mock_engine.connect.return_value = mock_conn
            MaintenanceService().vacuum()

        mock_conn.execution_options.assert_called_once_with(isolation_level="AUTOCOMMIT")

    def test_vacuum_uses_engine_connect_not_session(self):
        """VACUUM must use engine.connect(), not SessionLocal — no transaction wrapper."""
        from infra.maintenance import MaintenanceService

        mock_conn = self._mock_conn()

        with patch("domain.connection.engine") as mock_engine, \
             patch("domain.connection.SessionLocal") as mock_session:
            mock_engine.connect.return_value = mock_conn
            MaintenanceService().vacuum()

        mock_engine.connect.assert_called_once()
        mock_session.assert_not_called()


# ─── run_all ──────────────────────────────────────────────────────────────────


class TestRunAll:
    def test_calls_all_three_steps_in_order(self):
        from infra.maintenance import MaintenanceService

        calls: list[str] = []
        svc = MaintenanceService()

        def fake_tasks(**kw):
            calls.append("tasks")
            return 5

        def fake_events(**kw):
            calls.append("events")
            return 12

        def fake_vacuum():
            calls.append("vacuum")

        svc.cleanup_task_queue = fake_tasks
        svc.cleanup_event_log = fake_events
        svc.vacuum = fake_vacuum

        result = svc.run_all()

        assert calls == ["tasks", "events", "vacuum"]
        assert result.tasks_deleted == 5
        assert result.events_deleted == 12

    def test_returns_cleanup_result(self):
        from infra.maintenance import CleanupResult, MaintenanceService

        svc = MaintenanceService()
        svc.cleanup_task_queue = lambda **kw: 3
        svc.cleanup_event_log = lambda **kw: 7
        svc.vacuum = lambda: None

        result = svc.run_all()
        assert isinstance(result, CleanupResult)
        assert result.tasks_deleted == 3
        assert result.events_deleted == 7

    def test_default_params_forwarded(self):
        """run_all() passes its args through to the individual methods."""
        from infra.maintenance import MaintenanceService

        received: dict = {}

        def fake_tasks(older_than_hours):
            received["task_h"] = older_than_hours
            return 0

        def fake_events(older_than_days, protect_errors_hours):
            received["event_d"] = older_than_days
            received["protect_h"] = protect_errors_hours
            return 0

        svc = MaintenanceService()
        svc.cleanup_task_queue = fake_tasks
        svc.cleanup_event_log = fake_events
        svc.vacuum = lambda: None

        svc.run_all(task_retention_h=12, event_retention_d=3, protect_errors_h=24)

        assert received["task_h"] == 12
        assert received["event_d"] == 3
        assert received["protect_h"] == 24


