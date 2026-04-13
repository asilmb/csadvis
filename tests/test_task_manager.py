"""
Unit tests for database/repositories.py — SqlAlchemyTaskQueueRepository (PV-25).

Uses SQLite in-memory so the full query logic (deduplication, priority ordering,
lifecycle transitions) is exercised against a real engine.

Covers:
  - enqueue(): DTO returned, dedup PENDING, dedup PROCESSING,
               different payload allowed, different type allowed,
               COMPLETED task allows re-enqueue
  - pick_task(): highest priority first, FIFO within same priority,
                 marks PROCESSING, empty queue → None, RETRY is pickable
  - complete(): marks COMPLETED
  - fail():     increments retries, RETRY below max, FAILED at max,
                non-existent id → FAILED
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from database.models import Base
from database.repositories import SqlAlchemyTaskQueueRepository, TaskDTO

# ─── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture()
def db():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


@pytest.fixture()
def repo(db):
    return SqlAlchemyTaskQueueRepository(db)


# ─── enqueue ──────────────────────────────────────────────────────────────────


class TestEnqueue:
    def test_returns_task_dto(self, repo, db):
        dto = repo.enqueue("price_poll", priority=1)
        db.commit()
        assert isinstance(dto, TaskDTO)
        assert dto.type == "price_poll"
        assert dto.priority == 1
        assert dto.status == "PENDING"
        assert dto.retries == 0

    def test_dto_id_is_nonempty_string(self, repo, db):
        dto = repo.enqueue("price_poll", priority=1)
        db.commit()
        assert isinstance(dto.id, str) and len(dto.id) > 0

    def test_payload_stored(self, repo, db):
        payload = {"container_id": "abc-123"}
        dto = repo.enqueue("backfill", priority=3, payload=payload)
        db.commit()
        assert dto.payload == payload

    def test_dedup_pending_returns_none(self, repo, db):
        repo.enqueue("price_poll", priority=1, payload={"x": 1})
        db.commit()
        result = repo.enqueue("price_poll", priority=1, payload={"x": 1})
        db.commit()
        assert result is None

    def test_dedup_processing_returns_none(self, repo, db):
        repo.enqueue("price_poll", priority=1, payload={"x": 1})
        db.commit()
        repo.pick_task()  # → PROCESSING
        db.commit()
        result = repo.enqueue("price_poll", priority=1, payload={"x": 1})
        db.commit()
        assert result is None

    def test_different_payload_allowed(self, repo, db):
        repo.enqueue("price_poll", priority=1, payload={"x": 1})
        db.commit()
        dto = repo.enqueue("price_poll", priority=1, payload={"x": 2})
        db.commit()
        assert dto is not None
        assert dto.payload == {"x": 2}

    def test_different_type_allowed(self, repo, db):
        repo.enqueue("price_poll", priority=1)
        db.commit()
        dto = repo.enqueue("sync_inventory", priority=1)
        db.commit()
        assert dto is not None
        assert dto.type == "sync_inventory"

    def test_completed_allows_reenqueue(self, repo, db):
        dto = repo.enqueue("price_poll", priority=1)
        db.commit()
        repo.pick_task()
        db.commit()
        repo.complete(dto.id)
        db.commit()
        second = repo.enqueue("price_poll", priority=1)
        db.commit()
        assert second is not None

    def test_failed_allows_reenqueue(self, repo, db):
        dto = repo.enqueue("price_poll", priority=1)
        db.commit()
        repo.pick_task()
        db.commit()
        repo.fail(dto.id, max_retries=1)
        db.commit()
        second = repo.enqueue("price_poll", priority=1)
        db.commit()
        assert second is not None

    def test_none_payload_deduplicates(self, repo, db):
        repo.enqueue("housekeeping", priority=2, payload=None)
        db.commit()
        result = repo.enqueue("housekeeping", priority=2, payload=None)
        db.commit()
        assert result is None


# ─── pick_task ────────────────────────────────────────────────────────────────


class TestPickTask:
    def test_empty_queue_returns_none(self, repo, db):
        assert repo.pick_task() is None

    def test_returns_task_dto(self, repo, db):
        repo.enqueue("price_poll", priority=1)
        db.commit()
        dto = repo.pick_task()
        db.commit()
        assert isinstance(dto, TaskDTO)

    def test_marks_processing(self, repo, db):
        repo.enqueue("price_poll", priority=1)
        db.commit()
        dto = repo.pick_task()
        db.commit()
        assert dto.status == "PROCESSING"

    def test_highest_priority_first(self, repo, db):
        repo.enqueue("low_job", priority=3)
        repo.enqueue("high_job", priority=1)
        repo.enqueue("medium_job", priority=2)
        db.commit()

        first = repo.pick_task()
        db.commit()
        assert first.type == "high_job"

        second = repo.pick_task()
        db.commit()
        assert second.type == "medium_job"

        third = repo.pick_task()
        db.commit()
        assert third.type == "low_job"

    def test_same_priority_fifo(self, repo, db):
        # Insert with same priority — FIFO by created_at
        dto_a = repo.enqueue("job_a", priority=2, payload={"seq": 1})
        db.commit()
        dto_b = repo.enqueue("job_b", priority=2, payload={"seq": 2})
        db.commit()

        first = repo.pick_task()
        db.commit()
        assert first.id == dto_a.id

        second = repo.pick_task()
        db.commit()
        assert second.id == dto_b.id

    def test_retry_tasks_are_pickable(self, repo, db):
        dto = repo.enqueue("flaky_job", priority=1)
        db.commit()
        repo.pick_task()
        db.commit()
        repo.fail(dto.id, max_retries=3)  # retries=1 < 3 → RETRY
        db.commit()

        picked = repo.pick_task()
        db.commit()
        assert picked is not None
        assert picked.id == dto.id
        assert picked.status == "PROCESSING"

    def test_processing_tasks_not_picked_again(self, repo, db):
        repo.enqueue("job", priority=1)
        db.commit()
        repo.pick_task()
        db.commit()
        second = repo.pick_task()  # queue now empty (1 task in PROCESSING)
        assert second is None


# ─── complete ─────────────────────────────────────────────────────────────────


class TestComplete:
    def test_marks_completed(self, repo, db):
        dto = repo.enqueue("job", priority=1)
        db.commit()
        repo.pick_task()
        db.commit()
        repo.complete(dto.id)
        db.commit()

        from database.models import TaskQueue
        row = db.query(TaskQueue).filter(TaskQueue.id == dto.id).first()
        assert str(row.status) == "COMPLETED"

    def test_nonexistent_id_is_noop(self, repo, db):
        repo.complete("does-not-exist")  # must not raise
        db.commit()


# ─── fail ─────────────────────────────────────────────────────────────────────


class TestFail:
    def test_increments_retries(self, repo, db):
        dto = repo.enqueue("job", priority=1)
        db.commit()
        repo.pick_task()
        db.commit()
        repo.fail(dto.id, max_retries=3)
        db.commit()

        from database.models import TaskQueue
        row = db.query(TaskQueue).filter(TaskQueue.id == dto.id).first()
        assert row.retries == 1

    def test_below_max_returns_retry(self, repo, db):
        dto = repo.enqueue("job", priority=1)
        db.commit()
        repo.pick_task()
        db.commit()
        status = repo.fail(dto.id, max_retries=3)
        db.commit()
        assert status == "RETRY"

    def test_at_max_returns_failed(self, repo, db):
        dto = repo.enqueue("job", priority=1)
        db.commit()
        repo.pick_task()
        db.commit()
        # Simulate 2 prior retries already recorded
        from database.models import TaskQueue
        row = db.query(TaskQueue).filter(TaskQueue.id == dto.id).first()
        row.retries = 2
        db.commit()

        status = repo.fail(dto.id, max_retries=3)  # retries → 3 == max → FAILED
        db.commit()
        assert status == "FAILED"

    def test_nonexistent_id_returns_failed(self, repo, db):
        status = repo.fail("ghost-id", max_retries=3)
        assert status == "FAILED"

    def test_consecutive_fails_accumulate(self, repo, db):
        dto = repo.enqueue("flaky", priority=1)
        db.commit()
        repo.pick_task()
        db.commit()

        repo.fail(dto.id, max_retries=5)
        db.commit()
        repo.pick_task()
        db.commit()
        repo.fail(dto.id, max_retries=5)
        db.commit()

        from database.models import TaskQueue
        row = db.query(TaskQueue).filter(TaskQueue.id == dto.id).first()
        assert row.retries == 2
        assert str(row.status) == "RETRY"


# ─── TaskQueueService: flush_failed, reset_stuck_workers, get_system_health ──


class TestTaskQueueServiceExtensions:
    """Tests for PV-37 additions to TaskQueueService."""

    @pytest.fixture()
    def _svc_cm(self):
        """Return (svc, SM) with a real in-memory engine."""
        from sqlalchemy.orm import sessionmaker
        from unittest.mock import patch
        from services.task_manager import TaskQueueService

        engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
        Base.metadata.create_all(engine)
        SM = sessionmaker(bind=engine)
        svc = TaskQueueService()
        with patch("database.connection.SessionLocal", SM):
            yield svc, SM
        engine.dispose()

    def test_flush_failed_deletes_only_failed(self, _svc_cm):
        svc, SM = _svc_cm
        with SM() as db:
            from database.repositories import SqlAlchemyTaskQueueRepository
            repo = SqlAlchemyTaskQueueRepository(db)
            t1 = repo.enqueue("sync", priority=1)
            t2 = repo.enqueue("sync2", priority=2)
            db.commit()
            # manually set one to FAILED
            from database.models import TaskQueue, TaskStatus
            db.query(TaskQueue).filter(TaskQueue.id == t1.id).update({"status": TaskStatus.FAILED})
            db.commit()

        count = svc.flush_failed()
        assert count == 1

        with SM() as db:
            from database.models import TaskQueue
            remaining = db.query(TaskQueue).count()
        assert remaining == 1  # t2 (PENDING) is not deleted

    def test_reset_stuck_workers_changes_status(self, _svc_cm):
        svc, SM = _svc_cm
        with SM() as db:
            from database.models import WorkerRegistry
            db.add(WorkerRegistry(name="w1", status="STUCK"))
            db.add(WorkerRegistry(name="w2", status="DEAD"))
            db.add(WorkerRegistry(name="w3", status="IDLE"))
            db.commit()

        count = svc.reset_stuck_workers()
        assert count == 2

        with SM() as db:
            from database.models import WorkerRegistry
            statuses = {w.name: w.status for w in db.query(WorkerRegistry).all()}
        assert statuses["w1"] == "IDLE"
        assert statuses["w2"] == "IDLE"
        assert statuses["w3"] == "IDLE"  # unchanged

    def test_get_system_health_returns_snapshot(self, _svc_cm):
        svc, SM = _svc_cm
        with SM() as db:
            from database.repositories import SqlAlchemyTaskQueueRepository
            repo = SqlAlchemyTaskQueueRepository(db)
            repo.enqueue("backfill", priority=3)
            db.commit()

        from services.task_manager import SystemHealth
        h = svc.get_system_health()
        assert isinstance(h, SystemHealth)
        assert h.pending_count == 1
        assert h.failed_count == 0
        assert h.token_level in ("HIGH", "MED", "LOW", "UNKNOWN")
        assert isinstance(h.timestamp, str)
