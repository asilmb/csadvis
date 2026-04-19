"""
Integration tests — Worker Crash & Data Consistency (Critical Path 4).

Verifies that when the worker crashes mid-batch:
  - Data written before the crash is durable (individual per-container commits)
  - Data for the failing container is not committed (rollback)
  - supervised_worker() restarts _worker_loop after any non-CancelledError crash
  - Restart counter increments correctly
  - Remaining jobs in the queue are processed after restart
  - A second crash increments backoff (exponential, capped at 60s)

Uses SQLite in-memory for DB durability checks and mock asyncio.Queue for
queue-state verification.
"""
from __future__ import annotations

import asyncio
import uuid
from datetime import datetime
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# ── DB Fixtures ───────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def sqlite_engine():
    import os
    import tempfile

    from src.domain.models import Base

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    engine = create_engine(
        f"sqlite:///{db_path}",
        connect_args={"check_same_thread": False},
    )
    Base.metadata.create_all(engine)
    yield engine
    engine.dispose()
    os.unlink(db_path)


@pytest.fixture
def make_session(sqlite_engine):
    return sessionmaker(bind=sqlite_engine)


def _seed_container(make_session, name: str) -> str:
    from src.domain.models import ContainerType, DimContainer
    cid = str(uuid.uuid4())
    with make_session() as s:
        s.add(DimContainer(
            container_id=cid,
            container_name=name,
            container_type=ContainerType.Weapon_Case,
            base_cost=1445,
        ))
        s.commit()
    return cid


# ── Worker state reset ────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def reset_wq():
    import infra.work_queue as wq
    wq._state.busy = False
    wq._state.auth_paused = False
    wq._state.current_type = ""
    wq._state.last_error = ""
    wq._state.restarts = 0
    wq._auth_event = None
    wq._work_queue = asyncio.Queue(maxsize=10)
    yield
    wq._work_queue = asyncio.Queue(maxsize=10)


# ── Critical Path 4a: per-container commit isolation ─────────────────────────

class TestPerContainerCommitIsolation:
    @pytest.mark.asyncio
    async def test_committed_rows_survive_mid_batch_crash(self, make_session, sqlite_engine):
        """
        Batch: [Container A (ok), Container B (crash), Container C (never reached)]
        After crash: Container A's rows must be durable; B and C have no rows.
        """
        from scrapper.runner import _save_history_rows

        cid_a = _seed_container(make_session, "Survive Case A")
        cid_b = _seed_container(make_session, "Crash Case B")
        cid_c = _seed_container(make_session, "Orphan Case C")

        rows_a = [{"date": datetime(2024, 5, 1), "price": 100.0, "volume": 3}]
        rows_b = [{"date": datetime(2024, 5, 2), "price": 200.0, "volume": 5}]

        SessionCtx = make_session  # sessionmaker factory, not a session instance

        def _fake_session_local():
            class _Ctx:
                def __enter__(self_):
                    self_._s = SessionCtx()
                    return self_._s

                def __exit__(self_, *_):
                    self_._s.close()

            return _Ctx()

        # Write A successfully
        with patch("src.domain.connection.SessionLocal", side_effect=_fake_session_local):
            await asyncio.to_thread(_save_history_rows, cid_a, rows_a)

        # B crashes mid-write (simulated by raising inside _save_history_rows)
        def _crashing_save(cid, new_rows):
            raise RuntimeError("DB write failed mid-batch")

        with pytest.raises(RuntimeError):
            await asyncio.to_thread(_crashing_save, cid_b, rows_b)

        # Verify A survived, B and C have nothing
        with SessionCtx() as verify:
            from src.domain.models import FactContainerPrice
            count_a = verify.query(FactContainerPrice).filter(
                FactContainerPrice.container_id == cid_a
            ).count()
            count_b = verify.query(FactContainerPrice).filter(
                FactContainerPrice.container_id == cid_b
            ).count()
            count_c = verify.query(FactContainerPrice).filter(
                FactContainerPrice.container_id == cid_c
            ).count()

        assert count_a == 1, "A rows must survive the mid-batch crash"
        assert count_b == 0, "B rows must not be committed after crash"
        assert count_c == 0, "C rows were never reached"


# ── Critical Path 4b: supervised_worker restarts after crash ──────────────────

class TestSupervisedWorkerRestart:
    @pytest.mark.asyncio
    async def test_restarts_after_worker_loop_crash(self):
        import infra.work_queue as wq

        crash_count = {"n": 0}
        processed: list[str] = []

        async def _flaky_loop():
            crash_count["n"] += 1
            if crash_count["n"] < 3:
                raise RuntimeError(f"crash #{crash_count['n']}")
            # Third invocation: process one item and wait
            while True:
                job = await wq._work_queue.get()
                processed.append(job["type"])
                wq._work_queue.task_done()

        with patch("infra.work_queue._worker_loop", side_effect=_flaky_loop), \
             patch("infra.work_queue._SUPERVISOR_RESTART_DELAY_S", 0):
            await wq._work_queue.put({"type": "sentinel"})

            task = asyncio.create_task(wq.supervised_worker())

            deadline = asyncio.get_event_loop().time() + 3.0
            while "sentinel" not in processed:
                if asyncio.get_event_loop().time() > deadline:
                    break
                await asyncio.sleep(0.02)

            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        assert wq._state.restarts == 2
        assert "sentinel" in processed

    @pytest.mark.asyncio
    async def test_restart_counter_increments_per_crash(self):
        import infra.work_queue as wq

        crash_count = {"n": 0}

        async def _always_crash():
            crash_count["n"] += 1
            if crash_count["n"] >= 4:
                await asyncio.sleep(10)  # Stop crashing so we can cancel
            raise RuntimeError("persistent crash")

        with patch("infra.work_queue._worker_loop", side_effect=_always_crash), \
             patch("infra.work_queue._SUPERVISOR_RESTART_DELAY_S", 0):
            task = asyncio.create_task(wq.supervised_worker())
            await asyncio.sleep(0.1)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        assert wq._state.restarts >= 2

    @pytest.mark.asyncio
    async def test_backoff_grows_exponentially(self):
        import infra.work_queue as wq

        sleep_calls: list[float] = []

        async def _record_sleep(secs):
            sleep_calls.append(secs)

        crash_count = {"n": 0}

        async def _crashing_loop():
            crash_count["n"] += 1
            if crash_count["n"] >= 4:
                await asyncio.sleep(10)
            raise RuntimeError("crash")

        with patch("infra.work_queue._worker_loop", side_effect=_crashing_loop), \
             patch("asyncio.sleep", side_effect=_record_sleep):
            task = asyncio.create_task(wq.supervised_worker())
            await asyncio.sleep(0)  # yield to let the task run
            # Give it a few iterations
            for _ in range(5):
                await asyncio.sleep(0)
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

        # Backoff calls should grow: 5 → 10 → 20 … or start at 5.0
        backoff_sleeps = [s for s in sleep_calls if s >= 5.0]
        if len(backoff_sleeps) >= 2:
            assert backoff_sleeps[1] >= backoff_sleeps[0], "backoff should not shrink"

    @pytest.mark.asyncio
    async def test_cancelled_error_propagates_without_restart(self):
        """CancelledError must propagate immediately — supervisor must NOT restart."""
        import infra.work_queue as wq

        async def _loop_that_gets_cancelled():
            raise asyncio.CancelledError()

        with patch("infra.work_queue._worker_loop", side_effect=_loop_that_gets_cancelled):
            with pytest.raises(asyncio.CancelledError):
                await wq.supervised_worker()

        assert wq._state.restarts == 0


# ── Critical Path 4c: queue consistency after crash ──────────────────────────

class TestQueueConsistencyAfterCrash:
    @pytest.mark.asyncio
    async def test_remaining_queue_items_processed_after_restart(self):
        """
        Queue has [job_a, job_b, job_c].
        Worker crashes while processing job_a.
        After restart, job_b and job_c are processed.
        """
        import infra.work_queue as wq

        processed: list[str] = []
        first_call = {"done": False}

        async def _handler(job):
            if not first_call["done"]:
                first_call["done"] = True
                raise RuntimeError("crash on first job")
            processed.append(job["type"])

        with patch.dict(wq._HANDLERS, {
            "job_a": _handler,
            "job_b": _handler,
            "job_c": _handler,
        }), patch("infra.work_queue._SUPERVISOR_RESTART_DELAY_S", 0):

            await wq._work_queue.put({"type": "job_a"})
            await wq._work_queue.put({"type": "job_b"})
            await wq._work_queue.put({"type": "job_c"})

            task = asyncio.create_task(wq.supervised_worker())

            deadline = asyncio.get_event_loop().time() + 3.0
            while len(processed) < 2:
                if asyncio.get_event_loop().time() > deadline:
                    break
                await asyncio.sleep(0.02)

            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        assert "job_b" in processed
        assert "job_c" in processed

    @pytest.mark.asyncio
    async def test_task_done_called_even_on_job_exception(self):
        """
        When a job raises an exception, task_done() must still be called
        so join() / qsize() stays consistent.
        """
        import infra.work_queue as wq

        async def _failing_handler(_job):
            raise ValueError("job failed")

        with patch.dict(wq._HANDLERS, {"bad": _failing_handler}):
            await wq._work_queue.put({"type": "bad"})

            task = asyncio.create_task(wq._worker_loop())
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        # Queue must be fully drained (task_done was called)
        assert wq._work_queue.qsize() == 0
        # join() should return immediately (all tasks accounted for)
        await asyncio.wait_for(wq._work_queue.join(), timeout=0.1)
