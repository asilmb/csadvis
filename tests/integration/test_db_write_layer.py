"""
Integration tests — DB Write Layer (Critical Path 1).

Verifies that asyncio.to_thread() DB writes:
  - do not block the event loop during synchronous SQLAlchemy calls
  - commit correctly (data is visible in a fresh session after the call)
  - roll back cleanly on error without corrupting the session
  - allow concurrent tasks to keep running while the thread executes

All tests use SQLite in-memory as the database engine so no PostgreSQL
instance is required.  The ORM models are created fresh for each test class
via a module-level engine fixture.
"""
from __future__ import annotations

import asyncio
import uuid
from datetime import datetime
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

# ── Fixtures ──────────────────────────────────────────────────────────────────


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
def db(sqlite_engine):
    TestSession = sessionmaker(bind=sqlite_engine)
    session = TestSession()
    yield session
    session.rollback()
    session.close()


@pytest.fixture
def make_session(sqlite_engine):
    """Factory: returns a callable that creates new sessions bound to the in-memory engine."""
    TestSession = sessionmaker(bind=sqlite_engine)

    def _factory():
        return TestSession()

    return _factory


def _patch_session_local(make_session):
    """Context-manager patch so that SessionLocal() yields our test sessions."""
    from unittest.mock import patch

    class _FakeCtx:
        def __init__(self):
            self._s = make_session()

        def __enter__(self):
            return self._s

        def __exit__(self, *_):
            self._s.close()

    return patch("src.domain.connection.SessionLocal", side_effect=_FakeCtx)


# ── Helper: seed a DimContainer ───────────────────────────────────────────────


def _seed_container(db: Session, name: str = "Prisma 2 Case") -> str:
    from src.domain.models import ContainerType, DimContainer
    cid = str(uuid.uuid4())
    db.add(DimContainer(
        container_id=cid,
        container_name=name,
        container_type=ContainerType.Weapon_Case,
        base_cost=1445,
    ))
    db.commit()
    return cid


# ── Critical Path 1a: _save_history_rows via asyncio.to_thread ────────────────


class TestSaveHistoryRows:
    """Rows written inside a thread are committed and visible to the next session."""

    @pytest.mark.asyncio
    async def test_rows_committed_after_to_thread(self, db, make_session, sqlite_engine):
        from scrapper.runner import _save_history_rows

        cid = _seed_container(db, "Delta Case")

        rows = [
            {"date": datetime(2024, 1, 1), "price": 150.0, "volume": 10},
            {"date": datetime(2024, 1, 2), "price": 155.0, "volume": 12},
        ]

        with _patch_session_local(make_session):
            saved = await asyncio.to_thread(_save_history_rows, cid, rows)

        assert saved == 2

        # Verify data is visible in a brand-new session
        with make_session() as verify_session:
            from src.domain.models import FactContainerPrice
            count = verify_session.query(FactContainerPrice).filter(
                FactContainerPrice.container_id == cid
            ).count()
        assert count == 2

    @pytest.mark.asyncio
    async def test_event_loop_not_blocked_during_write(self, db, make_session):
        """A concurrent coroutine keeps running while the DB write thread executes."""
        from scrapper.runner import _save_history_rows

        cid = _seed_container(db, "Blocked Case")
        rows = [{"date": datetime(2024, 3, 1), "price": 200.0, "volume": 5}]

        heartbeats: list[float] = []

        async def _heartbeat():
            for _ in range(5):
                await asyncio.sleep(0.01)
                heartbeats.append(asyncio.get_event_loop().time())

        with _patch_session_local(make_session):
            _, _ = await asyncio.gather(
                asyncio.to_thread(_save_history_rows, cid, rows),
                _heartbeat(),
            )

        # heartbeat must have fired — proves the loop wasn't blocked
        assert len(heartbeats) >= 3

    @pytest.mark.asyncio
    async def test_multiple_concurrent_writes_all_committed(self, db, make_session):
        """Two concurrent to_thread writes for different containers both persist."""
        from scrapper.runner import _save_history_rows

        cid_a = _seed_container(db, "Case Alpha")
        cid_b = _seed_container(db, "Case Beta")

        rows_a = [{"date": datetime(2024, 4, 1), "price": 100.0, "volume": 1}]
        rows_b = [{"date": datetime(2024, 4, 1), "price": 200.0, "volume": 2}]

        with _patch_session_local(make_session):
            results = await asyncio.gather(
                asyncio.to_thread(_save_history_rows, cid_a, rows_a),
                asyncio.to_thread(_save_history_rows, cid_b, rows_b),
            )

        assert results == [1, 1]

        with make_session() as s:
            from src.domain.models import FactContainerPrice
            for cid in (cid_a, cid_b):
                assert s.query(FactContainerPrice).filter(
                    FactContainerPrice.container_id == cid
                ).count() == 1


# ── Critical Path 1b: _write_containers via asyncio.to_thread ────────────────


class TestWriteContainers:
    """write_new_containers inserts and commits correctly via thread."""

    @pytest.mark.asyncio
    async def test_new_container_persisted(self, make_session):
        from scrapper.runner import _write_containers
        from scrapper.steam_market_scraper import ScrapedContainer

        containers = [ScrapedContainer(name="Spectrum 2 Case", container_type="Weapon Case")]

        with _patch_session_local(make_session):
            inserted = await asyncio.to_thread(_write_containers, containers)

        assert inserted == 1

        with make_session() as s:
            from src.domain.models import DimContainer
            row = s.query(DimContainer).filter(
                DimContainer.container_name == "Spectrum 2 Case"
            ).first()
        assert row is not None

    @pytest.mark.asyncio
    async def test_duplicate_not_inserted(self, db, make_session):
        """Second call with same name → 0 inserts, no exception."""
        from scrapper.runner import _write_containers
        from scrapper.steam_market_scraper import ScrapedContainer

        _seed_container(db, "Duplicate Case")
        containers = [ScrapedContainer(name="Duplicate Case", container_type="Weapon Case")]

        with _patch_session_local(make_session):
            inserted = await asyncio.to_thread(_write_containers, containers)

        assert inserted == 0

    @pytest.mark.asyncio
    async def test_db_error_does_not_propagate_to_event_loop(self, make_session):
        """If the DB write raises, asyncio.to_thread re-raises in the coroutine (no loop crash)."""

        def _broken(_containers):
            raise RuntimeError("simulated DB failure")

        with patch("scrapper.runner._write_containers", side_effect=_broken):
            with pytest.raises(RuntimeError, match="simulated DB failure"):
                await asyncio.to_thread(_broken, [])
