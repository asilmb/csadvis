"""
Tests for src/infra/scrape_guard.py

All tests use SQLite in-memory — no PostgreSQL instance required.
"""
from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.domain.models import Base, RateLimitLog, ScrapeSession

# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def engine():
    eng = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(eng)
    yield eng
    eng.dispose()


@pytest.fixture
def db_session(engine):
    Session = sessionmaker(bind=engine)
    s = Session()
    yield s
    s.rollback()
    s.close()


@pytest.fixture(autouse=True)
def patch_session_local(engine, monkeypatch):
    """Redirect SessionLocal used inside scrape_guard to the test SQLite engine."""

    Session_factory = sessionmaker(bind=engine)

    class _FakeContextSession:
        def __init__(self):
            self._s = Session_factory()

        def __enter__(self):
            return self._s

        def __exit__(self, *_):
            self._s.close()

        # proxy attribute access for tests that use it as a plain session
        def __getattr__(self, name):
            return getattr(self._s, name)

    monkeypatch.setattr("infra.scrape_guard.SessionLocal", _FakeContextSession)


# ── Cooldown tests ────────────────────────────────────────────────────────────

class TestRecord429:
    def test_creates_rate_limit_log_row(self, engine):
        from infra.scrape_guard import record_429
        result = record_429(triggered_by="Prisma 2 Case")
        assert isinstance(result, datetime)
        Session = sessionmaker(bind=engine)
        with Session() as s:
            row = s.query(RateLimitLog).order_by(RateLimitLog.id.desc()).first()
        assert row is not None
        assert row.triggered_by == "Prisma 2 Case"

    def test_cooldown_is_6_hours_from_now(self, engine):
        from infra.scrape_guard import _COOLDOWN_HOURS, record_429
        before = datetime.now(UTC).replace(tzinfo=None)
        result = record_429(triggered_by="test")
        after = datetime.now(UTC).replace(tzinfo=None)
        assert before + timedelta(hours=_COOLDOWN_HOURS) <= result
        assert result <= after + timedelta(hours=_COOLDOWN_HOURS)


class TestGetActiveCooldown:
    def test_returns_none_when_no_rows(self, engine):
        from infra.scrape_guard import get_active_cooldown
        Session = sessionmaker(bind=engine)
        with Session() as s:
            s.query(RateLimitLog).delete()
            s.commit()
        assert get_active_cooldown() is None

    def test_returns_future_cooldown(self, engine):
        from infra.scrape_guard import get_active_cooldown
        future = datetime.now(UTC).replace(tzinfo=None) + timedelta(hours=3)
        Session = sessionmaker(bind=engine)
        with Session() as s:
            s.query(RateLimitLog).delete()
            s.add(RateLimitLog(triggered_by="test", cooldown_until=future))
            s.commit()
        result = get_active_cooldown()
        assert result is not None
        assert abs((result - future).total_seconds()) < 1

    def test_returns_none_for_expired_cooldown(self, engine):
        from infra.scrape_guard import get_active_cooldown
        past = datetime.now(UTC).replace(tzinfo=None) - timedelta(hours=1)
        Session = sessionmaker(bind=engine)
        with Session() as s:
            s.query(RateLimitLog).delete()
            s.add(RateLimitLog(triggered_by="old", cooldown_until=past))
            s.commit()
        assert get_active_cooldown() is None


class TestCheckCooldown:
    def test_raises_scrape_blocked_when_active(self, engine):
        from infra.scrape_guard import ScrapeBlocked, check_cooldown
        future = datetime.now(UTC).replace(tzinfo=None) + timedelta(hours=2)
        Session = sessionmaker(bind=engine)
        with Session() as s:
            s.query(RateLimitLog).delete()
            s.add(RateLimitLog(triggered_by="x", cooldown_until=future))
            s.commit()
        with pytest.raises(ScrapeBlocked) as exc_info:
            check_cooldown()
        assert exc_info.value.cooldown_until == future

    def test_no_raise_when_no_active_cooldown(self, engine):
        from infra.scrape_guard import check_cooldown
        Session = sessionmaker(bind=engine)
        with Session() as s:
            s.query(RateLimitLog).delete()
            s.commit()
        check_cooldown()  # must not raise


# ── Session lifecycle tests ───────────────────────────────────────────────────

class TestCreateSession:
    def test_persists_session_with_correct_fields(self, engine):
        from infra.scrape_guard import create_session
        ids = ["id1", "id2", "id3"]
        sid = create_session("price_poll", ids)
        Session = sessionmaker(bind=engine)
        with Session() as s:
            sess = s.get(ScrapeSession, sid)
        assert sess is not None
        assert sess.job_type == "price_poll"
        assert sess.total_count == 3
        assert sess.processed_count == 0
        assert json.loads(sess.all_ids) == ids

    def test_returns_integer_id(self):
        from infra.scrape_guard import create_session
        sid = create_session("backfill_history", ["a", "b"])
        assert isinstance(sid, int)
        assert sid > 0


class TestTickSession:
    def test_increments_processed_count(self, engine):
        from infra.scrape_guard import create_session, tick_session
        sid = create_session("price_poll", ["x", "y", "z"])
        tick_session(sid)
        tick_session(sid)
        Session = sessionmaker(bind=engine)
        with Session() as s:
            sess = s.get(ScrapeSession, sid)
        assert sess.processed_count == 2

    def test_noop_for_nonexistent_session(self):
        from infra.scrape_guard import tick_session
        tick_session(999999)  # must not raise


class TestFinishSession:
    def test_deletes_session(self, engine):
        from infra.scrape_guard import create_session, finish_session
        sid = create_session("price_poll", ["a"])
        finish_session(sid)
        Session = sessionmaker(bind=engine)
        with Session() as s:
            assert s.get(ScrapeSession, sid) is None

    def test_noop_for_nonexistent_session(self):
        from infra.scrape_guard import finish_session
        finish_session(999999)  # must not raise


class TestRemainingIds:
    def test_returns_unprocessed_ids(self, engine):
        from infra.scrape_guard import create_session, remaining_ids, tick_session
        ids = ["a", "b", "c", "d"]
        sid = create_session("price_poll", ids)
        tick_session(sid)
        tick_session(sid)
        remaining = remaining_ids(sid)
        assert remaining == ["c", "d"]

    def test_returns_empty_for_completed(self, engine):
        from infra.scrape_guard import create_session, remaining_ids, tick_session
        ids = ["x"]
        sid = create_session("price_poll", ids)
        tick_session(sid)
        assert remaining_ids(sid) == []

    def test_returns_empty_for_nonexistent_session(self):
        from infra.scrape_guard import remaining_ids
        assert remaining_ids(999999) == []

    def test_handles_corrupt_all_ids_gracefully(self, engine):
        from infra.scrape_guard import remaining_ids
        Session = sessionmaker(bind=engine)
        with Session() as s:
            sess = ScrapeSession(
                job_type="price_poll",
                all_ids="not-valid-json",
                total_count=1,
                processed_count=0,
            )
            s.add(sess)
            s.commit()
            s.refresh(sess)
            sid = sess.id
        result = remaining_ids(sid)
        assert result == []
        with Session() as s:
            assert s.get(ScrapeSession, sid) is None  # corrupt row deleted


class TestListSessions:
    def test_returns_list_of_dicts(self, engine):
        from infra.scrape_guard import create_session, list_sessions
        Session = sessionmaker(bind=engine)
        with Session() as s:
            s.query(ScrapeSession).delete()
            s.commit()
        create_session("price_poll", ["a", "b"])
        sessions = list_sessions()
        assert len(sessions) == 1
        sess = sessions[0]
        assert "id" in sess
        assert sess["total_count"] == 2
        assert sess["processed_count"] == 0
        assert sess["remaining_count"] == 2


class TestDeleteSession:
    def test_removes_session(self, engine):
        from infra.scrape_guard import create_session, delete_session
        sid = create_session("price_poll", ["x"])
        delete_session(sid)
        Session = sessionmaker(bind=engine)
        with Session() as s:
            assert s.get(ScrapeSession, sid) is None

    def test_noop_for_nonexistent(self):
        from infra.scrape_guard import delete_session
        delete_session(999999)  # must not raise
