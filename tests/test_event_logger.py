"""
Unit tests for PV-17: services/event_logger.py

Covers:
  log_event():
    - inserts one EventLog row per call
    - level stored uppercase
    - message truncated to 1000 chars
    - module truncated to 100 chars
    - does NOT raise when DB is unavailable (swallows exception)
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from database.models import Base, EventLog


@pytest.fixture()
def engine():
    e = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(e)
    yield e
    e.dispose()


def _make_cm(engine):
    """Return a context-manager factory that wraps the in-memory engine."""
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


def _rows(engine) -> list[EventLog]:
    TestSession = sessionmaker(bind=engine)
    with TestSession() as s:
        return s.query(EventLog).order_by(EventLog.timestamp.asc()).all()


class TestLogEvent:
    def test_inserts_one_row(self, engine):
        from services.event_logger import log_event

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("database.connection.SessionLocal", _make_cm(engine), raising=False)
            log_event("INFO", "test_module", "something happened")

        rows = _rows(engine)
        assert len(rows) == 1

    def test_level_stored_uppercase(self, engine):
        from services.event_logger import log_event

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("database.connection.SessionLocal", _make_cm(engine), raising=False)
            log_event("warning", "test_module", "msg")

        rows = _rows(engine)
        assert rows[0].level == "WARNING"

    def test_message_truncated_to_1000(self, engine):
        from services.event_logger import log_event

        long_msg = "x" * 1500
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("database.connection.SessionLocal", _make_cm(engine), raising=False)
            log_event("INFO", "mod", long_msg)

        rows = _rows(engine)
        assert len(rows[0].message) == 1000

    def test_module_truncated_to_100(self, engine):
        from services.event_logger import log_event

        long_mod = "m" * 200
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("database.connection.SessionLocal", _make_cm(engine), raising=False)
            log_event("INFO", long_mod, "msg")

        rows = _rows(engine)
        assert len(rows[0].module) == 100

    def test_fields_stored_correctly(self, engine):
        from services.event_logger import log_event

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("database.connection.SessionLocal", _make_cm(engine), raising=False)
            log_event("ERROR", "signal_handler", "AUTH ERROR: Recoil Case — HTTP 403")

        rows = _rows(engine)
        assert rows[0].level == "ERROR"
        assert rows[0].module == "signal_handler"
        assert "403" in rows[0].message

    def test_does_not_raise_on_db_failure(self):
        """log_event must swallow DB errors — never crash the caller."""
        from services.event_logger import log_event

        class _BrokenCM:
            def __enter__(self):
                raise RuntimeError("DB unavailable")

            def __exit__(self, *_):
                pass

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("database.connection.SessionLocal", _BrokenCM, raising=False)
            # Must not raise
            log_event("INFO", "mod", "msg")

    def test_multiple_calls_multiple_rows(self, engine):
        from services.event_logger import log_event

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("database.connection.SessionLocal", _make_cm(engine), raising=False)
            log_event("INFO", "mod", "first")
            log_event("WARNING", "mod", "second")
            log_event("ERROR", "mod", "third")

        rows = _rows(engine)
        assert len(rows) == 3
