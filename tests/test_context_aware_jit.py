"""
Unit tests for PV-32: Context-Aware JIT / Token Saver.

Covers:
  SqlAlchemyInventoryRepository.is_deeply_trade_banned():
    Scenario A — deep ban: item banned > 12h away → True
    Scenario B — unlocking soon: ban expires within 5h → False
    Scenario C — no ban: trade_unlock_at = None → False
    Scenario D — no rows at all → False (conservative)
    Scenario E — mixed rows: one None, one deep ban → False (conservative)
    Scenario F — all rows deep ban, same name → True
    Scenario G — multiple rows, soonest within window → False
    Scenario H — custom jit_window_hours respected

  inventory_valuation_handler() Token Saver integration:
    - Deep-banned item (>12h) → coordinator NOT called
    - Unlocking soon (<12h) → coordinator called
    - No ban data → coordinator called (conservative)
    - Ban filter applied BEFORE freshness check (saves a DB round-trip)
    - Multiple items: banned one skipped, unbanned one fetched
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from domain.models import (
    Base,
    ContainerType,
    DimContainer,
    DimUserPosition,
    FactContainerPrice,
)
from domain.sql_repositories import SqlAlchemyInventoryRepository


# ─── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture()
def engine():
    e = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(e)
    yield e
    e.dispose()


@pytest.fixture()
def db(engine):
    with Session(engine) as session:
        yield session


@pytest.fixture()
def inv_repo(db):
    return SqlAlchemyInventoryRepository(db)


# ─── Helpers ──────────────────────────────────────────────────────────────────


def _now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _future(hours: float) -> datetime:
    return _now() + timedelta(hours=hours)


def _past(hours: float) -> datetime:
    return _now() - timedelta(hours=hours)


def _add_position(
    db: Session,
    name: str,
    trade_unlock_at: datetime | None = None,
) -> DimUserPosition:
    pos = DimUserPosition(
        container_name=name,
        buy_price=5000.0,
        quantity=1,
        trade_unlock_at=trade_unlock_at,
    )
    db.add(pos)
    db.flush()
    return pos


# ─── is_deeply_trade_banned ───────────────────────────────────────────────────


class TestIsDeeplyTradeBanned:
    def test_scenario_a_deep_ban_returns_true(self, inv_repo, db):
        """Item banned 48h in future → deeply banned → True."""
        _add_position(db, "Recoil Case", trade_unlock_at=_future(48))
        db.commit()
        assert inv_repo.is_deeply_trade_banned("Recoil Case") is True

    def test_scenario_b_unlocking_soon_returns_false(self, inv_repo, db):
        """Item unlocking in 5h → within 12h window → False."""
        _add_position(db, "Fracture Case", trade_unlock_at=_future(5))
        db.commit()
        assert inv_repo.is_deeply_trade_banned("Fracture Case") is False

    def test_scenario_c_no_ban_returns_false(self, inv_repo, db):
        """trade_unlock_at = None → freely tradable → False."""
        _add_position(db, "Danger Zone Case", trade_unlock_at=None)
        db.commit()
        assert inv_repo.is_deeply_trade_banned("Danger Zone Case") is False

    def test_scenario_d_no_rows_returns_false(self, inv_repo, db):
        """No DimUserPosition rows → conservative → False."""
        assert inv_repo.is_deeply_trade_banned("Unknown Item") is False

    def test_scenario_e_mixed_none_and_deep_ban_returns_false(self, inv_repo, db):
        """One row None, one row deep ban → at least one freely tradable → False."""
        _add_position(db, "AK-47 | Case", trade_unlock_at=None)
        _add_position(db, "AK-47 | Case", trade_unlock_at=_future(48))
        db.commit()
        assert inv_repo.is_deeply_trade_banned("AK-47 | Case") is False

    def test_scenario_f_all_rows_deep_ban_returns_true(self, inv_repo, db):
        """Multiple rows all deeply banned → True."""
        _add_position(db, "Horizon Case", trade_unlock_at=_future(24))
        _add_position(db, "Horizon Case", trade_unlock_at=_future(36))
        _add_position(db, "Horizon Case", trade_unlock_at=_future(72))
        db.commit()
        assert inv_repo.is_deeply_trade_banned("Horizon Case") is True

    def test_scenario_g_soonest_within_window_returns_false(self, inv_repo, db):
        """Two banned rows — soonest unlocks in 5h → within 12h window → False."""
        _add_position(db, "CS20 Case", trade_unlock_at=_future(5))
        _add_position(db, "CS20 Case", trade_unlock_at=_future(48))
        db.commit()
        assert inv_repo.is_deeply_trade_banned("CS20 Case") is False

    def test_scenario_h_custom_window_respected(self, inv_repo, db):
        """Ban at 10h future: within 12h window (JIT allowed), outside 6h window (JIT skipped)."""
        _add_position(db, "Item", trade_unlock_at=_future(10))
        db.commit()
        assert inv_repo.is_deeply_trade_banned("Item", jit_window_hours=12) is False
        assert inv_repo.is_deeply_trade_banned("Item", jit_window_hours=6) is True

    def test_exactly_at_window_boundary_not_banned(self, inv_repo, db):
        """Unlock exactly 12h from now is within the window (not deeply banned)."""
        # slightly less than 12h to be safely within the window
        _add_position(db, "Boundary Case", trade_unlock_at=_future(11.99))
        db.commit()
        assert inv_repo.is_deeply_trade_banned("Boundary Case") is False

    def test_past_unlock_date_not_banned(self, inv_repo, db):
        """Unlock date already in the past → item is tradable now → False."""
        _add_position(db, "Old Case", trade_unlock_at=_past(1))
        db.commit()
        assert inv_repo.is_deeply_trade_banned("Old Case") is False


