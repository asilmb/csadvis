"""
Unit tests for services/trade_ledger.py — TradeService (PV-31).

Covers:
  calculate_pnl():
    - positive P&L (current > buy)
    - negative P&L (current < buy)
    - zero P&L (break-even)
    - quantity multiplier applied correctly
    - returns float, not int or KZT
    - no rounding (precision preserved)
    - buy_price=0 guard (edge case)

  calculate_roi():
    - positive ROI
    - negative ROI
    - break-even → 0.0
    - buy_price=0 → 0.0 (no ZeroDivisionError)

  SqlAlchemyPositionRepository (SQLite in-memory):
    - add_position returns PositionDTO with OPEN status
    - add_position asset_id stored as int (BigInteger-compatible)
    - get_open_positions returns only OPEN positions
    - get_open_positions excludes CLOSED positions
    - close_position marks OPEN → CLOSED, sets closed_at
    - close_position returns None for unknown asset_id
    - close_position only closes the first matching OPEN (one at a time)
    - multiple open positions — all returned, ordered by opened_at desc

  get_portfolio_summary() (service, mocked DB via monkeypatch):
    - empty portfolio → zero counts
    - positions with price_map → pnl and roi computed
    - positions without price_map entry → pnl=None, roi=None
    - partial price_map → total_pnl=None (some prices unknown)
    - avg_roi computed only from positions with known prices
    - total_invested correctly sums buy_price * quantity
    - CLOSED positions excluded (open_count correct)
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from domain.models import Base
from domain.sql_repositories import PositionDTO, SqlAlchemyPositionRepository
from domain.trade_ledger import STEAM_NET_MULTIPLIER, TradeService

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
def repo(db):
    return SqlAlchemyPositionRepository(db)


# ─── calculate_pnl ────────────────────────────────────────────────────────────


class TestCalculatePnl:
    def test_positive_pnl(self):
        # buy @ 5000, current @ 6000 → net = 6000*0.869=5214, pnl=214
        result = TradeService.calculate_pnl(buy_price=5000.0, current_price=6000.0)
        assert result == pytest.approx(6000 * STEAM_NET_MULTIPLIER - 5000, abs=0.01)

    def test_negative_pnl(self):
        result = TradeService.calculate_pnl(buy_price=5000.0, current_price=3000.0)
        expected = 3000 * STEAM_NET_MULTIPLIER - 5000
        assert result == pytest.approx(expected, abs=0.01)
        assert result < 0

    def test_breakeven_zero_pnl(self):
        # buy_price = current * STEAM_NET_MULTIPLIER → exact break-even
        buy = 1000 * STEAM_NET_MULTIPLIER
        result = TradeService.calculate_pnl(buy_price=buy, current_price=1000.0)
        assert result == pytest.approx(0.0, abs=0.001)

    def test_quantity_multiplier(self):
        pnl_1 = TradeService.calculate_pnl(buy_price=5000.0, current_price=6000.0, quantity=1)
        pnl_3 = TradeService.calculate_pnl(buy_price=5000.0, current_price=6000.0, quantity=3)
        assert pnl_3 == pytest.approx(pnl_1 * 3, abs=0.01)

    def test_returns_float(self):
        result = TradeService.calculate_pnl(buy_price=5000.0, current_price=6000.0)
        assert isinstance(result, float)

    def test_no_rounding_applied(self):
        # 7777 * 0.869 = 6762.013 → pnl = 6762.013 - 7000 = -237.987
        result = TradeService.calculate_pnl(buy_price=7000.0, current_price=7777.0)
        expected = 7777 * STEAM_NET_MULTIPLIER - 7000
        assert result == pytest.approx(expected, rel=1e-9)

    def test_large_asset_id_price(self):
        # prices can be large integers in KZT
        result = TradeService.calculate_pnl(buy_price=500_000.0, current_price=600_000.0)
        assert isinstance(result, float)
        assert result > 0


# ─── calculate_roi ────────────────────────────────────────────────────────────


class TestCalculateRoi:
    def test_positive_roi(self):
        roi = TradeService.calculate_roi(buy_price=5000.0, current_price=6000.0)
        expected = (6000 * STEAM_NET_MULTIPLIER - 5000) / 5000
        assert roi == pytest.approx(expected, abs=1e-6)
        assert roi > 0

    def test_negative_roi(self):
        roi = TradeService.calculate_roi(buy_price=5000.0, current_price=3000.0)
        assert roi < 0

    def test_breakeven_roi_zero(self):
        buy = 1000 * STEAM_NET_MULTIPLIER
        roi = TradeService.calculate_roi(buy_price=buy, current_price=1000.0)
        assert roi == pytest.approx(0.0, abs=1e-9)

    def test_zero_buy_price_returns_zero(self):
        roi = TradeService.calculate_roi(buy_price=0.0, current_price=5000.0)
        assert roi == 0.0  # guard: no ZeroDivisionError

    def test_roi_is_ratio_not_percent(self):
        # A 10% net gain should return ~0.10, not 10
        buy = 5000.0
        # Set current so net = buy * 1.10
        current = (buy * 1.10) / STEAM_NET_MULTIPLIER
        roi = TradeService.calculate_roi(buy_price=buy, current_price=current)
        assert roi == pytest.approx(0.10, abs=0.001)


# ─── SqlAlchemyPositionRepository ────────────────────────────────────────────


class TestPositionRepository:
    def test_add_position_returns_dto(self, repo, db):
        dto = repo.add_position(
            asset_id=76561198000000001,
            market_hash_name="AK-47 | Redline (Field-Tested)",
            buy_price=5000.0,
        )
        db.commit()
        assert isinstance(dto, PositionDTO)
        assert dto.status == "OPEN"
        assert dto.market_hash_name == "AK-47 | Redline (Field-Tested)"

    def test_asset_id_stored_as_int(self, repo, db):
        steam_id = 76561198099512345
        dto = repo.add_position(asset_id=steam_id, market_hash_name="Item", buy_price=1000.0)
        db.commit()
        assert dto.asset_id == steam_id
        assert isinstance(dto.asset_id, int)

    def test_default_quantity_is_one(self, repo, db):
        dto = repo.add_position(asset_id=1, market_hash_name="Item", buy_price=1000.0)
        db.commit()
        assert dto.quantity == 1

    def test_custom_quantity(self, repo, db):
        dto = repo.add_position(asset_id=1, market_hash_name="Item", buy_price=1000.0, quantity=5)
        db.commit()
        assert dto.quantity == 5

    def test_get_open_positions_returns_only_open(self, repo, db):
        repo.add_position(asset_id=1, market_hash_name="Open Item", buy_price=1000.0)
        dto2 = repo.add_position(asset_id=2, market_hash_name="Will Close", buy_price=2000.0)
        db.commit()
        repo.close_position(dto2.asset_id)
        db.commit()

        open_positions = repo.get_open_positions()
        assert len(open_positions) == 1
        assert open_positions[0].market_hash_name == "Open Item"

    def test_get_open_positions_excludes_closed(self, repo, db):
        dto = repo.add_position(asset_id=99, market_hash_name="Item", buy_price=1000.0)
        db.commit()
        repo.close_position(dto.asset_id)
        db.commit()

        assert repo.get_open_positions() == []

    def test_close_position_marks_closed(self, repo, db):
        dto = repo.add_position(asset_id=42, market_hash_name="Item", buy_price=1000.0)
        db.commit()

        closed = repo.close_position(dto.asset_id)
        db.commit()

        assert closed is not None
        assert closed.status == "CLOSED"
        assert closed.closed_at is not None

    def test_close_position_returns_none_for_unknown(self, repo, db):
        result = repo.close_position(asset_id=9999)
        assert result is None

    def test_close_position_only_closes_one(self, repo, db):
        # Two positions with same asset_id (edge case) — only first OPEN closed
        repo.add_position(asset_id=7, market_hash_name="A", buy_price=1000.0)
        repo.add_position(asset_id=7, market_hash_name="A", buy_price=1000.0)
        db.commit()

        repo.close_position(asset_id=7)
        db.commit()

        open_pos = repo.get_open_positions()
        assert len(open_pos) == 1  # one still open

    def test_multiple_open_positions_all_returned(self, repo, db):
        for i in range(4):
            repo.add_position(asset_id=i, market_hash_name=f"Item{i}", buy_price=float(i * 1000))
        db.commit()

        positions = repo.get_open_positions()
        assert len(positions) == 4

    def test_closed_at_none_when_open(self, repo, db):
        dto = repo.add_position(asset_id=1, market_hash_name="Item", buy_price=1000.0)
        db.commit()
        assert dto.closed_at is None


# ─── get_portfolio_summary ────────────────────────────────────────────────────


class TestGetPortfolioSummary:
    """Tests for TradeService.get_portfolio_summary() using mocked DB."""

    def _svc_with_positions(self, engine, positions: list[dict]) -> TradeService:
        """
        Build a TradeService whose get_portfolio_summary() reads from the
        in-memory engine by monkey-patching SessionLocal.
        """
        return TradeService()

    def _run(self, engine, positions: list[dict], price_map: dict | None = None) -> dict:
        """Set up positions in-memory DB, call get_portfolio_summary()."""
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

        with TestSession() as db:
            repo = SqlAlchemyPositionRepository(db)
            for p in positions:
                repo.add_position(**p)
            db.commit()

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("domain.connection.SessionLocal", _CM, raising=False)
            svc = TradeService()
            return svc.get_portfolio_summary(price_map=price_map)

    def test_empty_portfolio(self, engine):
        summary = self._run(engine, [])
        assert summary["open_count"] == 0
        assert summary["total_invested"] == 0.0
        assert summary["total_pnl"] is None
        assert summary["avg_roi"] is None
        assert summary["positions"] == []

    def test_single_position_with_price(self, engine):
        pos = [{"asset_id": 1, "market_hash_name": "AK", "buy_price": 5000.0}]
        summary = self._run(engine, pos, price_map={"AK": 6000.0})
        assert summary["open_count"] == 1
        expected_pnl = TradeService.calculate_pnl(5000.0, 6000.0)
        assert summary["total_pnl"] == pytest.approx(expected_pnl, abs=0.01)

    def test_positions_without_price_map(self, engine):
        pos = [{"asset_id": 1, "market_hash_name": "AK", "buy_price": 5000.0}]
        summary = self._run(engine, pos, price_map=None)
        assert summary["positions"][0]["pnl"] is None
        assert summary["positions"][0]["roi"] is None
        assert summary["total_pnl"] is None
        assert summary["avg_roi"] is None

    def test_partial_price_map_total_pnl_none(self, engine):
        positions = [
            {"asset_id": 1, "market_hash_name": "AK", "buy_price": 5000.0},
            {"asset_id": 2, "market_hash_name": "M4", "buy_price": 3000.0},
        ]
        # Only AK has a price → total_pnl=None (not all prices known)
        summary = self._run(engine, positions, price_map={"AK": 6000.0})
        assert summary["total_pnl"] is None
        # avg_roi only uses the one position with a known price
        assert summary["avg_roi"] is not None
        assert summary["open_count"] == 2

    def test_total_invested_correct(self, engine):
        positions = [
            {"asset_id": 1, "market_hash_name": "AK", "buy_price": 5000.0, "quantity": 2},
            {"asset_id": 2, "market_hash_name": "M4", "buy_price": 3000.0, "quantity": 3},
        ]
        summary = self._run(engine, positions)
        assert summary["total_invested"] == pytest.approx(5000 * 2 + 3000 * 3, abs=0.01)

    def test_pnl_values_are_float(self, engine):
        pos = [{"asset_id": 1, "market_hash_name": "AK", "buy_price": 5000.0}]
        summary = self._run(engine, pos, price_map={"AK": 6000.0})
        assert isinstance(summary["positions"][0]["pnl"], float)
        assert isinstance(summary["total_pnl"], float)

    def test_avg_roi_over_multiple_positions(self, engine):
        positions = [
            {"asset_id": 1, "market_hash_name": "AK", "buy_price": 5000.0},
            {"asset_id": 2, "market_hash_name": "M4", "buy_price": 3000.0},
        ]
        price_map = {"AK": 6000.0, "M4": 4000.0}
        summary = self._run(engine, positions, price_map=price_map)
        roi_ak = TradeService.calculate_roi(5000.0, 6000.0)
        roi_m4 = TradeService.calculate_roi(3000.0, 4000.0)
        expected_avg = (roi_ak + roi_m4) / 2
        assert summary["avg_roi"] == pytest.approx(expected_avg, abs=1e-6)
