"""
Unit tests for the Repository Pattern (DDD-3.1).

Tests cover:
  - domain/repositories.py  — InventoryRepository ABC contract
  - database/repositories.py — SqlAlchemyInventoryRepository (mocked Session)
  - services/portfolio.py    — get_balance_data() with injected repo
"""

from __future__ import annotations

from collections import namedtuple
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from database.repositories import SqlAlchemyInventoryRepository
from domain.repositories import InventoryRepository
from domain.value_objects import Amount
from services.portfolio import get_balance_data

# ─── Helpers / Fakes ──────────────────────────────────────────────────────────

class FakeInventoryRepository(InventoryRepository):
    """In-memory stub for unit testing services that depend on InventoryRepository."""

    def __init__(self, items: list[dict] | None = None, total_balance: float = 0.0):
        self._items = items or []
        self._total_balance = total_balance
        self.updated: list[tuple[str, int]] = []

    def get_all_items(self) -> list[dict]:
        return self._items

    def update_item_quantity(self, item_name: str, qty: int) -> None:
        self.updated.append((item_name, qty))

    def get_total_balance(self) -> Amount:
        return Amount(self._total_balance)


# ─── ABC contract ─────────────────────────────────────────────────────────────

class TestInventoryRepositoryABC:
    def test_cannot_instantiate_abc_directly(self):
        with pytest.raises(TypeError):
            InventoryRepository()  # type: ignore[abstract]

    def test_fake_repo_is_valid_subclass(self):
        repo = FakeInventoryRepository()
        assert isinstance(repo, InventoryRepository)

    def test_get_all_items_returns_list(self):
        repo = FakeInventoryRepository(items=[{"name": "Clutch Case", "current_price": 500.0}])
        items = repo.get_all_items()
        assert isinstance(items, list)

    def test_get_total_balance_returns_kzt(self):
        repo = FakeInventoryRepository(total_balance=5000.0)
        bal = repo.get_total_balance()
        assert isinstance(bal, Amount)
        assert bal.amount == 5000.0

    def test_update_item_quantity_is_callable(self):
        repo = FakeInventoryRepository()
        repo.update_item_quantity("Clutch Case", 42)
        assert ("Clutch Case", 42) in repo.updated


# ─── SqlAlchemyInventoryRepository — get_all_items ────────────────────────────

def _make_db_mock(containers=None, latest_rows=None, recent_rows=None):
    """Build a minimal Session mock for SqlAlchemy repository tests."""
    db = MagicMock()
    mock_query = MagicMock()
    db.query.return_value = mock_query

    # Chaining: .filter().group_by().subquery() etc. all return mock_query
    mock_query.filter.return_value = mock_query
    mock_query.group_by.return_value = mock_query
    mock_query.subquery.return_value = mock_query
    mock_query.join.return_value = mock_query
    mock_query.order_by.return_value = mock_query
    mock_query.first.return_value = None
    mock_query.scalar.return_value = None
    mock_query.all.return_value = []
    return db


class TestSqlAlchemyGetAllItems:
    def test_returns_empty_when_no_containers(self):
        db = _make_db_mock()
        repo = SqlAlchemyInventoryRepository(db)
        result = repo.get_all_items()
        assert result == []

    def test_returns_list_of_dicts(self):
        db = _make_db_mock()
        repo = SqlAlchemyInventoryRepository(db)
        assert isinstance(repo.get_all_items(), list)


# ─── SqlAlchemyInventoryRepository — get_total_balance ────────────────────────

class TestSqlAlchemyGetTotalBalance:
    def test_returns_kzt_zero_when_no_snapshot(self):
        db = MagicMock()
        mock_q = MagicMock()
        db.query.return_value = mock_q
        mock_q.order_by.return_value = mock_q
        mock_q.first.return_value = None

        repo = SqlAlchemyInventoryRepository(db)
        result = repo.get_total_balance()
        assert isinstance(result, Amount)
        assert result.amount == 0.0

    def test_returns_sum_of_wallet_and_inventory(self):
        FakeSnapshot = namedtuple("FakeSnapshot", ["wallet", "inventory", "snapshot_date"])
        snap = FakeSnapshot(wallet=3000.0, inventory=2000.0, snapshot_date=datetime(2026, 4, 4))

        db = MagicMock()
        mock_q = MagicMock()
        db.query.return_value = mock_q
        mock_q.order_by.return_value = mock_q
        mock_q.first.return_value = snap

        repo = SqlAlchemyInventoryRepository(db)
        result = repo.get_total_balance()
        assert isinstance(result, Amount)
        assert result.amount == 5000.0

    def test_handles_none_inventory_kzt(self):
        FakeSnapshot = namedtuple("FakeSnapshot", ["wallet", "inventory", "snapshot_date"])
        snap = FakeSnapshot(wallet=4000.0, inventory=None, snapshot_date=datetime(2026, 4, 4))

        db = MagicMock()
        mock_q = MagicMock()
        db.query.return_value = mock_q
        mock_q.order_by.return_value = mock_q
        mock_q.first.return_value = snap

        repo = SqlAlchemyInventoryRepository(db)
        result = repo.get_total_balance()
        assert result.amount == 4000.0


# ─── SqlAlchemyInventoryRepository — update_item_quantity ─────────────────────

class TestSqlAlchemyUpdateItemQuantity:
    def test_noop_when_container_not_found(self):
        db = MagicMock()
        mock_q = MagicMock()
        db.query.return_value = mock_q
        mock_q.filter.return_value = mock_q
        mock_q.first.return_value = None   # container not found

        repo = SqlAlchemyInventoryRepository(db)
        # Should not raise
        repo.update_item_quantity("Unknown Case", 10)

    def test_updates_volume_when_row_found(self):
        FakeContainer = namedtuple("FakeContainer", ["container_id"])
        container = FakeContainer(container_id="cid-1")
        row = MagicMock()
        row.volume_7d = 5

        db = MagicMock()

        # First call: DimContainer query → container
        # Second call: max(timestamp) → "2026-04-04"
        # Third call: FactContainerPrice row → row
        call_count = [0]
        def fake_query(model):
            call_count[0] += 1
            q = MagicMock()
            q.filter.return_value = q
            q.first.return_value = container if call_count[0] == 1 else row
            q.scalar.return_value = "2026-04-04"
            q.order_by.return_value = q
            return q

        db.query.side_effect = fake_query
        repo = SqlAlchemyInventoryRepository(db)
        repo.update_item_quantity("Clutch Case", 99)
        assert row.volume_7d == 99


# ─── get_balance_data with injected repository ────────────────────────────────

class TestGetBalanceDataWithRepo:
    def test_uses_repo_prices_for_inventory_valuation(self):
        repo = FakeInventoryRepository(
            items=[{"name": "Clutch Case", "current_price": 1000.0}],
        )
        inventory = [{"market_hash_name": "Clutch Case", "count": 2}]

        with patch("services.portfolio.get_snapshots", return_value=[]):
            result = get_balance_data(5000.0, inventory, repo=repo)

        assert result["inventory"] == 2000.0   # 2 × 1000₸
        assert result["wallet"] == 5000.0
        assert result["total"] == 7000.0

    def test_delta_computed_from_snapshots(self):
        repo = FakeInventoryRepository(items=[])
        snapshots = [{"date": "2026-03-01", "wallet": 4000.0, "inventory": 0.0, "total": 4000.0}]

        with patch("services.portfolio.get_snapshots", return_value=snapshots):
            result = get_balance_data(5000.0, None, repo=repo)

        assert result["delta"] == pytest.approx(1000.0)

    def test_empty_inventory_data_returns_zero_inventory(self):
        repo = FakeInventoryRepository(items=[])

        with patch("services.portfolio.get_snapshots", return_value=[]):
            result = get_balance_data(3000.0, None, repo=repo)

        assert result["inventory"] == 0.0
        assert result["total"] == 3000.0

    def test_missing_price_item_contributes_zero(self):
        repo = FakeInventoryRepository(items=[])  # no prices
        inventory = [{"market_hash_name": "Unknown Case", "count": 5}]

        with patch("services.portfolio.get_snapshots", return_value=[]):
            result = get_balance_data(1000.0, inventory, repo=repo)

        assert result["inventory"] == 0.0

    def test_no_repo_falls_back_to_get_portfolio_data(self):
        """Backward-compat: no repo arg → calls get_portfolio_data() as before."""
        with patch("services.portfolio.get_portfolio_data", return_value={}) as mock_gpd, \
             patch("services.portfolio.get_snapshots", return_value=[]):
            get_balance_data(1000.0, None)
        mock_gpd.assert_called_once()
