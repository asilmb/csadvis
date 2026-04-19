"""Unit tests for ItemService — service layer for market item data."""
from __future__ import annotations

from unittest.mock import MagicMock, patch


def _make_service(items: list[dict] | None = None, db=None):
    """Build an ItemService with a mocked InventoryRepository."""
    from src.domain.item_service import ItemService

    repo = MagicMock()
    repo.get_all_items.return_value = items or []
    svc = ItemService(repo, fee_divisor=1.15, fee_fixed=5.0, currency_symbol="₸")
    if db is not None:
        svc._db = db
    else:
        svc._db = None
    return svc


# ─── get_market_overview ──────────────────────────────────────────────────────


class TestGetMarketOverview:
    def test_returns_empty_when_no_items(self):
        svc = _make_service(items=[])
        assert svc.get_market_overview() == []

    def test_skips_items_with_zero_price(self):
        svc = _make_service(items=[
            {"name": "Item A", "current_price": 0, "mean_price": None, "quantity": 5, "lowest_price": None},
        ])
        assert svc.get_market_overview() == []

    def test_returns_item_dto_with_correct_name(self):
        svc = _make_service(items=[
            {"name": "Prisma 2 Case", "current_price": 150.0, "mean_price": 140.0, "quantity": 10, "lowest_price": 148.0},
        ])
        result = svc.get_market_overview()
        assert len(result) == 1
        assert result[0].name == "Prisma 2 Case"

    def test_flags_suspicious_price(self):
        # current > mean * 1.20 → suspicious
        svc = _make_service(items=[
            {"name": "X", "current_price": 200.0, "mean_price": 100.0, "quantity": 1, "lowest_price": None},
        ])
        result = svc.get_market_overview()
        assert result[0].is_suspicious is True

    def test_not_suspicious_when_price_normal(self):
        svc = _make_service(items=[
            {"name": "X", "current_price": 110.0, "mean_price": 100.0, "quantity": 1, "lowest_price": None},
        ])
        result = svc.get_market_overview()
        assert result[0].is_suspicious is False

    def test_handles_db_exception_gracefully(self):
        db = MagicMock()
        db.query.side_effect = Exception("DB unavailable")
        svc = _make_service(
            items=[{"name": "X", "current_price": 100.0, "mean_price": None, "quantity": 0, "lowest_price": None}],
            db=db,
        )
        result = svc.get_market_overview()
        assert len(result) == 1  # should not raise, returns what it can


# ─── process_new_price ────────────────────────────────────────────────────────


class TestProcessNewPrice:
    def test_rejects_zero_price(self):
        svc = _make_service()
        result = svc.process_new_price("some-id", 0.0)
        assert result is False

    def test_rejects_negative_price(self):
        svc = _make_service()
        assert svc.process_new_price("some-id", -10.0) is False

    def test_rejects_price_above_max(self):
        svc = _make_service()
        assert svc.process_new_price("some-id", 2_000_000.0) is False

    def test_rejects_nan(self):
        import math
        svc = _make_service()
        assert svc.process_new_price("some-id", math.nan) is False

    def test_rejects_unknown_container(self):
        mock_db = MagicMock()
        mock_db.__enter__ = lambda s: mock_db
        mock_db.__exit__ = MagicMock(return_value=False)
        mock_db.get.return_value = None  # container not found

        with patch("src.domain.connection.SessionLocal", return_value=mock_db):
            svc = _make_service()
            result = svc.process_new_price("unknown-id", 100.0)

        assert result is False

    def test_rejects_blacklisted_container(self):
        container = MagicMock()
        container.is_blacklisted = True

        mock_db = MagicMock()
        mock_db.__enter__ = lambda s: mock_db
        mock_db.__exit__ = MagicMock(return_value=False)
        mock_db.get.return_value = container

        with patch("src.domain.connection.SessionLocal", return_value=mock_db):
            svc = _make_service()
            result = svc.process_new_price("bl-id", 100.0)

        assert result is False


# ─── get_signals ──────────────────────────────────────────────────────────────


class TestGetSignals:
    def test_returns_cached_signals_when_available(self):
        cached = {"container-1": {"verdict": "BUY", "score": 0.8}}
        with patch("src.domain.portfolio.get_cached_signals", return_value=cached):
            svc = _make_service()
            result = svc.get_signals()
        assert result == cached

    def test_returns_empty_dict_when_no_items_and_no_cache(self):
        with patch("src.domain.portfolio.get_cached_signals", return_value={}):
            svc = _make_service(items=[])
            result = svc.get_signals()
        assert result == {}
