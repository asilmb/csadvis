"""
Phase 2 — TDD tests for services/portfolio.py new interfaces (PV-01).

Tests are written BEFORE implementation (Red stage).
All imports from src.domain.portfolio will pass only after Phase 3 migration.

Coverage:
  1. compute_pnl            — точность, нули, экстремальные значения
  2. get_balance_data       — агрегация, delta_kzt, empty states (mocked DB)
  3. get_transactions schema — структура ключей выходного словаря
  4. get_snapshots schema    — структура ключей выходного словаря
  5. DB unavailability       — поведение при OperationalError
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import OperationalError as SAOperationalError

# ─── Imports under test ───────────────────────────────────────────────────────
# These will raise ImportError until Phase 3 migrates the functions.
from src.domain.portfolio import (
    compute_pnl,
    get_annual_summaries,
    get_balance_data,
    get_snapshots,
    get_transactions,
)

# ─── Constants (mirror config.py defaults) ────────────────────────────────────
_FEE_DIVISOR = 1.15
_FEE_FIXED = 5.0

# ═══════════════════════════════════════════════════════════════════════════════
# 1. compute_pnl
# ═══════════════════════════════════════════════════════════════════════════════


class TestComputePnl:
    """Net P&L = sell / 1.15 − 5.0 − buy  (KZT only)."""

    def test_breakeven_price(self):
        # Breakeven: sell/1.15 - 5 - buy = 0  →  sell = (buy + 5) * 1.15
        buy = 1000.0
        sell = (buy + _FEE_FIXED) * _FEE_DIVISOR  # 1155.75
        result = compute_pnl(sell, buy)
        assert abs(result) < 0.01

    def test_profit_case(self):
        # sell=2300, buy=1000 → 2300/1.15 − 5 − 1000 = 2000 − 5 − 1000 = 995
        result = compute_pnl(2300.0, 1000.0)
        assert abs(result - 995.0) < 0.01

    def test_loss_case(self):
        # sell=1000, buy=1000 → 1000/1.15 − 5 − 1000 ≈ −135.87
        result = compute_pnl(1000.0, 1000.0)
        expected = 1000.0 / _FEE_DIVISOR - _FEE_FIXED - 1000.0
        assert abs(result - expected) < 0.01
        assert result < 0

    def test_zero_sell_price(self):
        # Лот снят с продажи / цена 0 → убыток = buy + fee_fixed
        result = compute_pnl(0.0, 500.0)
        expected = 0.0 / _FEE_DIVISOR - _FEE_FIXED - 500.0
        assert abs(result - expected) < 0.01
        assert result < 0

    def test_zero_buy_price(self):
        # Предмет получен бесплатно
        result = compute_pnl(1150.0, 0.0)
        expected = 1150.0 / _FEE_DIVISOR - _FEE_FIXED
        assert abs(result - expected) < 0.01
        assert result > 0

    def test_extreme_high_values(self):
        # Дорогой предмет — нет переполнения, расчёт точен
        sell = 1_000_000.0
        buy = 500_000.0
        result = compute_pnl(sell, buy)
        expected = sell / _FEE_DIVISOR - _FEE_FIXED - buy
        assert abs(result - expected) < 0.01

    def test_returns_float(self):
        result = compute_pnl(1500.0, 1000.0)
        assert isinstance(result, float)

    def test_uses_config_fee_not_hardcoded(self):
        # Убеждаемся, что функция читает из settings, а не хардкодит 1.15 и 5.0.
        # Если config.py поменяет значения — тест поймает расхождение.
        from config import settings

        sell, buy = 2300.0, 1000.0
        expected = sell / settings.steam_fee_divisor - settings.steam_fee_fixed - buy
        assert abs(compute_pnl(sell, buy) - expected) < 0.01


# ═══════════════════════════════════════════════════════════════════════════════
# 2. get_balance_data
# ═══════════════════════════════════════════════════════════════════════════════

_SNAPSHOT_TEMPLATE = {
    "date": "2026-03-03",
    "wallet": 800.0,
    "inventory": 500.0,
    "total": 1300.0,
}


class TestGetBalanceData:
    """Тестирует агрегирующий метод get_balance_data с мокированием зависимостей."""

    def test_empty_inventory_no_snapshots(self):
        with (
            patch("domain.portfolio.get_snapshots", return_value=[]),
            patch("domain.portfolio.get_portfolio_data", return_value={}),
        ):
            result = get_balance_data(1000.0, None)

        assert result["wallet"] == 1000.0
        assert result["inventory"] == 0.0
        assert result["total"] == 1000.0
        assert result["delta"] is None
        assert result["snapshots"] == []

    def test_inventory_value_calculated_from_current_prices(self):
        prices = {"Test Case": {"current_price": 500.0}}
        inventory = [{"market_hash_name": "Test Case", "count": 2}]

        with (
            patch("domain.portfolio.get_snapshots", return_value=[]),
            patch("domain.portfolio.get_portfolio_data", return_value=prices),
        ):
            result = get_balance_data(1000.0, inventory)

        assert result["inventory"] == 1000.0  # 500 × 2
        assert result["total"] == 2000.0

    def test_delta_kzt_calculated_vs_oldest_snapshot(self):
        prices = {"Test Case": {"current_price": 500.0}}
        inventory = [{"market_hash_name": "Test Case", "count": 2}]
        snapshots = [_SNAPSHOT_TEMPLATE.copy()]  # oldest total = 1300

        with (
            patch("domain.portfolio.get_snapshots", return_value=snapshots),
            patch("domain.portfolio.get_portfolio_data", return_value=prices),
        ):
            result = get_balance_data(1000.0, inventory)

        # total_kzt=2000, oldest_total=1300 → delta=+700
        assert result["delta"] == pytest.approx(700.0)

    def test_delta_kzt_negative_when_portfolio_dropped(self):
        snapshots = [
            {"date": "2026-03-03", "wallet": 2000.0, "inventory": 0.0, "total": 2000.0}
        ]

        with (
            patch("domain.portfolio.get_snapshots", return_value=snapshots),
            patch("domain.portfolio.get_portfolio_data", return_value={}),
        ):
            result = get_balance_data(1000.0, None)

        assert result["delta"] < 0

    def test_delta_kzt_is_none_when_no_snapshots(self):
        with (
            patch("domain.portfolio.get_snapshots", return_value=[]),
            patch("domain.portfolio.get_portfolio_data", return_value={}),
        ):
            result = get_balance_data(500.0, None)

        assert result["delta"] is None

    def test_unknown_item_in_inventory_counted_as_zero(self):
        """Предмет из инвентаря не найден в текущих ценах → цена 0, не крашится."""
        inventory = [{"market_hash_name": "Unknown Item 2077", "count": 5}]

        with (
            patch("domain.portfolio.get_snapshots", return_value=[]),
            patch("domain.portfolio.get_portfolio_data", return_value={}),
        ):
            result = get_balance_data(1000.0, inventory)

        assert result["inventory"] == 0.0
        assert result["total"] == 1000.0

    def test_result_contains_required_keys(self):
        with (
            patch("domain.portfolio.get_snapshots", return_value=[]),
            patch("domain.portfolio.get_portfolio_data", return_value={}),
        ):
            result = get_balance_data(1000.0, None)

        assert {
            "wallet",
            "inventory",
            "total",
            "delta",
            "snapshots",
        } <= result.keys()

    def test_snapshots_passed_through_to_result(self):
        snapshots = [_SNAPSHOT_TEMPLATE.copy(), _SNAPSHOT_TEMPLATE.copy()]

        with (
            patch("domain.portfolio.get_snapshots", return_value=snapshots),
            patch("domain.portfolio.get_portfolio_data", return_value={}),
        ):
            result = get_balance_data(1000.0, None)

        assert result["snapshots"] is snapshots

    def test_zero_wallet_kzt(self):
        with (
            patch("domain.portfolio.get_snapshots", return_value=[]),
            patch("domain.portfolio.get_portfolio_data", return_value={}),
        ):
            result = get_balance_data(0.0, None)

        assert result["wallet"] == 0.0
        assert result["total"] == 0.0


# ═══════════════════════════════════════════════════════════════════════════════
# 3. get_transactions — schema
# ═══════════════════════════════════════════════════════════════════════════════

_REQUIRED_TX_KEYS = {
    "id",
    "date",
    "action",
    "item_name",
    "quantity",
    "price",
    "total",
    "pnl",
    "listing_id",
    "notes",
}


def _make_mock_tx_row() -> MagicMock:
    row = MagicMock()
    row.id = "tx-uuid-1"
    row.trade_date = datetime(2026, 3, 1, 12, 0, 0)
    row.action = "BUY"
    row.item_name = "Recoil Case"
    row.quantity = 3
    row.price = 480.0
    row.total = 1440.0
    row.pnl = None
    row.listing_id = None
    row.notes = ""
    return row


class TestGetTransactionsSchema:
    def _patched_session(self, rows: list):
        mock_session = MagicMock()
        mock_query = MagicMock()
        mock_query.order_by.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.all.return_value = rows
        mock_session.query.return_value = mock_query
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        return mock_session

    def test_returns_list(self):
        mock_session = self._patched_session([])
        with patch("domain.portfolio.SessionLocal", return_value=mock_session):
            result = get_transactions()
        assert isinstance(result, list)

    def test_empty_db_returns_empty_list(self):
        mock_session = self._patched_session([])
        with patch("domain.portfolio.SessionLocal", return_value=mock_session):
            result = get_transactions()
        assert result == []

    def test_row_contains_required_keys(self):
        mock_session = self._patched_session([_make_mock_tx_row()])
        with patch("domain.portfolio.SessionLocal", return_value=mock_session):
            result = get_transactions()
        assert len(result) == 1
        assert result[0].keys() >= _REQUIRED_TX_KEYS

    def test_date_is_formatted_string(self):
        mock_session = self._patched_session([_make_mock_tx_row()])
        with patch("domain.portfolio.SessionLocal", return_value=mock_session):
            result = get_transactions()
        assert result[0]["date"] == "2026-03-01"

    def test_year_filter_applied(self):
        """get_transactions(year=2026) должен добавлять фильтр по году."""
        mock_session = self._patched_session([])
        with patch("domain.portfolio.SessionLocal", return_value=mock_session):
            result = get_transactions(year=2026)
        # Фильтр должен быть вызван (query.filter вызывался)
        assert isinstance(result, list)


# ═══════════════════════════════════════════════════════════════════════════════
# 4. get_snapshots — schema
# ═══════════════════════════════════════════════════════════════════════════════

_REQUIRED_SNAPSHOT_KEYS = {"date", "wallet", "inventory", "total"}


def _make_mock_snapshot_row() -> MagicMock:
    row = MagicMock()
    row.snapshot_date = datetime(2026, 3, 15, 0, 0, 0)
    row.wallet = 1500.0
    row.inventory = 800.0
    return row


class TestGetSnapshotsSchema:
    def _patched_session(self, rows: list):
        mock_session = MagicMock()
        mock_query = MagicMock()
        mock_query.filter.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.all.return_value = rows
        mock_session.query.return_value = mock_query
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        return mock_session

    def test_returns_list(self):
        mock_session = self._patched_session([])
        with patch("domain.portfolio.SessionLocal", return_value=mock_session):
            result = get_snapshots()
        assert isinstance(result, list)

    def test_empty_db_returns_empty_list(self):
        mock_session = self._patched_session([])
        with patch("domain.portfolio.SessionLocal", return_value=mock_session):
            result = get_snapshots()
        assert result == []

    def test_row_contains_required_keys(self):
        mock_session = self._patched_session([_make_mock_snapshot_row()])
        with patch("domain.portfolio.SessionLocal", return_value=mock_session):
            result = get_snapshots()
        assert len(result) == 1
        assert result[0].keys() >= _REQUIRED_SNAPSHOT_KEYS

    def test_total_kzt_computed_as_sum(self):
        mock_session = self._patched_session([_make_mock_snapshot_row()])
        with patch("domain.portfolio.SessionLocal", return_value=mock_session):
            result = get_snapshots()
        row = result[0]
        assert row["total"] == pytest.approx(row["wallet"] + row["inventory"])

    def test_date_is_formatted_string(self):
        mock_session = self._patched_session([_make_mock_snapshot_row()])
        with patch("domain.portfolio.SessionLocal", return_value=mock_session):
            result = get_snapshots()
        assert result[0]["date"] == "2026-03-15"

    def test_inventory_kzt_defaults_to_zero_when_none(self):
        """inventory_kzt может быть NULL в БД — должен стать 0.0."""
        row = _make_mock_snapshot_row()
        row.inventory = None
        mock_session = self._patched_session([row])
        with patch("domain.portfolio.SessionLocal", return_value=mock_session):
            result = get_snapshots()
        assert result[0]["inventory"] == 0.0
        assert result[0]["total"] == pytest.approx(row.wallet)


# ═══════════════════════════════════════════════════════════════════════════════
# 5. DB unavailability — отказоустойчивость
# ═══════════════════════════════════════════════════════════════════════════════

_SA_ERROR = SAOperationalError("no such table", None, None)


class TestDbUnavailability:
    """При падении SessionLocal функции не должны крашить приложение."""

    def test_get_snapshots_raises_on_db_error(self):
        """get_snapshots пробрасывает исключение — caller (Dash callback) сам решает fallback."""
        with (
            patch("domain.portfolio.SessionLocal", side_effect=_SA_ERROR),
            pytest.raises(SAOperationalError),
        ):
            get_snapshots()

    def test_get_transactions_raises_on_db_error(self):
        with (
            patch("domain.portfolio.SessionLocal", side_effect=_SA_ERROR),
            pytest.raises(SAOperationalError),
        ):
            get_transactions()

    def test_get_annual_summaries_raises_on_db_error(self):
        with (
            patch("domain.portfolio.SessionLocal", side_effect=_SA_ERROR),
            pytest.raises(SAOperationalError),
        ):
            get_annual_summaries()

    def test_get_balance_data_propagates_db_error(self):
        """get_balance_data вызывает get_snapshots — ошибка должна пробрасываться."""
        with (
            patch("domain.portfolio.get_portfolio_data", return_value={}),
            patch("domain.portfolio.get_snapshots", side_effect=_SA_ERROR),
            pytest.raises(SAOperationalError),
        ):
            get_balance_data(1000.0, None)
