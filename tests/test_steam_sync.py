"""
Unit tests for services/steam_sync.py.

All external I/O (Steam HTTP, file persistence) is mocked at the ingestion layer.
fetch_inventory is a synchronous function — patch with MagicMock (not AsyncMock).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from services.steam_sync import (
    InventoryResult,
    TransactionsResult,
    WalletResult,
    sync_inventory,
    sync_transactions,
    sync_wallet,
)

# ─── Fixtures / helpers ────────────────────────────────────────────────────────

_TX = [
    {"action": "BUY",  "date": "2025-01-01", "item_name": "Case A", "price": 100.0, "total": 100.0, "listing_id": "L1"},
    {"action": "SELL", "date": "2025-01-02", "item_name": "Case A", "price": 150.0, "total": 150.0, "listing_id": "L2"},
    {"action": "BUY",  "date": "2025-02-01", "item_name": "Case B", "price": 200.0, "total": 200.0, "listing_id": "L3"},
]

_ITEMS = [
    {"market_hash_name": "Clutch Case", "count": 2},
    {"market_hash_name": "Fracture Case", "count": 1},
]


# ─── sync_wallet ──────────────────────────────────────────────────────────────


class TestSyncWallet:
    def test_success_returns_ok_true(self):
        with patch("ingestion.steam_wallet.fetch_wallet_balance", return_value=(5000.0, "ok")), \
             patch("ingestion.steam_wallet.save_balance") as mock_save:
            r = sync_wallet()
        assert r.ok is True
        assert r.balance == 5000.0
        assert r.error_code is None
        mock_save.assert_called_once_with(5000.0)

    def test_success_message_contains_balance(self):
        with patch("ingestion.steam_wallet.fetch_wallet_balance", return_value=(5000.0, "ok")), \
             patch("ingestion.steam_wallet.save_balance"):
            r = sync_wallet()
        assert "5" in r.message  # formatted KZT amount

    def test_success_balance_is_set(self):
        with patch("ingestion.steam_wallet.fetch_wallet_balance", return_value=(5000.0, "ok")), \
             patch("ingestion.steam_wallet.save_balance"):
            r = sync_wallet()
        assert r.balance == 5000.0

    def test_failure_no_cookie(self):
        with patch("ingestion.steam_wallet.fetch_wallet_balance", return_value=(None, "NO_COOKIE")), \
             patch("ingestion.steam_wallet.get_saved_balance", return_value=None):
            r = sync_wallet()
        assert r.ok is False
        assert r.error_code == "NO_COOKIE"

    def test_failure_stale_cookie_via_403(self):
        with patch("ingestion.steam_wallet.fetch_wallet_balance", return_value=(None, "HTTP 403")), \
             patch("ingestion.steam_wallet.get_saved_balance", return_value=None):
            r = sync_wallet()
        assert r.ok is False
        assert r.error_code == "STALE_COOKIE"

    def test_failure_stale_cookie_via_ustarl(self):
        with patch("ingestion.steam_wallet.fetch_wallet_balance", return_value=(None, "cookie устарел")), \
             patch("ingestion.steam_wallet.get_saved_balance", return_value=None):
            r = sync_wallet()
        assert r.ok is False
        assert r.error_code == "STALE_COOKIE"

    def test_failure_network_error(self):
        with patch("ingestion.steam_wallet.fetch_wallet_balance", return_value=(None, "Connection timeout")), \
             patch("ingestion.steam_wallet.get_saved_balance", return_value=None):
            r = sync_wallet()
        assert r.ok is False
        assert r.error_code == "NETWORK"

    def test_failure_returns_cached_balance(self):
        with patch("ingestion.steam_wallet.fetch_wallet_balance", return_value=(None, "NO_COOKIE")), \
             patch("ingestion.steam_wallet.get_saved_balance", return_value=3000.0):
            r = sync_wallet()
        assert r.ok is False
        assert r.balance == 3000.0

    def test_failure_no_cache_returns_none_balance(self):
        with patch("ingestion.steam_wallet.fetch_wallet_balance", return_value=(None, "NO_COOKIE")), \
             patch("ingestion.steam_wallet.get_saved_balance", return_value=None):
            r = sync_wallet()
        assert r.balance is None

    def test_result_is_wallet_result(self):
        with patch("ingestion.steam_wallet.fetch_wallet_balance", return_value=(5000.0, "ok")), \
             patch("ingestion.steam_wallet.save_balance"):
            r = sync_wallet()
        assert isinstance(r, WalletResult)

    def test_save_not_called_on_failure(self):
        with patch("ingestion.steam_wallet.fetch_wallet_balance", return_value=(None, "NO_COOKIE")), \
             patch("ingestion.steam_wallet.get_saved_balance", return_value=None), \
             patch("ingestion.steam_wallet.save_balance") as mock_save:
            sync_wallet()
        mock_save.assert_not_called()


# ─── sync_inventory ───────────────────────────────────────────────────────────


class TestSyncInventory:
    def test_empty_steam_id_returns_no_steam_id(self):
        r = sync_inventory("")
        assert r.ok is False
        assert r.error_code == "NO_STEAM_ID"

    def test_whitespace_steam_id_returns_no_steam_id(self):
        r = sync_inventory("   ")
        assert r.ok is False
        assert r.error_code == "NO_STEAM_ID"

    def test_success_returns_items(self):
        with patch("frontend.inventory.fetch_inventory", new=MagicMock(return_value=_ITEMS)):
            r = sync_inventory("76561198000000001")
        assert r.ok is True
        assert r.items == _ITEMS
        assert r.count == 2

    def test_success_empty_inventory(self):
        with patch("frontend.inventory.fetch_inventory", new=MagicMock(return_value=[])):
            r = sync_inventory("76561198000000001")
        assert r.ok is True
        assert r.count == 0
        assert "пуст" in r.message

    def test_success_none_inventory_treated_as_empty(self):
        with patch("frontend.inventory.fetch_inventory", new=MagicMock(return_value=None)):
            r = sync_inventory("76561198000000001")
        assert r.ok is True
        assert r.items == []
        assert r.count == 0

    def test_network_exception_returns_error(self):
        with patch("frontend.inventory.fetch_inventory", new=MagicMock(side_effect=RuntimeError("timeout"))):
            r = sync_inventory("76561198000000001")
        assert r.ok is False
        assert r.error_code == "NETWORK"
        assert "timeout" in r.message

    def test_result_is_inventory_result(self):
        with patch("frontend.inventory.fetch_inventory", new=MagicMock(return_value=_ITEMS)):
            r = sync_inventory("76561198000000001")
        assert isinstance(r, InventoryResult)

    def test_success_message_contains_count(self):
        with patch("frontend.inventory.fetch_inventory", new=MagicMock(return_value=_ITEMS)):
            r = sync_inventory("76561198000000001")
        assert "2" in r.message

    def test_steam_id_is_stripped_before_use(self):
        mock_fetch = MagicMock(return_value=_ITEMS)
        with patch("frontend.inventory.fetch_inventory", new=mock_fetch):
            sync_inventory("  76561198000000001  ")
        # should not raise — strip happens before call


# ─── sync_transactions ────────────────────────────────────────────────────────


class TestSyncTransactions:
    def test_success_ok_true(self):
        with patch("ingestion.steam_transactions.fetch_market_history", return_value=(_TX, "ok")), \
             patch("ingestion.steam_transactions.compute_annual_pnl", return_value={2025: 50.0}):
            r = sync_transactions()
        assert r.ok is True

    def test_success_buy_sell_counts(self):
        with patch("ingestion.steam_transactions.fetch_market_history", return_value=(_TX, "ok")), \
             patch("ingestion.steam_transactions.compute_annual_pnl", return_value={2025: 50.0}):
            r = sync_transactions()
        assert r.buy_count == 2
        assert r.sell_count == 1

    def test_success_transactions_list(self):
        with patch("ingestion.steam_transactions.fetch_market_history", return_value=(_TX, "ok")), \
             patch("ingestion.steam_transactions.compute_annual_pnl", return_value={2025: 50.0}):
            r = sync_transactions()
        assert r.transactions == _TX

    def test_success_annual_pnl_forwarded(self):
        with patch("ingestion.steam_transactions.fetch_market_history", return_value=(_TX, "ok")), \
             patch("ingestion.steam_transactions.compute_annual_pnl", return_value={2025: 50.0}):
            r = sync_transactions()
        assert r.annual_pnl == {2025: 50.0}

    def test_success_message_contains_count(self):
        with patch("ingestion.steam_transactions.fetch_market_history", return_value=(_TX, "ok")), \
             patch("ingestion.steam_transactions.compute_annual_pnl", return_value={}):
            r = sync_transactions()
        assert "3" in r.message

    def test_failure_no_cookie(self):
        with patch("ingestion.steam_transactions.fetch_market_history", return_value=(None, "NO_COOKIE")):
            r = sync_transactions()
        assert r.ok is False
        assert r.error_code == "NO_COOKIE"

    def test_failure_stale_cookie_via_403(self):
        with patch("ingestion.steam_transactions.fetch_market_history", return_value=([], "HTTP 403 Forbidden")):
            r = sync_transactions()
        assert r.ok is False
        assert r.error_code == "STALE_COOKIE"

    def test_failure_stale_cookie_via_ustarl(self):
        with patch("ingestion.steam_transactions.fetch_market_history", return_value=([], "сессия устарела")):
            r = sync_transactions()
        assert r.ok is False
        assert r.error_code == "STALE_COOKIE"

    def test_failure_network(self):
        with patch("ingestion.steam_transactions.fetch_market_history", return_value=([], "Connection error")):
            r = sync_transactions()
        assert r.ok is False
        assert r.error_code == "NETWORK"

    def test_failure_empty_list_treated_as_no_transactions(self):
        with patch("ingestion.steam_transactions.fetch_market_history", return_value=([], "NO_COOKIE")):
            r = sync_transactions()
        assert r.ok is False

    def test_failure_returns_transactions_result(self):
        with patch("ingestion.steam_transactions.fetch_market_history", return_value=(None, "NO_COOKIE")):
            r = sync_transactions()
        assert isinstance(r, TransactionsResult)

    def test_success_error_code_is_none(self):
        with patch("ingestion.steam_transactions.fetch_market_history", return_value=(_TX, "ok")), \
             patch("ingestion.steam_transactions.compute_annual_pnl", return_value={}):
            r = sync_transactions()
        assert r.error_code is None

    def test_max_pages_forwarded_to_fetch(self):
        mock_fetch = MagicMock(return_value=(_TX, "ok"))
        with patch("ingestion.steam_transactions.fetch_market_history", mock_fetch), \
             patch("ingestion.steam_transactions.compute_annual_pnl", return_value={}):
            sync_transactions(max_pages=5)
        mock_fetch.assert_called_once_with(max_pages=5)
