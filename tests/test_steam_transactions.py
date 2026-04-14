"""
Tests for ingestion/steam_transactions.py — history parsing and P&L calculation.

Covers:
  - _parse_history_html: sell row, buy row, non-CS2 filter, date formats
  - compute_annual_pnl: single year, multi-year, empty input
  - fetch_market_history: no cookie, HTTP 403, success path (mocked)
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from scrapper.steam_transactions import (
    _parse_history_html,
    compute_annual_pnl,
    fetch_market_history,
)

# ── HTML fixtures ─────────────────────────────────────────────────────────────

_SELL_ROW = """
id="history_row_111_sell" class="market_listing_row market_recent_listing_row">
  <div class="market_listing_right_cell market_listing_gainorloss">+</div>
  <span class="market_listing_item_name" style="color: #D2D2D2;">Kilowatt Case</span>
  <span class="market_listing_game_name">Counter-Strike 2</span>
  <span title="Mar 15, 2024">Mar 15</span>
  <div class="can_combine"><span>2 450 ₸</span></div>
"""

_BUY_ROW = """
id="history_row_222_buy" class="market_listing_row market_recent_listing_row">
  <div class="market_listing_right_cell market_listing_gainorloss">-</div>
  <span class="market_listing_item_name" style="color: #D2D2D2;">Revolution Case</span>
  <span class="market_listing_game_name">Counter-Strike 2</span>
  <span title="Jan 5, 2024">Jan 5</span>
  <div class="can_combine"><span>1 800 ₸</span></div>
"""

_NON_CS2_ROW = """
id="history_row_333_sell" class="market_listing_row market_recent_listing_row">
  <div class="market_listing_right_cell market_listing_gainorloss">+</div>
  <span class="market_listing_item_name">Some Dota Item</span>
  <span class="market_listing_game_name">Dota 2</span>
  <span title="Feb 1, 2024">Feb 1</span>
  <div class="can_combine"><span>500 ₸</span></div>
"""


# ── _parse_history_html ───────────────────────────────────────────────────────


class TestParseHistoryHtml:
    def test_parses_sell_row(self) -> None:
        rows = _parse_history_html({"html": _SELL_ROW})
        assert len(rows) == 1
        r = rows[0]
        assert r["action"] == "SELL"
        assert r["item_name"] == "Kilowatt Case"
        assert r["date"] == datetime(2024, 3, 15)
        assert r["price"] == pytest.approx(2450.0)
        assert r["listing_id"] == "111"

    def test_parses_buy_row(self) -> None:
        rows = _parse_history_html({"html": _BUY_ROW})
        assert len(rows) == 1
        r = rows[0]
        assert r["action"] == "BUY"
        assert r["item_name"] == "Revolution Case"
        assert r["date"] == datetime(2024, 1, 5)
        assert r["price"] == pytest.approx(1800.0)

    def test_filters_non_cs2(self) -> None:
        rows = _parse_history_html({"html": _NON_CS2_ROW})
        assert rows == []

    def test_mixed_html(self) -> None:
        html = _SELL_ROW + _NON_CS2_ROW + _BUY_ROW
        rows = _parse_history_html({"html": html})
        assert len(rows) == 2
        actions = {r["action"] for r in rows}
        assert actions == {"SELL", "BUY"}

    def test_empty_html(self) -> None:
        assert _parse_history_html({"html": ""}) == []

    def test_no_valid_rows(self) -> None:
        assert _parse_history_html({"html": "<html><body>nothing</body></html>"}) == []


# ── compute_annual_pnl ────────────────────────────────────────────────────────


class TestComputeAnnualPnl:
    def _tx(self, year: int, action: str, amount: float) -> dict:
        return {
            "date": datetime(year, 6, 1),
            "action": action,
            "price": amount,
            "total": amount,
            "item_name": "Test Case",
            "listing_id": "0",
        }

    def test_single_sell(self) -> None:
        txs = [self._tx(2024, "SELL", 5000.0)]
        result = compute_annual_pnl(txs)
        assert result == {2024: pytest.approx(5000.0)}

    def test_buy_and_sell(self) -> None:
        txs = [self._tx(2024, "BUY", 3000.0), self._tx(2024, "SELL", 5000.0)]
        result = compute_annual_pnl(txs)
        assert result[2024] == pytest.approx(2000.0)

    def test_net_loss(self) -> None:
        txs = [self._tx(2023, "BUY", 10000.0), self._tx(2023, "SELL", 7000.0)]
        result = compute_annual_pnl(txs)
        assert result[2023] == pytest.approx(-3000.0)

    def test_multi_year(self) -> None:
        txs = [
            self._tx(2022, "SELL", 1000.0),
            self._tx(2023, "BUY", 2000.0),
            self._tx(2023, "SELL", 3000.0),
            self._tx(2024, "SELL", 4000.0),
        ]
        result = compute_annual_pnl(txs)
        assert result == {
            2022: pytest.approx(1000.0),
            2023: pytest.approx(1000.0),
            2024: pytest.approx(4000.0),
        }

    def test_empty_returns_empty_dict(self) -> None:
        assert compute_annual_pnl([]) == {}


# ── fetch_market_history ──────────────────────────────────────────────────────


def _mock_settings(cookie: str = "valid") -> MagicMock:
    s = MagicMock()
    s.steam_login_secure = cookie
    return s


def _mock_resp(data: dict, status: int = 200) -> MagicMock:
    r = MagicMock()
    r.status_code = status
    r.json.return_value = data
    return r


class TestFetchMarketHistory:
    def test_no_cookie_returns_empty(self) -> None:
        with patch("scrapper.steam_transactions.settings", _mock_settings("")):
            rows, msg = fetch_market_history()
        assert rows == []
        assert msg == "NO_COOKIE"

    def test_http_403_returns_error(self) -> None:
        with (
            patch("scrapper.steam_transactions.settings", _mock_settings()),
            patch("scrapper.steam_transactions.httpx.get", return_value=_mock_resp({}, 403)),
        ):
            rows, msg = fetch_market_history()
        assert rows == []
        assert "403" in msg

    def test_success_false_returns_error(self) -> None:
        data = {"success": False}
        with (
            patch("scrapper.steam_transactions.settings", _mock_settings()),
            patch("scrapper.steam_transactions.httpx.get", return_value=_mock_resp(data)),
        ):
            rows, msg = fetch_market_history()
        assert rows == []
        assert "success=false" in msg

    def test_parses_single_page(self) -> None:
        data = {
            "success": True,
            "total_count": 1,
            "results_html": _SELL_ROW,
        }
        with (
            patch("scrapper.steam_transactions.settings", _mock_settings()),
            patch("scrapper.steam_transactions.httpx.get", return_value=_mock_resp(data)),
        ):
            rows, msg = fetch_market_history(max_pages=1)
        assert len(rows) == 1
        assert rows[0]["action"] == "SELL"
        assert "1" in msg

    def test_network_error_returns_empty(self) -> None:
        with (
            patch("scrapper.steam_transactions.settings", _mock_settings()),
            patch("scrapper.steam_transactions.httpx.get", side_effect=Exception("timeout")),
        ):
            rows, _msg = fetch_market_history(max_pages=1)
        assert rows == []


# ── Sprint 10 regex whitespace fix ────────────────────────────────────────────


class TestGainOrLossRegexWhitespace:
    """Verify that gainorloss regex tolerates optional whitespace around +/- (Sprint 10 fix)."""

    def test_sell_with_spaces_around_plus(self) -> None:
        # Whitespace before and after + inside the gainorloss div
        html = """
id="history_row_999_sell" class="market_listing_row market_recent_listing_row">
  <div class="market_listing_right_cell market_listing_gainorloss">  +  </div>
  <span class="market_listing_item_name" style="color: #D2D2D2;">Kilowatt Case</span>
  <span class="market_listing_game_name">Counter-Strike 2</span>
  <span title="Mar 15, 2024">Mar 15</span>
  <div class="can_combine"><span>2 450 ₸</span></div>
"""
        rows = _parse_history_html({"html": html})
        assert len(rows) == 1
        assert rows[0]["action"] == "SELL"

    def test_buy_with_spaces_around_minus(self) -> None:
        html = """
id="history_row_888_buy" class="market_listing_row market_recent_listing_row">
  <div class="market_listing_right_cell market_listing_gainorloss"> - </div>
  <span class="market_listing_item_name" style="color: #D2D2D2;">Revolution Case</span>
  <span class="market_listing_game_name">Counter-Strike 2</span>
  <span title="Jan 5, 2024">Jan 5</span>
  <div class="can_combine"><span>1 800 ₸</span></div>
"""
        rows = _parse_history_html({"html": html})
        assert len(rows) == 1
        assert rows[0]["action"] == "BUY"
