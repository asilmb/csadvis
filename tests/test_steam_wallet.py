"""
Tests for ingestion/steam_wallet.py — balance parsing and persistence.

Covers:
  - _parse_amount: various KZT string formats
  - get_saved_balance / save_balance: round-trip via Redis mock
  - fetch_wallet_balance: mocked HTTP responses
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from scrapper.steam_wallet import (
    _parse_amount,
    fetch_wallet_balance,
    get_saved_balance,
    save_balance,
)

# ── _parse_amount ─────────────────────────────────────────────────────────


class TestParseKztAmount:
    def test_plain_number(self) -> None:
        assert _parse_amount("12345") == pytest.approx(12345.0)

    def test_kzt_symbol_stripped(self) -> None:
        assert _parse_amount("12 345 ₸") == pytest.approx(12345.0)

    def test_space_thousands_separator(self) -> None:
        assert _parse_amount("1 234 567") == pytest.approx(1234567.0)

    def test_comma_decimal(self) -> None:
        assert _parse_amount("12 345,50") == pytest.approx(12345.50)

    def test_dot_decimal(self) -> None:
        assert _parse_amount("12345.99") == pytest.approx(12345.99)

    def test_empty_string_returns_none(self) -> None:
        assert _parse_amount("") is None

    def test_symbol_only_returns_none(self) -> None:
        assert _parse_amount("₸") is None

    def test_zero_returns_zero(self) -> None:
        assert _parse_amount("0") == pytest.approx(0.0)

    def test_ruble_symbol_stripped(self) -> None:
        assert _parse_amount("500 ₽") == pytest.approx(500.0)


# ── save / get round-trip ─────────────────────────────────────────────────────


def _mock_redis(stored: dict | None = None) -> MagicMock:
    """Return a fake Redis client backed by an in-memory dict."""
    store: dict = stored or {}
    r = MagicMock()
    r.get.side_effect = lambda key: store.get(key)
    r.set.side_effect = lambda key, value: store.update({key: value})
    return r


class TestBalancePersistence:
    def test_round_trip(self) -> None:
        redis = _mock_redis()
        with patch("scrapper.steam_wallet.get_redis", return_value=redis):
            save_balance(50000.0)
            assert get_saved_balance() == pytest.approx(50000.0)

    def test_returns_none_when_key_missing(self) -> None:
        redis = _mock_redis()
        with patch("scrapper.steam_wallet.get_redis", return_value=redis):
            assert get_saved_balance() is None

    def test_returns_none_on_redis_error(self) -> None:
        redis = MagicMock()
        redis.get.side_effect = Exception("Redis unavailable")
        with patch("scrapper.steam_wallet.get_redis", return_value=redis):
            assert get_saved_balance() is None

    def test_saves_correct_value(self) -> None:
        redis = _mock_redis()
        with patch("scrapper.steam_wallet.get_redis", return_value=redis):
            save_balance(99999.0)
        redis.set.assert_called_once_with("cs2:wallet:balance", "99999.0")


# ── fetch_wallet_balance ─────────────────────────────────────────────────


def _mock_response(html: str, status: int = 200) -> MagicMock:
    r = MagicMock()
    r.status_code = status
    # Inject g_steamID so the auth check in fetch_wallet_balance passes.
    # Only inject for non-error responses that contain actual HTML content.
    if status == 200 and html:
        r.text = 'var g_steamID = "76561198000000001";' + html
    else:
        r.text = html
    return r


def _settings_with_cookie(cookie: str = "valid_cookie") -> MagicMock:
    s = MagicMock()
    s.steam_login_secure = cookie
    return s


class TestFetchWalletBalance:
    def test_no_cookie_returns_none(self) -> None:
        with patch("scrapper.steam_wallet.settings", _settings_with_cookie("")):
            balance, msg = fetch_wallet_balance()
        assert balance is None
        assert msg == "NO_COOKIE"

    def test_parses_market_wallet_span(self) -> None:
        html = '<span id="marketWalletBalanceAmount">12 345 ₸</span>'
        with (
            patch("scrapper.steam_wallet.settings", _settings_with_cookie()),
            patch("scrapper.steam_wallet.httpx.get", return_value=_mock_response(html)),
        ):
            balance, msg = fetch_wallet_balance()
        assert balance == pytest.approx(12345.0)
        assert "12" in msg

    def test_parses_large_balance(self) -> None:
        html = '<span id="marketWalletBalanceAmount">1 234 567 ₸</span>'
        with (
            patch("scrapper.steam_wallet.settings", _settings_with_cookie()),
            patch("scrapper.steam_wallet.httpx.get", return_value=_mock_response(html)),
        ):
            balance, _msg = fetch_wallet_balance()
        assert balance == pytest.approx(1234567.0)

    def test_http_403_returns_none(self) -> None:
        with (
            patch("scrapper.steam_wallet.settings", _settings_with_cookie()),
            patch("scrapper.steam_wallet.httpx.get", return_value=_mock_response("", 403)),
        ):
            balance, msg = fetch_wallet_balance()
        assert balance is None
        assert "403" in msg

    def test_element_not_found_raises_auth_error(self) -> None:
        from scrapper.steam_wallet import AuthError

        html = "<html><body>Not logged in</body></html>"
        with (
            patch("scrapper.steam_wallet.settings", _settings_with_cookie()),
            patch("scrapper.steam_wallet.httpx.get", return_value=_mock_response(html)),
        ):
            with pytest.raises(AuthError, match="Balance element not found"):
                fetch_wallet_balance()

    def test_network_error_returns_none(self) -> None:
        with (
            patch("scrapper.steam_wallet.settings", _settings_with_cookie()),
            patch("scrapper.steam_wallet.httpx.get", side_effect=Exception("Connection refused")),
        ):
            balance, msg = fetch_wallet_balance()
        assert balance is None
        assert msg  # some error message returned

    def test_double_quoted_id_attribute(self) -> None:
        html = "<span id='marketWalletBalanceAmount'>99 000 ₸</span>"
        with (
            patch("scrapper.steam_wallet.settings", _settings_with_cookie()),
            patch("scrapper.steam_wallet.httpx.get", return_value=_mock_response(html)),
        ):
            balance, _msg = fetch_wallet_balance()
        assert balance == pytest.approx(99000.0)
