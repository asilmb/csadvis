"""
Unit tests for PV-17: SignalHandler DB logging + webhook integration.

Handlers are called directly via notify_* module-level functions.

Covers:
  notify_super_deal:
    - calls log_event with level="INFO"
    - calls dispatch with event="super_deal"

  notify_liquidity_warning:
    - calls log_event with level="WARNING"
    - does NOT call dispatch

  notify_price_alert:
    - calls log_event with level="INFO"
    - does NOT call dispatch

  notify_auth_error:
    - calls log_event with level="ERROR"
    - calls dispatch with event="auth_error" and status_code
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import patch

import pytest

from domain.events import AuthError, LiquidityWarning, PriceAlert, SuperDealDetected


def _now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


class TestSignalHandlerPV17:
    # ── notify_super_deal ──────────────────────────────────────────────────────

    def test_super_deal_calls_log_event(self):
        with patch("infra.signal_handler.log_event") as mock_log, \
             patch("infra.signal_handler.dispatch"):
            from infra.signal_handler import notify_super_deal
            notify_super_deal(SuperDealDetected(
                timestamp=_now(),
                item_name="Recoil Case",
                payload={"buy_price": 5000, "target_exit_price": 6500,
                         "stop_loss_price": 4500, "expected_margin_pct": 25.0},
            ))
        mock_log.assert_called_once()
        level, module, msg = mock_log.call_args.args
        assert level == "INFO"
        assert "SUPER DEAL" in msg
        assert "Recoil Case" in msg

    def test_super_deal_calls_dispatch(self):
        with patch("infra.signal_handler.log_event"), \
             patch("infra.signal_handler.dispatch") as mock_dispatch:
            from infra.signal_handler import notify_super_deal
            notify_super_deal(SuperDealDetected(
                timestamp=_now(),
                item_name="Fracture Case",
                payload={"buy_price": 4000, "target_exit_price": 5200,
                         "stop_loss_price": 3600, "expected_margin_pct": 22.0},
            ))
        mock_dispatch.assert_called_once()
        payload = mock_dispatch.call_args.args[0]
        assert payload["event"] == "super_deal"
        assert payload["item"] == "Fracture Case"

    # ── notify_liquidity_warning ───────────────────────────────────────────────

    def test_liquidity_warning_calls_log_event(self):
        with patch("infra.signal_handler.log_event") as mock_log, \
             patch("infra.signal_handler.dispatch"):
            from infra.signal_handler import notify_liquidity_warning
            notify_liquidity_warning(LiquidityWarning(
                timestamp=_now(), item_name="Danger Zone Case", payload="thin volume"
            ))
        mock_log.assert_called_once()
        level, _, msg = mock_log.call_args.args
        assert level == "WARNING"
        assert "LIQUIDITY" in msg

    def test_liquidity_warning_no_dispatch(self):
        with patch("infra.signal_handler.log_event"), \
             patch("infra.signal_handler.dispatch") as mock_dispatch:
            from infra.signal_handler import notify_liquidity_warning
            notify_liquidity_warning(LiquidityWarning(
                timestamp=_now(), item_name="Danger Zone Case", payload="thin volume"
            ))
        mock_dispatch.assert_not_called()

    # ── notify_price_alert ─────────────────────────────────────────────────────

    def test_price_alert_calls_log_event(self):
        with patch("infra.signal_handler.log_event") as mock_log, \
             patch("infra.signal_handler.dispatch"):
            from infra.signal_handler import notify_price_alert
            notify_price_alert(PriceAlert(
                timestamp=_now(), item_name="Horizon Case",
                payload={"current_price": 3000.0, "threshold": 3500.0, "direction": "below"},
            ))
        mock_log.assert_called_once()
        level, _, msg = mock_log.call_args.args
        assert level == "INFO"
        assert "PRICE ALERT" in msg

    def test_price_alert_no_dispatch(self):
        with patch("infra.signal_handler.log_event"), \
             patch("infra.signal_handler.dispatch") as mock_dispatch:
            from infra.signal_handler import notify_price_alert
            notify_price_alert(PriceAlert(
                timestamp=_now(), item_name="Horizon Case",
                payload={"current_price": 3000.0, "threshold": 3500.0, "direction": "below"},
            ))
        mock_dispatch.assert_not_called()

    # ── notify_auth_error ──────────────────────────────────────────────────────

    def test_auth_error_calls_log_event_error(self):
        with patch("infra.signal_handler.log_event") as mock_log, \
             patch("infra.signal_handler.dispatch"):
            from infra.signal_handler import notify_auth_error
            notify_auth_error(AuthError(
                timestamp=_now(),
                item_name="Recoil Case",
                status_code=403,
                payload="Steam Market HTTP 403 for Recoil Case",
            ))
        mock_log.assert_called_once()
        level, _, msg = mock_log.call_args.args
        assert level == "ERROR"
        assert "403" in msg
        assert "Recoil Case" in msg

    def test_auth_error_calls_dispatch(self):
        with patch("infra.signal_handler.log_event"), \
             patch("infra.signal_handler.dispatch") as mock_dispatch:
            from infra.signal_handler import notify_auth_error
            notify_auth_error(AuthError(
                timestamp=_now(),
                item_name="Fracture Case",
                status_code=401,
                payload="Steam Market HTTP 401 for Fracture Case",
            ))
        mock_dispatch.assert_called_once()
        payload = mock_dispatch.call_args.args[0]
        assert payload["event"] == "auth_error"
        assert payload["status_code"] == 401
        assert payload["item"] == "Fracture Case"

    def test_auth_error_403_also_dispatches(self):
        with patch("infra.signal_handler.log_event"), \
             patch("infra.signal_handler.dispatch") as mock_dispatch:
            from infra.signal_handler import notify_auth_error
            notify_auth_error(AuthError(
                timestamp=_now(),
                item_name="CS20 Case",
                status_code=403,
                payload="Steam Market HTTP 403 for CS20 Case",
            ))
        mock_dispatch.assert_called_once()
        assert mock_dispatch.call_args.args[0]["status_code"] == 403
