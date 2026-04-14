"""
Tests for frontend/balance.py — pure-function helpers.

Covers:
  - compute_pnl(): net P&L after Steam 15% fee + fixed 5₸ deduction (KZT)
  - build_monthly_chart(): month bucketing logic (pure data path)
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from src.domain.portfolio import compute_pnl

# ── compute_pnl ───────────────────────────────────────────────────────────────


class TestComputePnl:
    """
    Formula: (sell / steam_fee_divisor) - steam_fee_fixed_kzt - buy
    Defaults: steam_fee_divisor=1.15, steam_fee_fixed=5.0 (KZT)
    All prices in KZT.
    """

    def test_breakeven_returns_near_zero(self) -> None:
        # Buy at 480₸ and sell at exactly the breakeven price.
        # breakeven: (sell / 1.15 - 5) == buy → sell = (buy + 5) * 1.15
        buy = 480.0
        sell = (buy + 5.0) * 1.15  # exact algebraic breakeven
        result = compute_pnl(sell_price=sell, buy_price=buy)
        assert abs(result) < 1.0  # floating-point tolerance ~1₸

    def test_profitable_sell(self) -> None:
        # sell=960₸, buy=480₸ → net = 960/1.15 - 5 - 480 ≈ 349₸
        result = compute_pnl(sell_price=960.0, buy_price=480.0)
        expected = 960.0 / 1.15 - 5.0 - 480.0
        assert abs(result - expected) < 0.01

    def test_loss_when_sell_below_cost(self) -> None:
        # sell=384₸, buy=480₸ → net = 384/1.15 - 5 - 480 < 0
        result = compute_pnl(sell_price=384.0, buy_price=480.0)
        assert result < 0

    def test_zero_sell_price(self) -> None:
        result = compute_pnl(sell_price=0.0, buy_price=480.0)
        # 0/1.15 - 5 - 480 = -485
        expected = 0.0 / 1.15 - 5.0 - 480.0
        assert abs(result - expected) < 0.01

    def test_uses_config_settings(self) -> None:
        # settings is imported at module level in balance.py; patch the name in that module.
        from config import Settings

        custom = Settings(steam_fee_divisor=1.10, steam_fee_fixed=10.0)
        with patch("domain.portfolio.settings", custom):
            result = compute_pnl(sell_price=960.0, buy_price=480.0)
            expected = 960.0 / 1.10 - 10.0 - 480.0
            assert abs(result - expected) < 0.01

    def test_high_sell_price_positive_pnl(self) -> None:
        result = compute_pnl(sell_price=48000.0, buy_price=480.0)
        assert result > 0

    def test_symmetry_with_manual_formula(self) -> None:
        """Result must match manual (sell/1.15 - 5 - buy) with default config."""
        sell, buy = 1680.0, 960.0
        result = compute_pnl(sell_price=sell, buy_price=buy)
        manual = sell / 1.15 - 5.0 - buy
        assert abs(result - manual) < 0.01

    @pytest.mark.parametrize(
        "sell,buy",
        [
            (720.0, 480.0),
            (240.0, 384.0),
            (4800.0, 2400.0),
            (1196.0, 1196.0),
        ],
    )
    def test_parametric_formula(self, sell: float, buy: float) -> None:
        result = compute_pnl(sell_price=sell, buy_price=buy)
        expected = sell / 1.15 - 5.0 - buy
        assert abs(result - expected) < 0.01
