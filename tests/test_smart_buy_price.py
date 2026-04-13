"""
Unit tests for domain.services.compute_smart_buy_price (PV-11).

Formula:
    net_proceeds    = sell_price / fee_divisor - fee_fixed
    smart_buy_price = net_proceeds / (1 + min_margin)

Returns Amount(0) when net_proceeds <= 0 (fixed fee dominates).
"""

from __future__ import annotations

from domain.services import compute_smart_buy_price
from domain.value_objects import Amount

# ─── Default fee parameters (mirror config.py defaults) ──────────────────────

_FEE_DIV = 1.15       # Steam 15 % fee
_FEE_FIXED = Amount(5)  # Steam fixed per-transaction fee
_MARGIN = 0.05        # 5 % minimum net margin


def _sbp(sell: float, *, fee_div: float = _FEE_DIV, fee_fixed: float = 5.0, margin: float = _MARGIN) -> float:
    """Helper: call compute_smart_buy_price and return .amount float."""
    return compute_smart_buy_price(
        Amount(sell),
        fee_divisor=fee_div,
        fee_fixed=Amount(fee_fixed),
        min_margin=margin,
    ).amount


# ─── Return type ─────────────────────────────────────────────────────────────


class TestReturnType:
    def test_returns_kzt_instance(self):
        result = compute_smart_buy_price(Amount(1000), fee_divisor=1.15, fee_fixed=Amount(5), min_margin=0.05)
        assert isinstance(result, Amount)

    def test_amount_is_rounded(self):
        """KZT.amount is always rounded to the nearest unit (round-half-up), stored as float."""
        result = compute_smart_buy_price(Amount(1000), fee_divisor=1.15, fee_fixed=Amount(5), min_margin=0.05)
        assert result.amount == round(result.amount)


# ─── Standard cases ───────────────────────────────────────────────────────────


class TestStandardCases:
    def test_typical_case(self):
        # sell=1000, net=1000/1.15-5=864.565, sbp=864.565/1.05=823.395 → Amount(823)
        assert _sbp(1000) == 823

    def test_higher_sell_price(self):
        # sell=5000, net=5000/1.15-5=4347.826-5=4342.826, sbp=4342.826/1.05=4136.025 → Amount(4136)
        assert _sbp(5000) == 4136

    def test_zero_margin_returns_net_proceeds(self):
        # margin=0 → sbp = net_proceeds
        # net = 1000/1.15-5 = 864.565 → Amount(865)
        assert _sbp(1000, margin=0.0) == 865

    def test_twenty_percent_margin(self):
        # sell=1000, net=864.565, sbp=864.565/1.20=720.471 → Amount(720)
        assert _sbp(1000, margin=0.20) == 720

    def test_result_strictly_below_net_proceeds(self):
        """With min_margin > 0, smart_buy_price must be less than net proceeds."""
        net = 1000 / 1.15 - 5  # ~864.565
        sbp = _sbp(1000, margin=0.05)
        assert sbp < net

    def test_profit_after_fees_meets_margin(self):
        """Buying at smart_buy_price and selling at sell_price yields >= min_margin."""
        sell = 2000.0
        sbp = _sbp(sell, margin=0.05)
        net = sell / _FEE_DIV - _FEE_FIXED.amount
        # margin achieved: (net - sbp) / sbp >= 0.05 (minus rounding)
        achieved = (net - sbp) / sbp
        assert achieved >= 0.04  # allow 1 % rounding slack


# ─── Fee-dominated (low price) edge cases ────────────────────────────────────


class TestFeeDominatedEdgeCases:
    def test_sell_price_equals_fee_times_divisor_returns_zero(self):
        # net = fee_fixed * fee_div / fee_div - fee_fixed = fee_fixed - fee_fixed = 0
        # sell = fee_fixed * fee_div = 5 * 1.15 = 5.75 → net = 5/1.15*1.15 - 5 = 5-5 = 0
        # Actually: sell=5.75 → net = 5.75/1.15 - 5 = 5 - 5 = 0 → Amount(0)
        assert _sbp(5.75) == 0

    def test_very_low_sell_price_returns_zero(self):
        # sell=1, net=1/1.15-5≈-4.13 → Amount(0)
        assert _sbp(1.0) == 0

    def test_sell_price_zero_returns_zero(self):
        assert _sbp(0.0) == 0

    def test_just_above_breakeven(self):
        # sell=10, net=10/1.15-5=8.695-5=3.695>0 → sbp=3.695/1.05=3.519 → Amount(4)
        result = _sbp(10.0)
        assert result > 0

    def test_high_fixed_fee_dominates(self):
        # Custom: fee_fixed=100, sell=50 → net=50/1.15-100<0 → Amount(0)
        assert _sbp(50.0, fee_fixed=100.0) == 0


# ─── Margin boundary cases ────────────────────────────────────────────────────


class TestMarginBoundaries:
    def test_min_margin_one_hundred_pct(self):
        # margin=1.0 (100 %) → sbp = net/2 = 864.565/2 = 432.28 → Amount(432)
        assert _sbp(1000, margin=1.0) == 432

    def test_small_margin_close_to_net(self):
        # margin=0.001 → sbp ≈ net ≈ 864.565/1.001 ≈ 863.7 → Amount(864)
        result = _sbp(1000, margin=0.001)
        assert result == 864

    def test_different_fee_divisor(self):
        # fee_div=1.10 (10 % fee), sell=1000, net=1000/1.10-5=909.09-5=904.09, sbp=904.09/1.05=861.04 → Amount(861)
        assert _sbp(1000, fee_div=1.10) == 861


# ─── Monotonicity invariants ──────────────────────────────────────────────────


class TestMonotonicity:
    def test_higher_sell_price_yields_higher_sbp(self):
        assert _sbp(500) < _sbp(1000) < _sbp(2000)

    def test_higher_margin_yields_lower_sbp(self):
        assert _sbp(1000, margin=0.20) < _sbp(1000, margin=0.10) < _sbp(1000, margin=0.05)

    def test_higher_fixed_fee_yields_lower_sbp(self):
        assert _sbp(1000, fee_fixed=20.0) < _sbp(1000, fee_fixed=10.0) < _sbp(1000, fee_fixed=5.0)
