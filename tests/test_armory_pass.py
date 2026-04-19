"""Tests for engine/armory_pass.py — Armory Pass vs. market comparison."""

from __future__ import annotations

import math

import pytest

from src.domain.armory_pass import ArmoryPassResult, compare_armory_pass
from src.domain.value_objects import ROI, Amount

# ─── helpers ─────────────────────────────────────────────────────────────────

# Amount reference values (~481₸/$)
# $2.00 → 960₸, $5.00 → 2400₸, $10.00 → 4800₸


def _compare(**kwargs) -> ArmoryPassResult:
    """Convenience wrapper with sensible defaults (all prices in Amount)."""
    defaults = dict(
        container_name="Test Case",
        market_price=960.0,  # ~$2.00
        pass_cost=2400.0,  # ~$5.00
        stars_in_pass=5,
        stars_per_case=1,
        steam_fee_divisor=1.15,
        steam_fee_fixed=5.0,
    )
    defaults.update(kwargs)
    return compare_armory_pass(**defaults)


# ─── basic recommendation logic ──────────────────────────────────────────────


class TestRecommendation:
    def test_market_better_when_net_proceeds_exceed_pass_cost(self) -> None:
        # pass_cost = 2400 / 5 * 1 = 480₸
        # net_market = 960 / 1.15 - 5 ≈ 830₸ > 480₸ → MARKET
        result = _compare(market_price=960.0, pass_cost=2400.0)
        assert result.recommendation == "MARKET"

    def test_pass_better_when_effective_cost_below_net_proceeds(self) -> None:
        # pass_cost = 4800 / 5 * 1 = 960₸
        # net_market = 960 / 1.15 - 5 ≈ 830₸ < 960₸ → PASS
        result = _compare(market_price=960.0, pass_cost=4800.0)
        assert result.recommendation == "PASS"

    def test_pass_better_returns_negative_margin(self) -> None:
        result = _compare(market_price=960.0, pass_cost=4800.0)
        assert result.margin_pct.value < 0.0

    def test_market_better_returns_positive_margin(self) -> None:
        result = _compare(market_price=960.0, pass_cost=2400.0)
        assert result.margin_pct.value > 0.0


# ─── derived value correctness ────────────────────────────────────────────────


class TestDerivedValues:
    def test_effective_pass_cost_formula(self) -> None:
        # pass_cost=2880₸, stars_in_pass=3, stars_per_case=1 → 2880/3*1 = 960₸
        result = _compare(pass_cost=2880.0, stars_in_pass=3, stars_per_case=1)
        assert isinstance(result.effective_pass_cost, Amount)
        assert result.effective_pass_cost.amount == pytest.approx(960.0)

    def test_effective_pass_cost_with_stars_per_case_2(self) -> None:
        # pass_cost=2880₸, stars_in_pass=6, stars_per_case=2 → 2880/6*2 = 960₸
        result = _compare(pass_cost=2880.0, stars_in_pass=6, stars_per_case=2)
        assert isinstance(result.effective_pass_cost, Amount)
        assert result.effective_pass_cost.amount == pytest.approx(960.0)

    def test_net_market_proceeds_formula(self) -> None:
        # market=1104₸, fee_div=1.15, fee_fixed=5₸ → 1104/1.15-5 = 960-5 = 955₸ (exact)
        result = _compare(market_price=1104.0, steam_fee_divisor=1.15, steam_fee_fixed=5.0)
        assert isinstance(result.net_market_proceeds, Amount)
        assert result.net_market_proceeds.amount == pytest.approx(955.0, abs=1e-6)

    def test_margin_pct_formula(self) -> None:
        # margin_pct stores ROI ratio (not percent): (net - pass) / pass
        result = _compare(market_price=960.0, pass_cost=2400.0)
        expected_net = 960.0 / 1.15 - 5.0
        expected_pass = 2400.0 / 5 * 1
        expected_ratio = (expected_net - expected_pass) / expected_pass
        assert isinstance(result.margin_pct, ROI)
        assert result.margin_pct.value == pytest.approx(expected_ratio, abs=1e-6)

    def test_result_fields_match_inputs(self) -> None:
        result = _compare(
            container_name="Revolution Case",
            market_price=1680.0,
            pass_cost=3600.0,
            stars_in_pass=5,
            stars_per_case=1,
        )
        assert result.container_name == "Revolution Case"
        assert result.market_price == pytest.approx(1680.0)
        assert result.pass_cost == pytest.approx(3600.0)
        assert result.stars_in_pass == 5
        assert result.stars_per_case == 1


# ─── message content ─────────────────────────────────────────────────────────


class TestMessage:
    def test_market_message_contains_percentage(self) -> None:
        result = _compare(market_price=960.0, pass_cost=2400.0)
        assert result.recommendation == "MARKET"
        # message should mention the margin
        assert "%" in result.message

    def test_pass_message_contains_percentage(self) -> None:
        result = _compare(market_price=960.0, pass_cost=4800.0)
        assert result.recommendation == "PASS"
        assert "%" in result.message

    def test_message_is_nonempty_string(self) -> None:
        result = _compare()
        assert isinstance(result.message, str)
        assert len(result.message) > 0


# ─── edge cases ──────────────────────────────────────────────────────────────


class TestEdgeCases:
    def test_zero_market_price_is_pass(self) -> None:
        # net_market = 0/1.15 - 5 = -5₸ < any positive pass cost → PASS
        result = _compare(market_price=0.0, pass_cost=2400.0)
        assert result.recommendation == "PASS"

    def test_stars_per_case_equals_stars_in_pass(self) -> None:
        # The entire pass is spent on one container
        result = _compare(pass_cost=2400.0, stars_in_pass=5, stars_per_case=5)
        assert result.effective_pass_cost.amount == pytest.approx(2400.0)

    def test_custom_fee_divisor(self) -> None:
        # With 10% fee divisor (1.10) net proceeds are higher
        result_low_fee = _compare(market_price=960.0, steam_fee_divisor=1.10)
        result_high_fee = _compare(market_price=960.0, steam_fee_divisor=1.15)
        assert result_low_fee.net_market_proceeds > result_high_fee.net_market_proceeds

    def test_result_is_frozen_dataclass(self) -> None:
        result = _compare()
        with pytest.raises((AttributeError, TypeError)):
            result.recommendation = "HACK"  # type: ignore[misc]

    def test_free_pass_with_positive_market_price(self) -> None:
        # pass_cost=0 → effective_pass_cost=0 → market always better (margin = inf)
        result = _compare(market_price=960.0, pass_cost=0.0)
        assert result.recommendation == "MARKET"
        assert math.isinf(result.margin_pct.value)


# ─── validation errors ────────────────────────────────────────────────────────


class TestValidation:
    def test_stars_in_pass_zero_raises(self) -> None:
        with pytest.raises(ValueError, match="stars_in_pass"):
            _compare(stars_in_pass=0)

    def test_stars_in_pass_negative_raises(self) -> None:
        with pytest.raises(ValueError, match="stars_in_pass"):
            _compare(stars_in_pass=-1)

    def test_stars_per_case_zero_raises(self) -> None:
        with pytest.raises(ValueError, match="stars_per_case"):
            _compare(stars_per_case=0)

    def test_negative_market_price_raises(self) -> None:
        with pytest.raises(ValueError, match="market_price"):
            _compare(market_price=-1.0)

    def test_negative_pass_cost_raises(self) -> None:
        with pytest.raises(ValueError, match="pass_cost"):
            _compare(pass_cost=-0.01)

    def test_stars_per_case_exceeds_stars_in_pass_raises(self) -> None:
        with pytest.raises(ValueError, match="stars_per_case"):
            _compare(stars_in_pass=3, stars_per_case=5)

    def test_fee_divisor_zero_raises(self) -> None:
        with pytest.raises(ValueError, match="steam_fee_divisor"):
            _compare(steam_fee_divisor=0.0)

    def test_fee_divisor_negative_raises(self) -> None:
        with pytest.raises(ValueError, match="steam_fee_divisor"):
            _compare(steam_fee_divisor=-1.15)
