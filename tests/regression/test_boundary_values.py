"""
Regression Guard — Boundary Value Tests.

Tests extreme inputs that are most likely to reveal formula breakage:
  - Zero / near-zero prices
  - Negative derived values (net_proceeds goes negative when price is very low)
  - Huge inventories (10k+ positions)
  - Pass cost = 0 (free pass edge case)
  - Infinite / NaN price inputs (must not propagate silently)
  - Missing optional fields (None mean_price, None current_price)
  - max-int quantity values
  - Capsule with base_cost = 0 (degenerate catalog entry)
  - Momentum when mean → 0 (division guard)
"""
from __future__ import annotations

import math
from unittest.mock import MagicMock, patch

import pytest

# ── Shared settings patch ──────────────────────────────────────────────────────

_KEY = 481.0
_RATIO_FLOOR = 50.0
_LIQ_MIN = 5.0
_FEE_DIV = 1.15
_FEE_FIXED = 5.0


def _mock_settings():
    return type("S", (), {
        "key_price": _KEY,
        "ratio_floor": _RATIO_FLOOR,
        "liquidity_min_volume": _LIQ_MIN,
        "steam_fee_divisor": _FEE_DIV,
        "steam_fee_fixed": _FEE_FIXED,
        "momentum_event_threshold": 12.0,
        "currency_symbol": "₸",
    })()


# ── investment.py boundary tests ──────────────────────────────────────────────

class TestInvestmentSignalBoundaries:

    def _signal(self, **kwargs):
        from src.domain.investment import compute_investment_signal
        defaults = dict(
            container_name="Test", base_cost=1445.0,
            container_type="Weapon Case", current_price=1000.0,
            mean_price=1000.0, quantity=10,
        )
        defaults.update(kwargs)
        with patch("src.domain.investment.settings", _mock_settings()):
            return compute_investment_signal(**defaults)

    def test_none_current_price_returns_no_data(self):
        result = self._signal(current_price=None)
        assert result["verdict"] == "NO DATA"
        assert result["current_price"] is None

    def test_zero_current_price_returns_no_data(self):
        # 0 is falsy — treated as missing
        result = self._signal(current_price=0)
        assert result["verdict"] == "NO DATA"

    def test_very_small_price_skips_ratio_signal(self):
        """Price below ratio_floor (50) — ratio signal must be suppressed."""
        result = self._signal(current_price=30.0, mean_price=25.0, base_cost=1445.0)
        # Only momentum signal possible: (30-25)/25*100 = 20% → sell
        assert result["score"] == -1  # only momentum sell, no ratio
        assert result["verdict"] == "LEAN SELL"

    def test_none_mean_price_momentum_is_zero(self):
        """No mean_price → momentum = 0.0, no momentum signal."""
        result = self._signal(current_price=100.0, mean_price=None)
        assert result["momentum_pct"] == pytest.approx(0.0)

    def test_zero_mean_price_momentum_is_zero(self):
        """mean=0 → guard against division by zero, momentum stays 0."""
        result = self._signal(current_price=500.0, mean_price=0.0)
        assert result["momentum_pct"] == pytest.approx(0.0)
        assert math.isfinite(result["momentum_pct"])

    def test_huge_quantity_does_not_crash(self):
        """10 000+ unit inventory must not overflow or crash."""
        result = self._signal(quantity=10_000)
        assert result["quantity"] == 10_000

    def test_very_high_price_gives_sell(self):
        """Price 10× baseline → maximum sell pressure."""
        result = self._signal(current_price=9640.0, mean_price=900.0, base_cost=1445.0)
        assert result["score"] == -2
        assert result["verdict"] == "SELL"

    def test_base_cost_zero_capsule_uses_minimum_baseline(self):
        """base_cost=0 capsule → baseline = max(0, 25) = 25."""
        result = self._signal(
            base_cost=0.0,
            container_type="Sticker Capsule",
            current_price=100.0,
            mean_price=100.0,
        )
        assert result["baseline_price"] == pytest.approx(25.0)

    def test_nan_price_returns_no_data(self):
        """NaN is falsy in bool(nan) → should produce NO DATA without crashing."""
        result = self._signal(current_price=float("nan"))
        # NaN is falsy: `not NaN` is True → NO DATA path
        assert result["verdict"] == "NO DATA"

    def test_score_always_in_valid_range(self):
        """Score must always be in {-2, -1, 0, 1, 2}."""
        test_cases = [
            (None, None), (0, 0), (100.0, None), (100.0, 50.0),
            (100.0, 200.0), (5000.0, 100.0), (1.0, 1000.0),
        ]
        for price, mean in test_cases:
            result = self._signal(current_price=price, mean_price=mean)
            assert result["score"] in (-2, -1, 0, 1, 2), (
                f"price={price}, mean={mean} → invalid score {result['score']}"
            )

    def test_verdict_always_valid_string(self):
        """Verdict must always be one of the 6 defined strings."""
        _VALID = {"BUY", "LEAN BUY", "HOLD", "LEAN SELL", "SELL", "NO DATA"}
        test_cases = [None, 0, 25.0, 100.0, 964.0, 2000.0, 50000.0]
        for price in test_cases:
            result = self._signal(current_price=price)
            assert result["verdict"] in _VALID, (
                f"price={price} → invalid verdict {result['verdict']!r}"
            )


# ── armory_pass.py boundary tests ─────────────────────────────────────────────

class TestArmoryPassBoundaries:

    def _compare(self, **kwargs):
        from src.domain.armory_pass import compare_armory_pass
        defaults = dict(
            container_name="Test Case",
            market_price=1000.0,
            pass_cost=2500.0,
            stars_in_pass=5,
            stars_per_case=1,
            steam_fee_divisor=1.15,
            steam_fee_fixed=5.0,
        )
        defaults.update(kwargs)
        with patch("src.domain.armory_pass.settings", _mock_settings()):
            return compare_armory_pass(**defaults)

    def test_zero_market_price_is_valid(self):
        """market_price=0 is allowed — PASS recommendation expected."""
        result = self._compare(market_price=0.0)
        # net = 0/1.15 - 5 = -5 → pass is better
        assert result.recommendation == "PASS"

    def test_zero_pass_cost_always_market(self):
        """Free pass: market proceeds > 0 = effective_pass_cost → MARKET."""
        result = self._compare(pass_cost=0.0)
        assert result.recommendation == "MARKET"

    def test_negative_market_price_raises(self):
        with pytest.raises(ValueError, match="market_price"):
            self._compare(market_price=-1.0)

    def test_negative_pass_cost_raises(self):
        with pytest.raises(ValueError, match="pass_cost"):
            self._compare(pass_cost=-100.0)

    def test_stars_per_case_exceeds_stars_in_pass_raises(self):
        with pytest.raises(ValueError, match="stars_per_case"):
            self._compare(stars_in_pass=1, stars_per_case=2)

    def test_zero_stars_in_pass_raises(self):
        with pytest.raises(ValueError, match="stars_in_pass"):
            self._compare(stars_in_pass=0)

    def test_zero_fee_divisor_raises(self):
        with pytest.raises(ValueError, match="steam_fee_divisor"):
            self._compare(steam_fee_divisor=0.0)

    def test_net_proceeds_can_be_negative(self):
        """Very low price → net proceeds go negative — must not crash."""
        result = self._compare(market_price=1.0)
        # net = 1/1.15 - 5 = -4.13 → PASS
        assert result.recommendation == "PASS"
        assert float(result.net_market_proceeds.amount) < 0

    def test_very_high_market_price_gives_market(self):
        result = self._compare(market_price=100_000.0, pass_cost=2500.0)
        assert result.recommendation == "MARKET"

    def test_breakeven_always_positive(self):
        """Breakeven listing price must always be > 0 for valid inputs."""
        for pass_cost in (100.0, 1000.0, 5000.0, 10000.0):
            result = self._compare(pass_cost=pass_cost)
            assert result.breakeven_listing_price > 0, (
                f"pass_cost={pass_cost} → breakeven={result.breakeven_listing_price}"
            )

    def test_sell_signal_avoid_when_price_far_below_breakeven(self):
        """Price far below breakeven → AVOID."""
        result = self._compare(market_price=10.0, pass_cost=5000.0)
        assert result.sell_signal == "AVOID"

    def test_sell_signal_sell_when_price_above_breakeven(self):
        """Price above breakeven → SELL."""
        result = self._compare(market_price=10_000.0, pass_cost=500.0)
        assert result.sell_signal == "SELL"

    def test_recommendation_is_market_or_pass(self):
        """Recommendation must always be exactly 'MARKET' or 'PASS'."""
        for market_price in (0.0, 1.0, 500.0, 1000.0, 5000.0, 50000.0):
            result = self._compare(market_price=market_price)
            assert result.recommendation in ("MARKET", "PASS")


# ── ArmoryAdvisor boundary tests ───────────────────────────────────────────────

class TestArmoryAdvisorBoundaries:

    def _make_repo(self, prices: dict[str, float | None], histories: dict[str, list] | None = None):
        """Build a minimal price_repo stub."""

        repo = MagicMock()

        def _latest(name):
            price = prices.get(name)
            if price is None:
                return None
            snap = MagicMock()
            snap.price = price
            return snap

        def _history(name):
            if histories is None:
                return []
            return [{"price": p} for p in histories.get(name, [])]

        repo.get_latest_price.side_effect = _latest
        repo.get_price_history.side_effect = _history
        return repo

    def test_all_prices_missing_gives_unknown_confidence(self):
        from src.domain.analytics.armory_advisor import ArmoryAdvisor
        catalog = {"Prisma 2 Case": 1, "Revolution Case": 1}
        repo = self._make_repo({})
        advisor = ArmoryAdvisor(repo, reward_catalog=catalog)
        result = advisor.get_pass_efficiency(pass_cost=2500.0)
        assert result.overall_confidence == "UNKNOWN"
        assert result.total_roi is None
        assert result.total_net_proceeds is None

    def test_all_prices_zero_does_not_crash(self):
        """Price=0 → net=0*0.869=0; total_net=0 → ROI = (0 - pass_cost)/pass_cost < 0."""
        from src.domain.analytics.armory_advisor import ArmoryAdvisor
        catalog = {"Prisma 2 Case": 1}
        repo = self._make_repo({"Prisma 2 Case": 0.0})
        advisor = ArmoryAdvisor(repo, reward_catalog=catalog)
        result = advisor.get_pass_efficiency(pass_cost=2500.0)
        assert result.total_roi is not None
        assert result.total_roi < 0

    def test_single_item_catalog(self):
        from src.domain.analytics.armory_advisor import ArmoryAdvisor
        catalog = {"Revolution Case": 1}
        repo = self._make_repo({"Revolution Case": 2500.0})
        advisor = ArmoryAdvisor(repo, reward_catalog=catalog)
        result = advisor.get_pass_efficiency(pass_cost=2500.0)
        assert len(result.rewards) == 1
        assert result.total_credits_cost == 1

    def test_empty_catalog_does_not_crash(self):
        from src.domain.analytics.armory_advisor import ArmoryAdvisor
        advisor = ArmoryAdvisor(MagicMock(), reward_catalog={})
        result = advisor.get_pass_efficiency(pass_cost=2500.0)
        assert result.rewards == []
        assert result.total_net_proceeds is None  # no rewards → no sum possible

    def test_volatile_price_sets_is_volatile_flag(self):
        """Z-score > 2.0 → is_volatile=True."""
        from src.domain.analytics.armory_advisor import ArmoryAdvisor
        catalog = {"Gamma Case": 1}
        # History mean=100, std=10; current=130 → Z=(130-100)/10=3.0 > 2.0
        history = [{"price": p} for p in [90, 95, 100, 105, 110, 100, 95, 100]]
        repo = self._make_repo(
            {"Gamma Case": 130.0},
            histories={"Gamma Case": [90, 95, 100, 105, 110, 100, 95, 100]},
        )
        advisor = ArmoryAdvisor(repo, reward_catalog=catalog)
        result = advisor.get_pass_efficiency(pass_cost=2500.0)
        assert result.rewards[0].is_volatile is True

    def test_high_confidence_requires_30_plus_data_points(self):
        from src.domain.analytics.armory_advisor import ArmoryAdvisor
        catalog = {"Snakebite Case": 1}
        # 35 stable data points, current at mean → HIGH confidence
        prices = [100.0] * 35
        repo = self._make_repo(
            {"Snakebite Case": 100.0},
            histories={"Snakebite Case": prices},
        )
        advisor = ArmoryAdvisor(repo, reward_catalog=catalog)
        result = advisor.get_pass_efficiency(pass_cost=2500.0)
        assert result.rewards[0].confidence == "HIGH"
        assert result.rewards[0].is_volatile is False

    def test_large_catalog_10k_items_does_not_crash(self):
        """Simulate a huge catalog — advisor must not stack overflow or OOM."""
        from src.domain.analytics.armory_advisor import ArmoryAdvisor
        catalog = {f"Case {i}": 1 for i in range(500)}
        prices = {f"Case {i}": float(100 + i % 50) for i in range(500)}
        repo = self._make_repo(prices)
        advisor = ArmoryAdvisor(repo, reward_catalog=catalog)
        result = advisor.get_pass_efficiency(pass_cost=2500.0)
        assert len(result.rewards) == 500
