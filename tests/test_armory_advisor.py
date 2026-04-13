"""
Unit tests for services/analytics/armory_advisor.py — ArmoryAdvisor (PV-22).

All tests are pure: PriceRepository is replaced with a lightweight fake.
No DB, no network.

Covers:
  _assess_confidence():
    - empty history → UNKNOWN, not volatile
    - < 7 points, not volatile → MEDIUM (only 1 point, std=0 → not volatile)
    - < 7 points, volatile → LOW
    - >= 7 points, volatile → MEDIUM (not high because volatile or < 30)
    - >= 30 points, not volatile → HIGH
    - >= 30 points, volatile → MEDIUM
    - Z-score calculation correctness

  _evaluate_reward():
    - no price → UNKNOWN, all numeric fields None
    - price present → net = price * 0.869
    - credit_value = net / credits_required
    - confidence + is_volatile forwarded from _assess_confidence

  get_pass_efficiency():
    - single reward, known price → total_roi computed
    - total_roi formula: (total_net - pass_cost) / pass_cost
    - any unknown price → total_roi=None, total_net=None
    - partial unknown → total_roi=None
    - pass_cost=0 → total_roi=None (guard)
    - overall_confidence = minimum confidence across rewards
    - total_credits_cost = sum of credits_required
    - returns float types (not int) for roi
"""

from __future__ import annotations

import pytest

from services.analytics.armory_advisor import (
    STEAM_NET_MULTIPLIER,
    ArmoryAdvisor,
    PassEfficiencyResult,
    RewardEvaluation,
)

# ─── Fake PriceRepository ─────────────────────────────────────────────────────


class _FakeSnapshot:
    def __init__(self, price: float):
        self.price = price


class FakePriceRepo:
    """Minimal fake — inject via constructor."""

    def __init__(
        self,
        prices: dict[str, float] | None = None,
        histories: dict[str, list[dict]] | None = None,
    ) -> None:
        self._prices = prices or {}
        self._histories = histories or {}

    def get_latest_price(self, name: str):
        p = self._prices.get(name)
        return _FakeSnapshot(p) if p is not None else None

    def get_price_history(self, name: str) -> list[dict]:
        return self._histories.get(name, [])


def _history(prices: list[float]) -> list[dict]:
    return [{"price": p, "volume_7d": 100} for p in prices]


def _advisor(
    prices: dict[str, float] | None = None,
    histories: dict[str, list[dict]] | None = None,
    catalog: dict[str, int] | None = None,
) -> ArmoryAdvisor:
    repo = FakePriceRepo(prices, histories)
    return ArmoryAdvisor(repo, reward_catalog=catalog)


# ─── _assess_confidence ────────────────────────────────────────────────────────


class TestAssessConfidence:
    def _adv(self) -> ArmoryAdvisor:
        return _advisor()

    def test_empty_history_returns_unknown(self):
        adv = self._adv()
        conf, volatile = adv._assess_confidence([], 1000.0)
        assert conf == "UNKNOWN"
        assert volatile is False

    def test_single_point_std_zero_not_volatile(self):
        adv = self._adv()
        conf, volatile = adv._assess_confidence(_history([1000.0]), 1000.0)
        assert volatile is False
        assert conf == "MEDIUM"  # not volatile → MEDIUM (even with 1 point)

    def test_few_points_volatile_returns_low(self):
        # 3 points tightly clustered, current is far outlier → volatile → LOW
        hist = _history([1000.0, 1010.0, 990.0])
        adv = self._adv()
        conf, volatile = adv._assess_confidence(hist, 5000.0)  # extreme outlier
        assert volatile is True
        assert conf == "LOW"

    def test_seven_points_volatile_returns_medium(self):
        # ≥ 7 points → MEDIUM even if volatile
        hist = _history([1000.0] * 6 + [1010.0])  # 7 points
        adv = self._adv()
        conf, volatile = adv._assess_confidence(hist, 9999.0)  # big outlier → volatile
        assert volatile is True
        assert conf == "MEDIUM"

    def test_thirty_points_not_volatile_returns_high(self):
        hist = _history([1000.0] * 30)
        adv = self._adv()
        conf, volatile = adv._assess_confidence(hist, 1000.0)  # exactly on mean
        assert volatile is False
        assert conf == "HIGH"

    def test_thirty_points_volatile_returns_medium(self):
        hist = _history([1000.0] * 29 + [1010.0])  # 30 points
        adv = self._adv()
        conf, volatile = adv._assess_confidence(hist, 9999.0)
        assert volatile is True
        assert conf == "MEDIUM"

    def test_z_score_below_threshold_not_volatile(self):
        # mean=1000, std≈119.5, current=1150 → Z≈1.25 < 2.0 → not volatile
        # prices spread: [800, 900, 1000, 1100, 1200, 1000, 1000]
        prices = [800.0, 900.0, 1000.0, 1100.0, 1200.0, 1000.0, 1000.0]
        adv = self._adv()
        conf, volatile = adv._assess_confidence(_history(prices), 1150.0)
        assert volatile is False

    def test_z_score_above_threshold_volatile(self):
        # tightly clustered, current far off
        prices = [1000.0, 1001.0, 999.0, 1000.0, 1000.0]
        adv = self._adv()
        # std ≈ 0.7; current = 1010 → Z ≈ 14 → volatile
        conf, volatile = adv._assess_confidence(_history(prices), 1010.0)
        assert volatile is True


# ─── _evaluate_reward ─────────────────────────────────────────────────────────


class TestEvaluateReward:
    def test_no_price_all_none(self):
        adv = _advisor()
        result = adv._evaluate_reward("Unknown Case", 1)
        assert result.price is None
        assert result.net_proceeds is None
        assert result.credit_value is None
        assert result.confidence == "UNKNOWN"
        assert result.is_volatile is False

    def test_net_proceeds_formula(self):
        adv = _advisor(prices={"AK Case": 1000.0})
        result = adv._evaluate_reward("AK Case", 1)
        assert result.net_proceeds == pytest.approx(1000.0 * STEAM_NET_MULTIPLIER, rel=1e-9)

    def test_credit_value_divides_by_credits(self):
        adv = _advisor(prices={"AK Case": 1000.0})
        result = adv._evaluate_reward("AK Case", 2)
        expected = (1000.0 * STEAM_NET_MULTIPLIER) / 2
        assert result.credit_value == pytest.approx(expected, rel=1e-9)

    def test_credit_value_one_credit(self):
        adv = _advisor(prices={"AK Case": 2000.0})
        result = adv._evaluate_reward("AK Case", 1)
        assert result.credit_value == pytest.approx(2000.0 * STEAM_NET_MULTIPLIER, rel=1e-9)

    def test_confidence_propagated(self):
        hist = _history([1000.0] * 30)
        adv = _advisor(prices={"AK Case": 1000.0}, histories={"AK Case": hist})
        result = adv._evaluate_reward("AK Case", 1)
        assert result.confidence == "HIGH"

    def test_is_volatile_propagated(self):
        hist = _history([1000.0, 1001.0, 999.0, 1000.0, 1000.0])
        adv = _advisor(prices={"AK Case": 5000.0}, histories={"AK Case": hist})
        result = adv._evaluate_reward("AK Case", 1)
        assert result.is_volatile is True

    def test_returns_reward_evaluation_type(self):
        adv = _advisor(prices={"AK Case": 1000.0})
        result = adv._evaluate_reward("AK Case", 1)
        assert isinstance(result, RewardEvaluation)


# ─── get_pass_efficiency ──────────────────────────────────────────────────────


class TestGetPassEfficiency:
    def test_returns_pass_efficiency_result(self):
        catalog = {"AK Case": 1}
        adv = _advisor(prices={"AK Case": 1000.0}, catalog=catalog)
        result = adv.get_pass_efficiency(500.0)
        assert isinstance(result, PassEfficiencyResult)

    def test_single_known_reward_total_roi(self):
        catalog = {"AK Case": 1}
        price = 2000.0
        pass_cost = 500.0
        adv = _advisor(prices={"AK Case": price}, catalog=catalog)
        result = adv.get_pass_efficiency(pass_cost)

        net = price * STEAM_NET_MULTIPLIER
        expected_roi = (net - pass_cost) / pass_cost
        assert result.total_roi == pytest.approx(expected_roi, rel=1e-9)
        assert result.total_net_proceeds == pytest.approx(net, rel=1e-9)

    def test_unknown_price_total_roi_none(self):
        catalog = {"Missing Case": 1}
        adv = _advisor(catalog=catalog)  # no prices
        result = adv.get_pass_efficiency(500.0)
        assert result.total_roi is None
        assert result.total_net_proceeds is None

    def test_partial_unknown_total_roi_none(self):
        catalog = {"AK Case": 1, "Missing Case": 1}
        adv = _advisor(prices={"AK Case": 1000.0}, catalog=catalog)
        result = adv.get_pass_efficiency(500.0)
        assert result.total_roi is None

    def test_pass_cost_zero_total_roi_none(self):
        catalog = {"AK Case": 1}
        adv = _advisor(prices={"AK Case": 1000.0}, catalog=catalog)
        result = adv.get_pass_efficiency(0.0)
        assert result.total_roi is None

    def test_total_credits_cost_sum(self):
        catalog = {"A": 2, "B": 3, "C": 1}
        adv = _advisor(catalog=catalog)
        result = adv.get_pass_efficiency(1000.0)
        assert result.total_credits_cost == 6

    def test_overall_confidence_minimum(self):
        # One HIGH, one UNKNOWN → overall = UNKNOWN
        hist_high = _history([1000.0] * 30)
        catalog = {"A": 1, "B": 1}
        adv = _advisor(
            prices={"A": 1000.0},  # B has no price → UNKNOWN
            histories={"A": hist_high},
            catalog=catalog,
        )
        result = adv.get_pass_efficiency(500.0)
        assert result.overall_confidence == "UNKNOWN"

    def test_overall_confidence_all_high(self):
        hist = _history([1000.0] * 30)
        catalog = {"A": 1, "B": 1}
        adv = _advisor(
            prices={"A": 1000.0, "B": 2000.0},
            histories={"A": hist, "B": hist},
            catalog=catalog,
        )
        result = adv.get_pass_efficiency(500.0)
        assert result.overall_confidence == "HIGH"

    def test_multiple_rewards_total_net_sum(self):
        catalog = {"A": 1, "B": 1}
        adv = _advisor(prices={"A": 1000.0, "B": 2000.0}, catalog=catalog)
        result = adv.get_pass_efficiency(500.0)
        expected_net = (1000.0 + 2000.0) * STEAM_NET_MULTIPLIER
        assert result.total_net_proceeds == pytest.approx(expected_net, rel=1e-9)

    def test_total_roi_is_float(self):
        catalog = {"A": 1}
        adv = _advisor(prices={"A": 1000.0}, catalog=catalog)
        result = adv.get_pass_efficiency(500.0)
        assert isinstance(result.total_roi, float)

    def test_rewards_list_length_matches_catalog(self):
        catalog = {"A": 1, "B": 1, "C": 1}
        adv = _advisor(catalog=catalog)
        result = adv.get_pass_efficiency(500.0)
        assert len(result.rewards) == 3

    def test_pass_cost_stored_in_result(self):
        catalog = {"A": 1}
        adv = _advisor(prices={"A": 1000.0}, catalog=catalog)
        result = adv.get_pass_efficiency(1234.5)
        assert result.pass_cost == pytest.approx(1234.5)
