"""
Regression Guard — Model Comparator Framework.

Architecture
────────────
ModelComparator runs the same input simultaneously through:
  - baseline_fn: the "frozen" reference implementation (copied at the time
    of a known-good release)
  - current_fn:  the live implementation being tested

It then compares outputs and raises AssertionError when divergence exceeds
a configurable threshold.

Thresholds
──────────
  verdict_match:   required (zero tolerance — decision logic must not change)
  score_match:     required (zero tolerance)
  numeric_delta:   configurable relative tolerance (default 1%)
                   applied to price_ratio_pct, momentum_pct

This file contains:
  1. The ModelComparator framework class
  2. A frozen baseline implementation (copied from investment.py at v97b7ac4)
  3. Regression tests that run both model and current model side by side
  4. Divergence tests: intentionally mutated models must be caught

HOW TO ADD A NEW MODEL VERSION:
  - Register a new baseline in BASELINE_REGISTRY
  - Add a parametrized test using @pytest.mark.parametrize("baseline", ...)
  - The comparator will automatically alert on any divergence > threshold
"""
from __future__ import annotations

from typing import Callable
from unittest.mock import patch

import pytest

# ── Frozen baseline (investment.py @ commit 97b7ac4, April 2026) ──────────────
# This is a verbatim copy of compute_investment_signal at the reference commit.
# DO NOT MODIFY — this is the golden reference.  If you need to update the
# baseline, create a new function with a version suffix and add it to the registry.

def _baseline_v1(
    container_name: str,
    base_cost: float,
    container_type: str,
    current_price: float | None,
    mean_price: float | None = None,
    quantity: int = 0,
    is_event_matched: bool = False,
    *,
    key_price: float = 481.0,
    ratio_floor: float = 50.0,
    momentum_sell_threshold: float = 8.0,
    momentum_event_threshold: float = 12.0,
) -> dict:
    """Frozen baseline — investment signal v1 (April 2026)."""
    _CAPSULE_TYPES = {"Sticker Capsule", "Autograph Capsule", "Event Capsule"}
    _BUY_RATIO = 0.85
    _SELL_RATIO = 1.20
    _BUY_MOM = -5.0

    current = current_price
    mean = mean_price
    qty = quantity

    if not current:
        return {
            "verdict": "NO DATA", "current_price": None, "baseline_price": None,
            "price_ratio_pct": None, "momentum_pct": None, "quantity": 0, "score": 0,
        }

    is_weapon_case = container_type not in _CAPSULE_TYPES
    if is_weapon_case:
        baseline = max(base_cost - key_price, 25.0)
    else:
        baseline = max(base_cost, 25.0)

    ratio = current / baseline
    ratio_pct = (ratio - 1.0) * 100

    momentum_pct = 0.0
    if mean and mean > 0:
        momentum_pct = (current - mean) / mean * 100

    buy_points = 0
    sell_points = 0

    if current >= ratio_floor:
        if ratio < _BUY_RATIO:
            buy_points += 1
        elif ratio > _SELL_RATIO:
            sell_points += 1

    _msell = momentum_event_threshold if is_event_matched else momentum_sell_threshold

    if momentum_pct < _BUY_MOM:
        buy_points += 1
    elif momentum_pct > _msell:
        sell_points += 1

    score = buy_points - sell_points

    if score >= 2:
        verdict = "BUY"
    elif score == 1:
        verdict = "LEAN BUY"
    elif score <= -2:
        verdict = "SELL"
    elif score == -1:
        verdict = "LEAN SELL"
    else:
        verdict = "HOLD"

    return {
        "verdict": verdict,
        "current_price": round(current, 2),
        "baseline_price": round(baseline, 2),
        "price_ratio_pct": round(ratio_pct, 1),
        "momentum_pct": round(momentum_pct, 1),
        "quantity": qty,
        "score": score,
    }


# ── Baseline registry ─────────────────────────────────────────────────────────
# Add new baselines here when upgrading the model.  Tests are parametrized over
# this registry so every registered baseline is checked against the current model.

BASELINE_REGISTRY: dict[str, Callable] = {
    "v1_april_2026": _baseline_v1,
}


# ── ModelComparator ───────────────────────────────────────────────────────────

class ModelComparator:
    """
    Compare a baseline model against the current model on a shared dataset.

    Parameters
    ----------
    baseline_fn:
        The frozen reference callable.  Must accept the same keyword arguments
        as current_fn and return a dict with "verdict", "score", and numeric fields.
    current_fn:
        The live implementation callable.
    numeric_tolerance:
        Relative tolerance for numeric field comparison (default 0.01 = 1%).
    """

    def __init__(
        self,
        baseline_fn: Callable,
        current_fn: Callable,
        numeric_tolerance: float = 0.01,
    ) -> None:
        self._baseline = baseline_fn
        self._current = current_fn
        self._tol = numeric_tolerance
        self.divergences: list[dict] = []

    def compare(self, label: str, baseline_kwargs: dict, current_kwargs: dict) -> dict:
        """
        Run both models and collect divergences.

        Returns a report dict.  Raises AssertionError if verdict or score diverge.
        """
        b_result = self._baseline(**baseline_kwargs)
        c_result = self._current(**current_kwargs)

        report = {
            "label": label,
            "baseline_verdict": b_result["verdict"],
            "current_verdict": c_result["verdict"],
            "baseline_score": b_result["score"],
            "current_score": c_result["score"],
            "numeric_diffs": {},
            "diverged": False,
        }

        # Zero-tolerance checks
        if b_result["verdict"] != c_result["verdict"]:
            report["diverged"] = True
            self.divergences.append({
                "label": label,
                "field": "verdict",
                "baseline": b_result["verdict"],
                "current": c_result["verdict"],
            })

        if b_result["score"] != c_result["score"]:
            report["diverged"] = True
            self.divergences.append({
                "label": label,
                "field": "score",
                "baseline": b_result["score"],
                "current": c_result["score"],
            })

        # Numeric tolerance check
        for field in ("price_ratio_pct", "momentum_pct", "current_price", "baseline_price"):
            bv = b_result.get(field)
            cv = c_result.get(field)
            if bv is None and cv is None:
                continue
            if bv is None or cv is None:
                diff = float("inf")
            else:
                ref = abs(bv) if abs(bv) > 1e-9 else 1.0
                diff = abs(bv - cv) / ref
            if diff > self._tol:
                report["numeric_diffs"][field] = {
                    "baseline": bv, "current": cv, "rel_diff": diff
                }
                report["diverged"] = True

        return report

    def assert_no_divergence(self) -> None:
        if self.divergences:
            lines = [f"Model divergence detected ({len(self.divergences)} issue(s)):"]
            for d in self.divergences:
                lines.append(
                    f"  [{d['label']}] field={d['field']} "
                    f"baseline={d['baseline']!r} current={d['current']!r}"
                )
            raise AssertionError("\n".join(lines))


# ── Test dataset ──────────────────────────────────────────────────────────────

_COMPARATOR_DATASET = [
    # (label, current_price, mean_price, base_cost, container_type)
    ("cheap_weapon_case",    900.0,  950.0,  1445.0, "Weapon Case"),
    ("expensive_weapon_case", 2500.0, 2000.0, 1445.0, "Weapon Case"),
    ("fair_hold",           1000.0, 1010.0, 1445.0, "Weapon Case"),
    ("falling_momentum",     800.0,  920.0,  1445.0, "Weapon Case"),
    ("spiking_momentum",    1300.0, 1000.0,  1445.0, "Weapon Case"),
    ("capsule_cheap",        300.0,  310.0,   480.0, "Sticker Capsule"),
    ("capsule_expensive",    900.0,  500.0,   480.0, "Autograph Capsule"),
    ("near_ratio_floor",      55.0,   50.0,  1445.0, "Weapon Case"),
    ("below_ratio_floor",     30.0,   25.0,  1445.0, "Weapon Case"),
    ("no_mean",              500.0,   None,  1445.0, "Weapon Case"),
    ("no_price",              None,   None,  1445.0, "Weapon Case"),
]

_MOCK_S = type("S", (), {
    "key_price": 481.0,
    "ratio_floor": 50.0,
    "liquidity_min_volume": 5.0,
    "steam_fee_divisor": 1.15,
    "steam_fee_fixed": 5.0,
    "momentum_event_threshold": 12.0,
    "currency_symbol": "₸",
})()


def _current_signal(**kwargs):
    from src.domain.investment import compute_investment_signal
    with patch("src.domain.investment.settings", _MOCK_S):
        return compute_investment_signal(**kwargs)


def _baseline_kwargs(current_price, mean_price, base_cost, container_type):
    return dict(
        container_name="HypTest",
        base_cost=base_cost,
        container_type=container_type,
        current_price=current_price,
        mean_price=mean_price,
        key_price=481.0,
        ratio_floor=50.0,
    )


def _current_kwargs(current_price, mean_price, base_cost, container_type):
    return dict(
        container_name="HypTest",
        base_cost=base_cost,
        container_type=container_type,
        current_price=current_price,
        mean_price=mean_price,
    )


# ── Regression tests ──────────────────────────────────────────────────────────

class TestModelComparatorVsCurrent:
    """Run every registered baseline against the current model on all scenarios."""

    @pytest.mark.parametrize("baseline_name,baseline_fn", list(BASELINE_REGISTRY.items()))
    @pytest.mark.parametrize(
        "label,current_price,mean_price,base_cost,container_type",
        _COMPARATOR_DATASET,
    )
    def test_no_divergence_from_baseline(
        self,
        baseline_name,
        baseline_fn,
        label,
        current_price,
        mean_price,
        base_cost,
        container_type,
    ):
        comparator = ModelComparator(
            baseline_fn=lambda **kw: baseline_fn(**kw),
            current_fn=_current_signal,
            numeric_tolerance=0.01,
        )

        report = comparator.compare(
            label=f"{baseline_name}/{label}",
            baseline_kwargs=_baseline_kwargs(current_price, mean_price, base_cost, container_type),
            current_kwargs=_current_kwargs(current_price, mean_price, base_cost, container_type),
        )

        assert not report["diverged"], (
            f"Divergence detected for {baseline_name}/{label}:\n"
            f"  verdict: {report['baseline_verdict']!r} → {report['current_verdict']!r}\n"
            f"  score:   {report['baseline_score']} → {report['current_score']}\n"
            f"  numeric: {report['numeric_diffs']}"
        )


class TestModelComparatorDetectsMutation:
    """The comparator MUST detect intentional formula mutations (meta-test)."""

    @staticmethod
    def _mutated_signal_buy_threshold_relaxed(**kwargs):
        """Mutated model: BUY threshold changed from 0.85 → 0.70 (would miss cheap signals)."""
        # Extract from kwargs
        current_price = kwargs.get("current_price")
        mean_price = kwargs.get("mean_price")
        base_cost = kwargs.get("base_cost", 1445.0)
        container_type = kwargs.get("container_type", "Weapon Case")

        _CAPSULE_TYPES = {"Sticker Capsule", "Autograph Capsule", "Event Capsule"}

        if not current_price:
            return {
                "verdict": "NO DATA", "current_price": None, "baseline_price": None,
                "price_ratio_pct": None, "momentum_pct": None, "quantity": 0, "score": 0,
            }

        is_wc = container_type not in _CAPSULE_TYPES
        baseline = max(base_cost - 481.0, 25.0) if is_wc else max(base_cost, 25.0)
        ratio = current_price / baseline
        ratio_pct = (ratio - 1.0) * 100
        momentum_pct = 0.0
        if mean_price and mean_price > 0:
            momentum_pct = (current_price - mean_price) / mean_price * 100

        buy_points = 0
        sell_points = 0
        if current_price >= 50.0:
            if ratio < 0.70:   # MUTATED: was 0.85
                buy_points += 1
            elif ratio > 1.20:
                sell_points += 1
        if momentum_pct < -5.0:
            buy_points += 1
        elif momentum_pct > 8.0:
            sell_points += 1

        score = buy_points - sell_points
        verdict_map = {2: "BUY", 1: "LEAN BUY", 0: "HOLD", -1: "LEAN SELL", -2: "SELL"}
        # clamp
        score = max(-2, min(2, score))
        verdict = verdict_map.get(score, "HOLD")

        return {
            "verdict": verdict, "current_price": round(current_price, 2),
            "baseline_price": round(baseline, 2), "price_ratio_pct": round(ratio_pct, 1),
            "momentum_pct": round(momentum_pct, 1), "quantity": 0, "score": score,
        }

    def test_comparator_catches_threshold_mutation(self):
        """
        Input: ratio = 0.80 (between 0.70 and 0.85).
        Baseline (0.85 threshold): BUY signal triggered.
        Mutated (0.70 threshold): BUY signal NOT triggered.
        Comparator must detect this divergence.
        """
        comparator = ModelComparator(
            baseline_fn=_baseline_v1,
            current_fn=self._mutated_signal_buy_threshold_relaxed,
            numeric_tolerance=0.01,
        )

        # ratio = 768 / 964 = 0.797 → between 0.70 and 0.85
        # baseline triggers BUY (ratio < 0.85), mutated does NOT
        report = comparator.compare(
            label="mutation_detection",
            baseline_kwargs=_baseline_kwargs(
                current_price=768.0,
                mean_price=920.0,
                base_cost=1445.0,
                container_type="Weapon Case",
            ),
            current_kwargs={
                "current_price": 768.0,
                "mean_price": 920.0,
                "base_cost": 1445.0,
                "container_type": "Weapon Case",
            },
        )

        assert report["diverged"], (
            "Comparator failed to detect threshold mutation! "
            f"baseline={report['baseline_verdict']!r} current={report['current_verdict']!r}"
        )

    def test_assert_no_divergence_raises_on_mismatch(self):
        """assert_no_divergence() must raise AssertionError when divergences exist."""
        comparator = ModelComparator(
            baseline_fn=lambda **kw: {"verdict": "BUY", "score": 2,
                                      "current_price": 100.0, "baseline_price": 100.0,
                                      "price_ratio_pct": 0.0, "momentum_pct": 0.0,
                                      "quantity": 0},
            current_fn=lambda **kw: {"verdict": "SELL", "score": -2,
                                     "current_price": 100.0, "baseline_price": 100.0,
                                     "price_ratio_pct": 0.0, "momentum_pct": 0.0,
                                     "quantity": 0},
        )
        comparator.compare("test", baseline_kwargs={}, current_kwargs={})
        with pytest.raises(AssertionError, match="Model divergence"):
            comparator.assert_no_divergence()

    def test_comparator_passes_when_models_identical(self):
        """Identical models must produce zero divergences."""
        comparator = ModelComparator(
            baseline_fn=_baseline_v1,
            current_fn=lambda **kw: _baseline_v1(**kw),
            numeric_tolerance=0.01,
        )
        for label, cp, mp, bc, ct in _COMPARATOR_DATASET:
            comparator.compare(
                label=label,
                baseline_kwargs=_baseline_kwargs(cp, mp, bc, ct),
                current_kwargs=_baseline_kwargs(cp, mp, bc, ct),
            )
        comparator.assert_no_divergence()  # must not raise


# ── Threshold drift detection ─────────────────────────────────────────────────

class TestThresholdDriftDetection:
    """
    Verify that the model is sensitive to inputs near decision boundaries.
    These tests will fail if thresholds are accidentally shifted.
    """

    def test_buy_threshold_boundary(self):
        """ratio = 0.84 (just below 0.85) → BUY point; ratio = 0.86 → no ratio signal."""
        with patch("src.domain.investment.settings", _MOCK_S):
            from src.domain.investment import compute_investment_signal
            # baseline = 964; 0.84 * 964 = 809.76 → ratio < 0.85 → buy point
            r_cheap = compute_investment_signal(
                container_name="T", base_cost=1445.0, container_type="Weapon Case",
                current_price=809.0, mean_price=809.0,
            )
            # 0.86 * 964 = 829.0 → ratio > 0.85 → no ratio buy
            r_fair = compute_investment_signal(
                container_name="T", base_cost=1445.0, container_type="Weapon Case",
                current_price=830.0, mean_price=830.0,
            )

        assert r_cheap["score"] > r_fair["score"], (
            "BUY threshold boundary not respected: "
            f"cheap score={r_cheap['score']}, fair score={r_fair['score']}"
        )

    def test_sell_threshold_boundary(self):
        """ratio = 1.21 → SELL point; ratio = 1.19 → no ratio sell."""
        with patch("src.domain.investment.settings", _MOCK_S):
            from src.domain.investment import compute_investment_signal
            # baseline = 964; 1.21 * 964 = 1166.4 → ratio > 1.20 → sell point
            r_expensive = compute_investment_signal(
                container_name="T", base_cost=1445.0, container_type="Weapon Case",
                current_price=1167.0, mean_price=1167.0,
            )
            # 1.19 * 964 = 1147.2 → ratio < 1.20 → no ratio sell
            r_fair = compute_investment_signal(
                container_name="T", base_cost=1445.0, container_type="Weapon Case",
                current_price=1147.0, mean_price=1147.0,
            )

        assert r_expensive["score"] < r_fair["score"], (
            "SELL threshold boundary not respected: "
            f"expensive score={r_expensive['score']}, fair score={r_fair['score']}"
        )

    def test_momentum_buy_boundary(self):
        """momentum = −5.1% → buy point; −4.9% → neutral."""
        with patch("src.domain.investment.settings", _MOCK_S):
            from src.domain.investment import compute_investment_signal
            # current = mean * (1 - 0.051) → momentum ≈ -5.1%
            mean = 1000.0
            r_falling = compute_investment_signal(
                container_name="T", base_cost=1445.0, container_type="Weapon Case",
                current_price=mean * 0.949, mean_price=mean,
            )
            r_neutral = compute_investment_signal(
                container_name="T", base_cost=1445.0, container_type="Weapon Case",
                current_price=mean * 0.951, mean_price=mean,
            )

        assert r_falling["score"] > r_neutral["score"], (
            "Momentum BUY boundary not respected"
        )

    def test_momentum_sell_boundary(self):
        """momentum = 8.1% → sell point; 7.9% → neutral."""
        with patch("src.domain.investment.settings", _MOCK_S):
            from src.domain.investment import compute_investment_signal
            mean = 1000.0
            r_spiking = compute_investment_signal(
                container_name="T", base_cost=1445.0, container_type="Weapon Case",
                current_price=mean * 1.081, mean_price=mean,
            )
            r_neutral = compute_investment_signal(
                container_name="T", base_cost=1445.0, container_type="Weapon Case",
                current_price=mean * 1.079, mean_price=mean,
            )

        assert r_spiking["score"] < r_neutral["score"], (
            "Momentum SELL boundary not respected"
        )
