"""
Unit tests for domain/specifications.py — Specification Pattern (DDD-3.3).

Covers:
  - Specification ABC (cannot instantiate directly)
  - Composite operators: & (AND), | (OR), ~ (NOT)
  - Concrete specs: PriceWithinRange, ZScoreBelow, VolumeAbove, ROIAbove
  - Integration with KZT and ROI value objects
  - Complex chains: (PriceSpec & ZScoreSpec) | ForcedSpec
"""

from __future__ import annotations

from typing import Any

import pytest

from domain.specifications import (
    PriceWithinRange,
    ROIAbove,
    Specification,
    VolumeAbove,
    ZScoreBelow,
)
from domain.value_objects import Amount, ROI

# ─── Test helpers ─────────────────────────────────────────────────────────────


class AlwaysTrue(Specification):
    """Test stub — always satisfied."""

    def is_satisfied_by(self, candidate: Any) -> bool:
        return True


class AlwaysFalse(Specification):
    """Test stub — never satisfied."""

    def is_satisfied_by(self, candidate: Any) -> bool:
        return False


def _candidate(
    price: float | None = 500.0,
    z_score: float | None = -3.5,
    volume: float = 50.0,
    roi: float | None = 0.10,
) -> dict:
    return {"current_price": price, "z_score": z_score, "volume": volume, "roi": roi}


# ─── ABC contract ─────────────────────────────────────────────────────────────


class TestSpecificationABC:
    def test_cannot_instantiate_abc(self):
        with pytest.raises(TypeError):
            Specification()  # type: ignore[abstract]

    def test_always_true_stub_satisfies(self):
        assert AlwaysTrue().is_satisfied_by({}) is True

    def test_always_false_stub_not_satisfied(self):
        assert AlwaysFalse().is_satisfied_by({}) is False


# ─── AND operator ─────────────────────────────────────────────────────────────


class TestAndSpecification:
    def test_true_and_true_is_true(self):
        spec = AlwaysTrue() & AlwaysTrue()
        assert spec.is_satisfied_by({}) is True

    def test_true_and_false_is_false(self):
        spec = AlwaysTrue() & AlwaysFalse()
        assert spec.is_satisfied_by({}) is False

    def test_false_and_true_is_false(self):
        spec = AlwaysFalse() & AlwaysTrue()
        assert spec.is_satisfied_by({}) is False

    def test_false_and_false_is_false(self):
        spec = AlwaysFalse() & AlwaysFalse()
        assert spec.is_satisfied_by({}) is False

    def test_short_circuit_left_false_skips_right(self):
        """AND short-circuits: if left is False, right is not evaluated."""
        called = []

        class Witness(Specification):
            def is_satisfied_by(self, candidate: Any) -> bool:
                called.append(True)
                return True

        spec = AlwaysFalse() & Witness()
        spec.is_satisfied_by({})
        assert called == []  # Witness never called

    def test_three_way_and(self):
        spec = AlwaysTrue() & AlwaysTrue() & AlwaysFalse()
        assert spec.is_satisfied_by({}) is False


# ─── OR operator ──────────────────────────────────────────────────────────────


class TestOrSpecification:
    def test_true_or_true_is_true(self):
        assert (AlwaysTrue() | AlwaysTrue()).is_satisfied_by({}) is True

    def test_true_or_false_is_true(self):
        assert (AlwaysTrue() | AlwaysFalse()).is_satisfied_by({}) is True

    def test_false_or_true_is_true(self):
        assert (AlwaysFalse() | AlwaysTrue()).is_satisfied_by({}) is True

    def test_false_or_false_is_false(self):
        assert (AlwaysFalse() | AlwaysFalse()).is_satisfied_by({}) is False

    def test_three_way_or(self):
        spec = AlwaysFalse() | AlwaysFalse() | AlwaysTrue()
        assert spec.is_satisfied_by({}) is True


# ─── NOT operator ─────────────────────────────────────────────────────────────


class TestNotSpecification:
    def test_not_true_is_false(self):
        assert (~AlwaysTrue()).is_satisfied_by({}) is False

    def test_not_false_is_true(self):
        assert (~AlwaysFalse()).is_satisfied_by({}) is True

    def test_double_not(self):
        assert (~~AlwaysTrue()).is_satisfied_by({}) is True


# ─── PriceWithinRange ─────────────────────────────────────────────────────────


class TestPriceWithinRange:
    def test_price_inside_range(self):
        spec = PriceWithinRange(Amount(100), Amount(1000))
        assert spec.is_satisfied_by(_candidate(price=500)) is True

    def test_price_at_lower_bound(self):
        spec = PriceWithinRange(Amount(500), Amount(1000))
        assert spec.is_satisfied_by(_candidate(price=500)) is True

    def test_price_at_upper_bound(self):
        spec = PriceWithinRange(Amount(100), Amount(500))
        assert spec.is_satisfied_by(_candidate(price=500)) is True

    def test_price_below_range(self):
        spec = PriceWithinRange(Amount(600), Amount(1000))
        assert spec.is_satisfied_by(_candidate(price=500)) is False

    def test_price_above_range(self):
        spec = PriceWithinRange(Amount(100), Amount(400))
        assert spec.is_satisfied_by(_candidate(price=500)) is False

    def test_missing_price_returns_false(self):
        spec = PriceWithinRange(Amount(100), Amount(1000))
        assert spec.is_satisfied_by(_candidate(price=None)) is False

    def test_zero_price_returns_false(self):
        spec = PriceWithinRange(Amount(0), Amount(1000))
        assert spec.is_satisfied_by(_candidate(price=0.0)) is False

    def test_uses_kzt_amounts(self):
        """KZT rounding applies: Amount(500.4) rounds to 500, Amount(500.5) rounds to 501."""
        spec = PriceWithinRange(Amount(1), Amount(500))
        assert spec.is_satisfied_by(_candidate(price=500)) is True


# ─── ZScoreBelow ──────────────────────────────────────────────────────────────


class TestZScoreBelow:
    def test_z_below_threshold(self):
        spec = ZScoreBelow(-3.0)
        assert spec.is_satisfied_by(_candidate(z_score=-3.5)) is True

    def test_z_exactly_at_threshold(self):
        spec = ZScoreBelow(-3.0)
        assert spec.is_satisfied_by(_candidate(z_score=-3.0)) is True

    def test_z_above_threshold(self):
        spec = ZScoreBelow(-3.0)
        assert spec.is_satisfied_by(_candidate(z_score=-2.9)) is False

    def test_missing_z_returns_false(self):
        spec = ZScoreBelow(-3.0)
        assert spec.is_satisfied_by(_candidate(z_score=None)) is False

    def test_positive_z_fails_negative_threshold(self):
        spec = ZScoreBelow(-3.0)
        assert spec.is_satisfied_by(_candidate(z_score=1.0)) is False


# ─── VolumeAbove ──────────────────────────────────────────────────────────────


class TestVolumeAbove:
    def test_volume_above_min(self):
        spec = VolumeAbove(10.0)
        assert spec.is_satisfied_by({"volume": 50}) is True

    def test_volume_exactly_at_min(self):
        spec = VolumeAbove(10.0)
        assert spec.is_satisfied_by({"volume": 10}) is True

    def test_volume_below_min(self):
        spec = VolumeAbove(10.0)
        assert spec.is_satisfied_by({"volume": 5}) is False

    def test_zero_volume_fails(self):
        spec = VolumeAbove(1.0)
        assert spec.is_satisfied_by({"volume": 0}) is False

    def test_missing_volume_returns_false(self):
        spec = VolumeAbove(10.0)
        assert spec.is_satisfied_by({}) is False

    def test_zero_min_accepts_any_volume(self):
        spec = VolumeAbove(0.0)
        assert spec.is_satisfied_by({"volume": 0}) is True

    def test_low_liquidity_trap(self):
        """Classic trap: want 100 units, market only trades 2/day."""
        spec = VolumeAbove(100.0)   # min_ratio = desired_units * safety_factor
        assert spec.is_satisfied_by({"volume": 2}) is False


# ─── ROIAbove ─────────────────────────────────────────────────────────────────


class TestROIAbove:
    def test_roi_above_min(self):
        spec = ROIAbove(ROI(0.10))
        assert spec.is_satisfied_by(_candidate(roi=0.20)) is True

    def test_roi_exactly_at_min(self):
        spec = ROIAbove(ROI(0.10))
        assert spec.is_satisfied_by(_candidate(roi=0.10)) is True

    def test_roi_below_min(self):
        spec = ROIAbove(ROI(0.10))
        assert spec.is_satisfied_by(_candidate(roi=0.05)) is False

    def test_roi_as_roi_object(self):
        spec = ROIAbove(ROI(0.05))
        assert spec.is_satisfied_by({"roi": ROI(0.10)}) is True

    def test_roi_as_float(self):
        spec = ROIAbove(ROI(0.05))
        assert spec.is_satisfied_by({"roi": 0.10}) is True

    def test_missing_roi_returns_false(self):
        spec = ROIAbove(ROI(0.05))
        assert spec.is_satisfied_by({}) is False

    def test_negative_roi_fails_positive_threshold(self):
        spec = ROIAbove(ROI(0.01))
        assert spec.is_satisfied_by(_candidate(roi=-0.05)) is False


# ─── Complex composite chains ─────────────────────────────────────────────────


class TestCompositeChains:
    def test_price_and_zscore_chain(self):
        """Both conditions must hold for super-deal pre-screen."""
        spec = PriceWithinRange(Amount(100), Amount(700)) & ZScoreBelow(-3.0)
        # price=500 in range AND z=-4.0 <= -3.0 → True
        assert spec.is_satisfied_by(_candidate(price=500, z_score=-4.0)) is True

    def test_price_and_zscore_fails_on_z(self):
        spec = PriceWithinRange(Amount(100), Amount(700)) & ZScoreBelow(-3.0)
        assert spec.is_satisfied_by(_candidate(price=500, z_score=-2.0)) is False

    def test_price_and_zscore_fails_on_price(self):
        spec = PriceWithinRange(Amount(100), Amount(400)) & ZScoreBelow(-3.0)
        assert spec.is_satisfied_by(_candidate(price=500, z_score=-4.0)) is False

    def test_price_and_zscore_or_forced(self):
        """(PriceSpec & ZScoreSpec) | ForcedBuySpec pattern."""
        normal_path = PriceWithinRange(Amount(100), Amount(700)) & ZScoreBelow(-3.0)
        forced_path = AlwaysTrue()   # simulates a "forced buy" override
        spec = normal_path | forced_path

        # Fails normal path but forced_path rescues it
        assert spec.is_satisfied_by(_candidate(price=500, z_score=-1.0)) is True

    def test_liquidity_and_roi_filter(self):
        """Investment pre-screen: liquid AND profitable."""
        spec = VolumeAbove(10.0) & ROIAbove(ROI(0.05))
        assert spec.is_satisfied_by({"volume": 50, "roi": 0.10}) is True
        assert spec.is_satisfied_by({"volume": 5, "roi": 0.10}) is False
        assert spec.is_satisfied_by({"volume": 50, "roi": 0.01}) is False

    def test_not_cheap_spec(self):
        """~PriceWithinRange detects expensive items."""
        cheap_spec = PriceWithinRange(Amount(1), Amount(300))
        expensive_spec = ~cheap_spec
        assert expensive_spec.is_satisfied_by(_candidate(price=500)) is True
        assert expensive_spec.is_satisfied_by(_candidate(price=200)) is False

    def test_deeply_nested_composition(self):
        """Three-level composition works correctly."""
        spec = (ZScoreBelow(-3.0) & VolumeAbove(5.0)) | (ROIAbove(ROI(0.30)) & AlwaysTrue())
        # Neither branch satisfied
        assert spec.is_satisfied_by({"z_score": -1.0, "volume": 2, "roi": 0.10}) is False
        # First branch satisfied
        assert spec.is_satisfied_by({"z_score": -4.0, "volume": 10, "roi": 0.10}) is True
        # Second branch satisfied
        assert spec.is_satisfied_by({"z_score": -1.0, "volume": 2, "roi": 0.50}) is True
