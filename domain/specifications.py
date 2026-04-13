"""
Specification Pattern — composable, reusable business-rule objects.

Specifications encapsulate a single predicate that can be combined with
other specifications using & (AND), | (OR), and ~ (NOT) operators.

All concrete specs accept a candidate dict with well-known keys:
    {
        "current_price": float | None,   # price per unit
        "z_score":       float | None,   # statistical Z-score
        "volume":        float | int,    # daily traded units
        "roi":           float | None,   # ROI ratio (e.g. 0.05 = 5 %)
    }

Missing keys are treated conservatively (spec returns False / not satisfied).

Usage:
    spec = ZScoreBelow(-3.0) & PriceWithinRange(Amount(50), Amount(700))
    if spec.is_satisfied_by(candidate):
        ...

    # Composition:
    cheap_and_oversold = PriceWithinRange(Amount(1), Amount(500)) & ZScoreBelow(-2.5)
    liquid_or_forced   = VolumeAbove(10) | AlwaysTrue()   # example
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from domain.value_objects import Amount, ROI

# ─── Base + composites ────────────────────────────────────────────────────────


class Specification(ABC):
    """Abstract base for all domain specifications."""

    @abstractmethod
    def is_satisfied_by(self, candidate: Any) -> bool:
        """Return True iff candidate satisfies this specification."""

    def __and__(self, other: Specification) -> Specification:
        return _AndSpecification(self, other)

    def __or__(self, other: Specification) -> Specification:
        return _OrSpecification(self, other)

    def __invert__(self) -> Specification:
        return _NotSpecification(self)


class _AndSpecification(Specification):
    def __init__(self, left: Specification, right: Specification) -> None:
        self._left = left
        self._right = right

    def is_satisfied_by(self, candidate: Any) -> bool:
        return self._left.is_satisfied_by(candidate) and self._right.is_satisfied_by(candidate)


class _OrSpecification(Specification):
    def __init__(self, left: Specification, right: Specification) -> None:
        self._left = left
        self._right = right

    def is_satisfied_by(self, candidate: Any) -> bool:
        return self._left.is_satisfied_by(candidate) or self._right.is_satisfied_by(candidate)


class _NotSpecification(Specification):
    def __init__(self, spec: Specification) -> None:
        self._spec = spec

    def is_satisfied_by(self, candidate: Any) -> bool:
        return not self._spec.is_satisfied_by(candidate)


# ─── Concrete specifications ──────────────────────────────────────────────────


class PriceWithinRange(Specification):
    """
    Satisfied when candidate["current_price"] is in [min_p.amount, max_p.amount].

    Returns False when current_price is missing or non-positive.
    """

    def __init__(self, min_p: Amount, max_p: Amount) -> None:
        self._min = min_p.amount
        self._max = max_p.amount

    def is_satisfied_by(self, candidate: Any) -> bool:
        price = candidate.get("current_price") if isinstance(candidate, dict) else None
        if not price or price <= 0:
            return False
        return self._min <= price <= self._max


class ZScoreBelow(Specification):
    """
    Satisfied when candidate["z_score"] <= threshold.

    A very negative Z-score signals a statistical anomaly / deep crash.
    Returns False when z_score is missing.
    """

    def __init__(self, threshold: float) -> None:
        self._threshold = threshold

    def is_satisfied_by(self, candidate: Any) -> bool:
        z = candidate.get("z_score") if isinstance(candidate, dict) else None
        if z is None:
            return False
        return float(z) <= self._threshold


class VolumeAbove(Specification):
    """
    Satisfied when candidate["volume"] >= min_vol.

    Used as a Liquidity Guard: reject BUY signals when the market is too thin
    to absorb the intended position without significant price impact.
    Returns False when volume is missing.
    """

    def __init__(self, min_vol: float) -> None:
        self._min_vol = min_vol

    def is_satisfied_by(self, candidate: Any) -> bool:
        vol = candidate.get("volume") if isinstance(candidate, dict) else None
        if vol is None:
            return False
        return float(vol) >= self._min_vol


class ROIAbove(Specification):
    """
    Satisfied when candidate["roi"] >= min_roi.value.

    candidate["roi"] may be a plain float (ratio) or a ROI object.
    Returns False when roi is missing.
    """

    def __init__(self, min_roi: ROI) -> None:
        self._min = min_roi.value

    def is_satisfied_by(self, candidate: Any) -> bool:
        raw = candidate.get("roi") if isinstance(candidate, dict) else None
        if raw is None:
            return False
        roi_val = raw.value if isinstance(raw, ROI) else float(raw)
        return roi_val >= self._min
