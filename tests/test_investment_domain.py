"""
Unit tests for InvestmentDomainService (Liquidity Guard).
domain/services.py — InvestmentDomainService.evaluate_investment()
"""

from __future__ import annotations

import dataclasses

import pytest

from domain.services import InvestmentDomainService, LiquidityDecision
from domain.value_objects import Amount


@pytest.fixture()
def svc() -> InvestmentDomainService:
    return InvestmentDomainService()


def _liq(volume_24h: int, min_ratio: float, price: float = 500.0, avg_7d: float = 100.0) -> LiquidityDecision:
    return InvestmentDomainService().evaluate_investment(
        price=Amount(price),
        volume_24h=volume_24h,
        avg_volume_7d=avg_7d,
        min_liquidity_ratio=min_ratio,
    )


# ─── Basic guard logic ─────────────────────────────────────────────────────────

class TestLiquidityGuardBasic:
    def test_liquid_when_volume_meets_threshold(self, svc):
        d = _liq(volume_24h=50, min_ratio=10.0)
        assert d.is_liquid is True

    def test_liquid_reason_is_none_on_success(self, svc):
        d = _liq(volume_24h=50, min_ratio=10.0)
        assert d.reason is None

    def test_illiquid_when_volume_below_threshold(self, svc):
        d = _liq(volume_24h=5, min_ratio=10.0)
        assert d.is_liquid is False

    def test_illiquid_reason_populated(self, svc):
        d = _liq(volume_24h=5, min_ratio=10.0)
        assert d.reason is not None
        assert len(d.reason) > 0

    def test_illiquid_reason_contains_volume(self, svc):
        d = _liq(volume_24h=5, min_ratio=10.0)
        assert "5" in d.reason

    def test_volume_exactly_at_threshold_is_liquid(self, svc):
        # volume_24h == min_liquidity_ratio → NOT < → liquid
        d = _liq(volume_24h=10, min_ratio=10.0)
        assert d.is_liquid is True

    def test_volume_one_below_threshold_is_illiquid(self, svc):
        d = _liq(volume_24h=9, min_ratio=10.0)
        assert d.is_liquid is False

    def test_result_is_frozen_dataclass(self, svc):
        d = _liq(volume_24h=50, min_ratio=10.0)
        with pytest.raises(dataclasses.FrozenInstanceError):
            d.is_liquid = False  # type: ignore[misc]

    def test_returns_liquidity_decision(self, svc):
        d = _liq(volume_24h=50, min_ratio=10.0)
        assert isinstance(d, LiquidityDecision)


# ─── Low Liquidity Trap (PV-05 test case) ────────────────────────────────────

class TestLowLiquidityTrap:
    def test_low_volume_trap_rejected(self, svc):
        """
        Classic low-liquidity trap: Z-score excellent, price low, but
        only 2 units traded per day while intending to buy 100.
        Guard must reject: volume_24h=2 < min_liquidity_ratio=100.
        """
        d = svc.evaluate_investment(
            price=Amount(50),       # cheap item
            volume_24h=2,        # only 2 units/day
            avg_volume_7d=3.0,   # thin market historically too
            min_liquidity_ratio=100.0,  # = desired_units (100) * safety_factor (1.0)
        )
        assert d.is_liquid is False

    def test_low_volume_trap_reason_explains_gap(self, svc):
        d = svc.evaluate_investment(
            price=Amount(50),
            volume_24h=2,
            avg_volume_7d=3.0,
            min_liquidity_ratio=100.0,
        )
        assert "2" in d.reason        # actual volume mentioned
        assert "100" in d.reason      # required volume mentioned

    def test_same_item_liquid_with_higher_volume(self, svc):
        """Same cheap item becomes liquid once volume recovers above threshold."""
        d = svc.evaluate_investment(
            price=Amount(50),
            volume_24h=150,
            avg_volume_7d=120.0,
            min_liquidity_ratio=100.0,
        )
        assert d.is_liquid is True

    def test_high_price_item_low_volume_still_rejected(self, svc):
        """Expensive items with low daily volume are also rejected."""
        d = svc.evaluate_investment(
            price=Amount(5000),
            volume_24h=1,
            avg_volume_7d=2.0,
            min_liquidity_ratio=5.0,
        )
        assert d.is_liquid is False

    def test_zero_volume_always_rejected(self, svc):
        d = svc.evaluate_investment(
            price=Amount(500),
            volume_24h=0,
            avg_volume_7d=0.0,
            min_liquidity_ratio=1.0,
        )
        assert d.is_liquid is False

    def test_zero_min_ratio_always_liquid(self, svc):
        """min_liquidity_ratio=0 disables the guard (all markets are liquid)."""
        d = svc.evaluate_investment(
            price=Amount(500),
            volume_24h=0,
            avg_volume_7d=0.0,
            min_liquidity_ratio=0.0,
        )
        assert d.is_liquid is True


# ─── Price parameter used in reason string ────────────────────────────────────

class TestPriceInContext:
    def test_price_reflected_in_reason(self, svc):
        d = svc.evaluate_investment(
            price=Amount(250),
            volume_24h=1,
            avg_volume_7d=50.0,
            min_liquidity_ratio=10.0,
        )
        assert d.is_liquid is False
        # KZT str representation should appear in reason
        assert "250" in d.reason

    def test_avg_volume_reflected_in_reason(self, svc):
        d = svc.evaluate_investment(
            price=Amount(500),
            volume_24h=3,
            avg_volume_7d=80.0,
            min_liquidity_ratio=10.0,
        )
        assert "80" in d.reason
