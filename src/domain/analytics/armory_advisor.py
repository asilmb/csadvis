"""
Armory Pass ROI Advisor (PV-22).

Evaluates the efficiency of an Armory Pass purchase by comparing the expected
net proceeds from selling its rewards on the Steam Market against the pass cost.

Formula:
    net_reward = price * STEAM_NET_MULTIPLIER   (per unit)
    credit_value = net_reward / credits_required
    total_roi = (Σ net_reward - pass_cost) / pass_cost

Confidence levels (per reward):
    HIGH    ≥ 30 price data points, |Z-score| ≤ 2.0
    MEDIUM  ≥ 7 points   OR  |Z-score| ≤ 2.0  (but not both for HIGH)
    LOW     < 7 points  AND  |Z-score| > 2.0   OR < 7 points with no volatility data
    UNKNOWN no price data

Usage:
    from src.domain.connection import SessionLocal
    from src.domain.sql_repositories import SqlAlchemyPriceRepository
    from src.domain.analytics.armory_advisor import ArmoryAdvisor, DEFAULT_REWARD_CATALOG

    with SessionLocal() as db:
        advisor = ArmoryAdvisor(SqlAlchemyPriceRepository(db))
        result = advisor.get_pass_efficiency(pass_cost=2500.0)
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

# Steam Market net multiplier: seller receives ~86.9% of sale price (13.1% fee)
STEAM_NET_MULTIPLIER: float = 0.869

# Confidence rank (lower = worse) — used for overall_confidence aggregation.
_CONFIDENCE_RANK: dict[str, int] = {
    "UNKNOWN": 0,
    "LOW": 1,
    "MEDIUM": 2,
    "HIGH": 3,
}

# Armory Pass reward catalog: market_hash_name → credits required to earn one unit.
# Update when Valve releases new passes / reward pools.
DEFAULT_REWARD_CATALOG: dict[str, int] = {
    "Revolution Case": 1,
    "Recoil Case": 1,
    "Dreams & Nightmares Case": 1,
    "Fracture Case": 1,
    "Snakebite Case": 1,
    "Clutch Case": 1,
    "Prisma 2 Case": 1,
    "Prisma Case": 1,
    "CS20 Case": 1,
    "Danger Zone Case": 1,
    "Horizon Case": 1,
    "Spectrum 2 Case": 1,
    "Spectrum Case": 1,
    "Operation Hydra Case": 1,
    "Glove Case": 1,
    "Gamma 2 Case": 1,
    "Gamma Case": 1,
    "Chroma 3 Case": 1,
    "Chroma 2 Case": 1,
    "Chroma Case": 1,
    "Falchion Case": 1,
    "Shadow Case": 1,
    "Revolver Case": 1,
    "Operation Vanguard Weapon Case": 1,
    "Breakout Weapon Case": 1,
    "Huntsman Weapon Case": 1,
    "Phoenix Weapon Case": 1,
    "Winter Offensive Weapon Case": 1,
    "CS:GO Weapon Case 3": 1,
    "CS:GO Weapon Case 2": 1,
    "CS:GO Weapon Case": 1,
}


@dataclass(frozen=True)
class RewardEvaluation:
    """Evaluation result for a single Armory Pass reward."""

    market_hash_name: str
    credits_required: int
    price: float | None           # latest known price; None = no data
    net_proceeds: float | None    # price * STEAM_NET_MULTIPLIER; None if price unknown
    credit_value: float | None    # net / credits_required; None if price unknown
    confidence: str               # "HIGH" | "MEDIUM" | "LOW" | "UNKNOWN"
    is_volatile: bool             # True when |Z-score(current vs history)| > 2.0


@dataclass(frozen=True)
class PassEfficiencyResult:
    """Aggregate result of a full Armory Pass efficiency evaluation."""

    pass_cost: float
    rewards: list[RewardEvaluation]   # one entry per catalog item
    total_credits_cost: int           # Σ credits_required across all rewards
    total_net_proceeds: float | None  # None if any reward price is unknown
    total_roi: float | None           # (total_net - pass_cost) / pass_cost; None if incomplete
    overall_confidence: str           # minimum confidence level across all rewards


class ArmoryAdvisor:
    """
    Stateless advisor — reads prices from an injected PriceRepository.

    Parameters
    ----------
    price_repo:
        Any object implementing get_latest_price(name) → PriceSnapshotDTO | None
        and get_price_history(name) → list[dict].
        Typically SqlAlchemyPriceRepository with an open session.
    reward_catalog:
        Mapping of market_hash_name → credits_required.
        Defaults to DEFAULT_REWARD_CATALOG when None.
    """

    _VOLATILE_Z_THRESHOLD: float = 2.0
    _HIGH_CONFIDENCE_MIN_POINTS: int = 30
    _MEDIUM_CONFIDENCE_MIN_POINTS: int = 7

    def __init__(self, price_repo, reward_catalog: dict[str, int] | None = None) -> None:
        self._price_repo = price_repo
        self._catalog = reward_catalog if reward_catalog is not None else DEFAULT_REWARD_CATALOG

    # ── public API ────────────────────────────────────────────────────────────

    def get_pass_efficiency(self, pass_cost: float) -> PassEfficiencyResult:
        """
        Evaluate all catalog rewards against current prices.

        Returns PassEfficiencyResult.  total_roi is None when any reward has
        an unknown price (incomplete data precludes a reliable ROI figure).
        """
        rewards = [
            self._evaluate_reward(name, credits)
            for name, credits in self._catalog.items()
        ]

        total_credits = sum(r.credits_required for r in rewards)

        # total_net: None if any reward lacks a price.
        if any(r.net_proceeds is None for r in rewards):
            total_net: float | None = None
        else:
            total_net = sum(r.net_proceeds for r in rewards)  # type: ignore[misc]

        if total_net is None or pass_cost == 0.0:
            total_roi: float | None = None
        else:
            total_roi = (total_net - pass_cost) / pass_cost

        # overall_confidence = minimum confidence across rewards.
        min_rank = min((_CONFIDENCE_RANK[r.confidence] for r in rewards), default=0)
        overall_confidence = next(
            k for k, v in _CONFIDENCE_RANK.items() if v == min_rank
        )

        return PassEfficiencyResult(
            pass_cost=pass_cost,
            rewards=rewards,
            total_credits_cost=total_credits,
            total_net_proceeds=total_net,
            total_roi=total_roi,
            overall_confidence=overall_confidence,
        )

    # ── internal helpers ──────────────────────────────────────────────────────

    def _evaluate_reward(self, name: str, credits: int) -> RewardEvaluation:
        snapshot = self._price_repo.get_latest_price(name)
        if snapshot is None:
            return RewardEvaluation(
                market_hash_name=name,
                credits_required=credits,
                price=None,
                net_proceeds=None,
                credit_value=None,
                confidence="UNKNOWN",
                is_volatile=False,
            )

        price = snapshot.price
        net = price * STEAM_NET_MULTIPLIER
        credit_val = net / credits if credits > 0 else None

        history = self._price_repo.get_price_history(name)
        confidence, is_volatile = self._assess_confidence(history, price)

        return RewardEvaluation(
            market_hash_name=name,
            credits_required=credits,
            price=price,
            net_proceeds=net,
            credit_value=credit_val,
            confidence=confidence,
            is_volatile=is_volatile,
        )

    def _assess_confidence(
        self, history: list[dict], current_price: float
    ) -> tuple[str, bool]:
        """
        Derive confidence level and volatility flag from price history.

        Confidence:
            HIGH   ≥ 30 points AND not volatile
            MEDIUM ≥ 7 points  OR  not volatile (but not HIGH)
            LOW    otherwise (< 7 points and volatile, or < 7 with no Z-score)
        Volatility: |Z-score(current vs historical mean)| > 2.0
                    Requires ≥ 2 historical points and std > 0.
        """
        n = len(history)
        if n == 0:
            return "UNKNOWN", False

        is_volatile = False
        if n >= 2:
            prices = [float(h["price"]) for h in history]
            mean = sum(prices) / n
            variance = sum((p - mean) ** 2 for p in prices) / n
            std = math.sqrt(variance)
            if std > 0:
                z = abs(current_price - mean) / std
                is_volatile = z > self._VOLATILE_Z_THRESHOLD

        if n >= self._HIGH_CONFIDENCE_MIN_POINTS and not is_volatile:
            confidence = "HIGH"
        elif n >= self._MEDIUM_CONFIDENCE_MIN_POINTS or not is_volatile:
            confidence = "MEDIUM"
        else:
            confidence = "LOW"

        return confidence, is_volatile
