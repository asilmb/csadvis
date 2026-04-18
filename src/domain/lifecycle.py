"""
Lifecycle stage classifier for CS2 containers.

Determines which trading stage a container is in based on its age
and current price/volume behavior. Used to gate flip and invest recommendations.

Stages
------
NEW     0–6 months    high volatility, speculative — don't flip
ACTIVE  6–24 months   main trading zone, good liquidity — primary flip candidate
AGING   2–5 years     interest falling, patterns more predictable — cautious
LEGACY  5+ years      stable, investment class — INVEST only, not FLIP
DEAD    any age       detected by price/volume criteria — ban list

DEAD is checked first and overrides the age-based classification.

DEAD criteria (any one is sufficient):
  1. Current price is within 5 % of the all-time historical minimum.
  2. No price movement: (max − min) / mean < 3 % over last 30 days.
  3. Declining volume:  avg_vol_7d < avg_vol_30d * 0.50.

Usage example::

    >>> from datetime import date
    >>> today = date(2025, 6, 1)
    >>> first_seen = date(2023, 6, 1)          # 2 years ago → AGING
    >>> prices_30d = [150.0] * 30              # flat price, no movement
    >>> vol_7d, vol_30d = 5.0, 20.0            # declining volume → DEAD
    >>> all_time_prices = [100.0, 200.0, 150.0]
    >>> classify_lifecycle(first_seen, today, prices_30d, vol_7d, vol_30d, all_time_prices)
    <LifecycleStage.DEAD: 'DEAD'>

    >>> vol_7d_ok, vol_30d_ok = 18.0, 20.0    # healthy volume
    >>> prices_30d_moving = list(range(130, 160))  # clear movement
    >>> classify_lifecycle(first_seen, today, prices_30d_moving, vol_7d_ok, vol_30d_ok, all_time_prices)
    <LifecycleStage.AGING: 'AGING'>
"""

from __future__ import annotations

from datetime import date
from enum import StrEnum

# ─── Stage thresholds (days) ──────────────────────────────────────────────────

_NEW_MAX_DAYS = 180        # 0–6 months
_ACTIVE_MAX_DAYS = 730     # 6–24 months  (2 years)
_AGING_MAX_DAYS = 1825     # 2–5 years    (5 years)
# >= 1825 days → LEGACY

# ─── DEAD detection thresholds ────────────────────────────────────────────────

_DEAD_ATH_MIN_PCT = 0.05          # within 5 % of all-time minimum
_DEAD_NO_MOVEMENT_PCT = 0.03      # (max − min) / mean < 3 % over 30 days
_DEAD_VOLUME_DECLINE_RATIO = 0.50  # avg_vol_7d < avg_vol_30d * 50 %


# ─── Stage enum ───────────────────────────────────────────────────────────────


class LifecycleStage(StrEnum):
    NEW = "NEW"
    ACTIVE = "ACTIVE"
    AGING = "AGING"
    LEGACY = "LEGACY"
    DEAD = "DEAD"


# ─── DEAD detection helpers ───────────────────────────────────────────────────


def _is_near_all_time_min(current_price: float, all_time_prices: list[float]) -> bool:
    """Return True when current_price is within 5 % of the all-time minimum."""
    if not all_time_prices:
        return False
    atl = min(all_time_prices)
    if atl <= 0:
        return False
    return current_price <= atl * (1.0 + _DEAD_ATH_MIN_PCT)


def _has_no_price_movement(prices_30d: list[float]) -> bool:
    """Return True when price range over last 30 days is less than 3 % of the mean."""
    if not prices_30d:
        return False
    mean = sum(prices_30d) / len(prices_30d)
    if mean <= 0:
        return False
    movement = (max(prices_30d) - min(prices_30d)) / mean
    return movement < _DEAD_NO_MOVEMENT_PCT


def _has_declining_volume(vol_7d: float, vol_30d_avg: float) -> bool:
    """Return True when 7-day avg volume is less than 50 % of 30-day avg volume."""
    if vol_30d_avg <= 0:
        return False
    return vol_7d < vol_30d_avg * _DEAD_VOLUME_DECLINE_RATIO


def _is_dead(
    current_price: float,
    prices_30d: list[float],
    vol_7d: float,
    vol_30d_avg: float,
    all_time_prices: list[float],
) -> bool:
    """Return True when any DEAD criterion is met."""
    return (
        _is_near_all_time_min(current_price, all_time_prices)
        or _has_no_price_movement(prices_30d)
        or _has_declining_volume(vol_7d, vol_30d_avg)
    )


# ─── Public API ───────────────────────────────────────────────────────────────


def classify_lifecycle(
    first_seen_date: date,       # earliest date in price history
    current_date: date,          # today
    prices_30d: list[float],     # prices in last 30 days (chronological)
    vol_7d: float,               # avg daily volume last 7 days
    vol_30d_avg: float,          # avg daily volume last 30 days
    all_time_prices: list[float], # all historical prices
) -> LifecycleStage:
    """
    Classify a container into a LifecycleStage.

    DEAD is evaluated before age-based classification — a container of any age
    can be marked DEAD if its price/volume behaviour matches the criteria.

    Args:
        first_seen_date:  Earliest date in the price history.
        current_date:     Reference date for age calculation (typically today).
        prices_30d:       Chronological list of prices over the last 30 days.
        vol_7d:           Average daily trading volume for the last 7 days.
        vol_30d_avg:      Average daily trading volume for the last 30 days.
        all_time_prices:  Complete historical price list (used for ATL detection).

    Returns:
        The matching LifecycleStage enum member.
    """
    current_price = prices_30d[-1] if prices_30d else 0.0

    # DEAD check takes priority over everything else
    if _is_dead(current_price, prices_30d, vol_7d, vol_30d_avg, all_time_prices):
        return LifecycleStage.DEAD

    age_days = (current_date - first_seen_date).days

    if age_days < _NEW_MAX_DAYS:
        return LifecycleStage.NEW
    if age_days < _ACTIVE_MAX_DAYS:
        return LifecycleStage.ACTIVE
    if age_days < _AGING_MAX_DAYS:
        return LifecycleStage.AGING
    return LifecycleStage.LEGACY


def is_flip_eligible(stage: LifecycleStage) -> bool:
    """Return True when the stage is suitable for flip trading.

    ACTIVE is the primary flip candidate; AGING is cautious-ok.
    NEW is too volatile, LEGACY is investment-only, DEAD is banned.
    """
    return stage in (LifecycleStage.ACTIVE, LifecycleStage.AGING)


def is_invest_eligible(stage: LifecycleStage) -> bool:
    """Return True when the stage is suitable for long-term investment.

    AGING and LEGACY stages are invest candidates.
    """
    return stage in (LifecycleStage.AGING, LifecycleStage.LEGACY)
