"""
Armory Pass cost-benefit engine.

Answers: is it cheaper to buy a container from the Steam Market, or to earn
it via an Armory Pass?

Business logic:
  effective_pass_cost = pass_cost / stars_in_pass * stars_per_case
  net_market_proceeds = market_price / steam_fee_divisor - steam_fee_fixed

  If net_market_proceeds > effective_pass_cost:
      recommendation = "MARKET"   (sell on market, then re-buy cheaper via Pass)
  Else:
      recommendation = "PASS"     (earn via Pass is better value)

  margin (ROI) = (net_market_proceeds - effective_pass_cost)
                 / effective_pass_cost
                 (positive = market is better; negative = Pass is better)

The function is a pure calculation with no I/O — all parameters are explicit.
"""

from __future__ import annotations

from dataclasses import dataclass

from config import settings
from domain.value_objects import Amount, ROI


@dataclass(frozen=True)
class ArmoryPassResult:
    """Result of a single Armory Pass vs. market comparison."""

    container_name: str
    market_price: float
    pass_cost: float
    stars_in_pass: int
    stars_per_case: int

    # Derived — monetary values use Amount (round-half-up), efficiency uses ROI (ratio)
    effective_pass_cost: Amount  # how much 1 container costs via Pass
    net_market_proceeds: Amount  # what you keep after selling 1 unit on Steam Market
    margin_pct: ROI              # (net - pass_cost) / pass_cost as ratio; >0 = market better
    recommendation: str          # "MARKET" or "PASS"
    message: str


def compare_armory_pass(
    *,
    container_name: str,
    market_price: float,
    pass_cost: float,
    stars_in_pass: int,
    stars_per_case: int,
    steam_fee_divisor: float = 1.15,
    steam_fee_fixed: float = 5.0,
) -> ArmoryPassResult:
    """Compare earning a container via Armory Pass vs. buying/selling on Steam Market.

    Parameters
    ----------
    container_name:
        Display name of the container (used in the result message only).
    market_price:
        Current Steam Market price of the container.
    pass_cost:
        Price of the Armory Pass.
    stars_in_pass:
        Total stars granted by the pass (e.g. 5 for the standard Armory Pass).
    stars_per_case:
        Stars required to earn one container (e.g. 1 for most cases).
    steam_fee_divisor:
        Steam Market fee divisor (default 1.15 = 15% fee).
    steam_fee_fixed:
        Steam Market fixed per-transaction fee (default 5 = $0.01 equivalent).

    Returns
    -------
    ArmoryPassResult
        Frozen dataclass with all inputs, derived values, and recommendation.

    Raises
    ------
    ValueError
        If stars_in_pass <= 0 or stars_per_case <= 0 or market_price < 0
        or pass_cost < 0 or steam_fee_divisor <= 0.
    """
    if stars_in_pass <= 0:
        raise ValueError(f"stars_in_pass must be > 0, got {stars_in_pass}")
    if stars_per_case <= 0:
        raise ValueError(f"stars_per_case must be > 0, got {stars_per_case}")
    if market_price < 0:
        raise ValueError(f"market_price must be >= 0, got {market_price}")
    if pass_cost < 0:
        raise ValueError(f"pass_cost must be >= 0, got {pass_cost}")
    if steam_fee_divisor <= 0:
        raise ValueError(f"steam_fee_divisor must be > 0, got {steam_fee_divisor}")
    if stars_per_case > stars_in_pass:
        raise ValueError(
            f"stars_per_case ({stars_per_case}) cannot exceed stars_in_pass ({stars_in_pass})"
        )

    # Precise float arithmetic for recommendation logic (rounding must not flip decisions)
    effective_float = pass_cost / stars_in_pass * stars_per_case
    net_float = market_price / steam_fee_divisor - steam_fee_fixed

    if effective_float == 0:
        # Free pass edge case: market is always strictly better if price > 0
        margin_ratio = float("inf") if net_float > 0 else 0.0
    else:
        margin_ratio = (net_float - effective_float) / effective_float

    if net_float > effective_float:
        recommendation = "MARKET"
        message = (
            f"Market is better by {abs(margin_ratio) * 100:.1f}% — "
            f"net proceeds {int(net_float):,}{settings.currency_symbol} > pass cost {int(effective_float):,}{settings.currency_symbol}"
        )
    else:
        recommendation = "PASS"
        message = (
            f"Pass is better by {abs(margin_ratio) * 100:.1f}% — "
            f"pass cost {int(effective_float):,}{settings.currency_symbol} <= net market {int(net_float):,}{settings.currency_symbol}"
        )

    return ArmoryPassResult(
        container_name=container_name,
        market_price=market_price,
        pass_cost=pass_cost,
        stars_in_pass=stars_in_pass,
        stars_per_case=stars_per_case,
        effective_pass_cost=Amount(effective_float),
        net_market_proceeds=Amount(net_float),
        margin_pct=ROI(margin_ratio),
        recommendation=recommendation,
        message=message,
    )
