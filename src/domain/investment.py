"""
Investment signal engine.

Answers: should you BUY, HOLD, or SELL *the container itself* as a tradeable asset?
This is completely separate from the EV/opening model.

Decision logic (two independent signals combined):

  1. Price vs baseline
       ratio = current_price / baseline_price
       baseline = base_cost − key_price  (weapon cases)
               = base_cost               (capsules, no key needed)
       ratio < 0.85  → cheap  (+1 buy point)
       ratio > 1.20  → expensive (+1 sell point)

  2. Price momentum (Steam median vs historical mean)
       momentum = (median − mean) / mean
       momentum < −0.05  → price falling  (+1 buy point)
       momentum > +0.08  → price rising / spiking (+1 sell point)

  Final verdict:
       2 buy points  → BUY
       1 buy point   → LEAN BUY
       2 sell points → SELL
       1 sell point  → LEAN SELL
       0 points      → HOLD

All prices are plain floats — no currency metadata.
"""

from __future__ import annotations

import math

from config import settings
from src.domain.events import LiquidityWarning
from src.domain.specifications import VolumeAbove

# Price-ratio thresholds for buy/sell signal
_BUY_RATIO_THRESHOLD = 0.85  # price < 85% of baseline  → cheap (buy point)
_SELL_RATIO_THRESHOLD = 1.20  # price > 120% of baseline → expensive (sell point)

# Momentum thresholds (current vs 30-day mean, in %)
_BUY_MOMENTUM_THRESHOLD = -5.0  # falling > 5%  → buy point
_SELL_MOMENTUM_THRESHOLD = 8.0  # rising  > 8%  → sell point

_CAPSULE_TYPES = {
    "Sticker Capsule",
    "Autograph Capsule",
    "Event Capsule",
}


def compute_investment_signal(
    container_name: str,
    base_cost: float,
    container_type: str,
    current_price: float | None,  # latest Steam Market median price
    mean_price: float | None = None,  # 30-day average from DB (for momentum)
    quantity: int = 0,  # listing volume
    is_event_matched: bool = False,  # True when container has an active event signal
) -> dict:
    """
    Returns investment signal dict for one container.

    Keys: verdict, current_price, baseline_price, price_ratio_pct,
          momentum_pct, quantity, score
    All price values are floats.
    """
    current = current_price
    mean = mean_price
    qty = quantity

    if not current or (isinstance(current, float) and math.isnan(current)):
        return {
            "verdict": "NO DATA",
            "current_price": None,
            "baseline_price": None,
            "price_ratio_pct": None,
            "momentum_pct": None,
            "quantity": 0,
            "score": 0,
        }

    # Baseline price
    is_weapon_case = container_type not in _CAPSULE_TYPES
    if is_weapon_case:
        baseline = max(base_cost - settings.key_price, 25.0)
    else:
        baseline = max(base_cost, 25.0)

    ratio = current / baseline
    ratio_pct = (ratio - 1.0) * 100

    # Momentum
    momentum_pct = 0.0
    if mean and mean > 0:
        momentum_pct = (current - mean) / mean * 100

    # Score: positive = buy pressure, negative = sell pressure
    buy_points = 0
    sell_points = 0

    # T13-T3-1: skip ratio signal entirely for very cheap containers (noisy signal)
    ratio_skip = current < settings.ratio_floor

    if not ratio_skip:
        if ratio < _BUY_RATIO_THRESHOLD:
            buy_points += 1
        elif ratio > _SELL_RATIO_THRESHOLD:
            sell_points += 1

    # T13-T3-3: event-matched containers use a higher momentum sell threshold
    _momentum_sell_threshold = (
        settings.momentum_event_threshold if is_event_matched else _SELL_MOMENTUM_THRESHOLD
    )

    if momentum_pct < _BUY_MOMENTUM_THRESHOLD:
        buy_points += 1
    elif momentum_pct > _momentum_sell_threshold:
        sell_points += 1

    score = buy_points - sell_points  # −2..+2

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


def compute_all_investment_signals(
    containers: list,  # list of DimContainer ORM objects
    price_data: dict[str, dict],  # {container_name: {current_price, mean_price, quantity}}
    positions_buy_price: dict[str, float] | None = None,  # {container_name: buy_price} T13-T3-2
) -> dict[str, dict]:
    """
    Returns {container_id: signal_dict} for every container.

    price_data comes from DB (Steam Market prices).
    SELL / LEAN SELL verdicts are annotated with sell_at_loss (bool) — True when
    net proceeds after Steam 15 % fee are below the container's base_cost.
    """
    _liq_spec = VolumeAbove(settings.liquidity_min_volume)
    result: dict[str, dict] = {}
    for c in containers:
        cid = str(c.container_id)
        name = str(c.container_name)
        pd = price_data.get(name, {})
        qty = int(pd.get("quantity", 0) or 0)

        sig = compute_investment_signal(
            container_name=name,
            base_cost=c.base_cost,
            container_type=c.container_type.value,
            current_price=pd.get("current_price"),
            mean_price=pd.get("mean_price"),
            quantity=qty,
        )

        # Liquidity Guard via Specification: suppress BUY signals for illiquid markets
        if (
            sig["verdict"] in ("BUY", "LEAN BUY")
            and sig.get("current_price")
            and not _liq_spec.is_satisfied_by({"volume": qty})
        ):
                from datetime import UTC, datetime

                reason = (
                    f"Insufficient liquidity: volume_24h={qty} < "
                    f"required={settings.liquidity_min_volume:.1f} units/day"
                )
                from infra.signal_handler import notify_liquidity_warning
                notify_liquidity_warning(
                    LiquidityWarning(
                        timestamp=datetime.now(UTC).replace(tzinfo=None),
                        item_name=name,
                        payload=reason,
                    )
                )
                sig = {**sig, "verdict": "HOLD", "liquidity_warning": reason}

        if sig["verdict"] in ("SELL", "LEAN SELL") and sig["current_price"]:
            net_proceeds = (
                sig["current_price"] / settings.steam_fee_divisor - settings.steam_fee_fixed
            )
            # T13-T3-2: use user's actual buy price when available, else fall back to MSRP
            cost_basis = (
                positions_buy_price.get(name) if positions_buy_price else None
            ) or float(c.base_cost)
            sig["sell_at_loss"] = net_proceeds < cost_basis
        result[cid] = sig
    return result
