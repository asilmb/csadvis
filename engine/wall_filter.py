"""
Wall filter — computes sell wall metrics from Steam Market order book data.

All functions are pure (no I/O, no HTTP, no DB) and fully unit-testable.

Used to determine whether the sell wall between the current price and the
flip target price is thin enough to exit within wall_max_days.
"""

from __future__ import annotations

import math

from config import settings


def compute_wall_metrics(
    sell_order_graph: list,
    current_price: float,
    target_price: float,
    avg_daily_vol: float,
) -> dict:
    """
    Compute sell wall metrics between current_price and target_price.

    sell_order_graph: list of [price, qty, description] entries from
        Steam Market itemordershistogram API. Each entry represents a price
        level with cumulative quantity.

    Algorithm:
        Sum qty for all price levels where current_price <= price <= target_price.
        estimated_days = volume_to_target / avg_daily_vol
        passes_wall_filter = estimated_days <= settings.wall_max_days

    Returns:
        {
            "volume_to_target": int,     cumulative qty in sell wall [current, target]
            "estimated_days": float,     volume_to_target / avg_daily_vol
            "passes_wall_filter": bool,  estimated_days <= wall_max_days
        }

    Edge cases:
        - Empty sell_order_graph      → volume=0, estimated_days=0.0, passes=True
        - avg_daily_vol=0, volume>0   → estimated_days=inf, passes=False
        - avg_daily_vol=0, volume=0   → estimated_days=0.0, passes=True
        - target_price <= current     → volume=0 (nothing to absorb), passes=True
    """
    volume_to_target = 0

    for entry in sell_order_graph:
        try:
            price = float(entry[0])
            qty = int(entry[1])
        except (IndexError, TypeError, ValueError):
            continue

        if current_price <= price <= target_price:
            volume_to_target += qty

    if volume_to_target == 0:
        estimated_days = 0.0
    elif avg_daily_vol <= 0:
        estimated_days = math.inf
    else:
        estimated_days = volume_to_target / avg_daily_vol

    passes = False if math.isinf(estimated_days) else estimated_days <= settings.wall_max_days

    return {
        "volume_to_target": volume_to_target,
        "estimated_days": estimated_days,
        "passes_wall_filter": passes,
    }


def get_best_buy_order(buy_order_graph: list) -> float:
    """
    Return the highest buy order price (best bid = immediate liquidation price).

    buy_order_graph is sorted descending by price — the first entry is the
    best (highest) bid available for immediate sale.

    Returns 0.0 if buy_order_graph is empty or unparseable.
    """
    if not buy_order_graph:
        return 0.0
    try:
        return float(buy_order_graph[0][0])
    except (IndexError, TypeError, ValueError):
        return 0.0
