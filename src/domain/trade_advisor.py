"""
Trade Advisor — specific buy/sell price targets for each container.

Uses Steam Market price history from the local DB.

Logic:
  buy_target  = 20th percentile of 90-day price history
                (or baseline × 0.85 if fewer than 5 snapshots)
  sell_target = 70th percentile of 90-day price history
                (or baseline × 1.20)

Net margin accounts for Steam's 15% seller fee (5% platform + 10% CS2).

Hold detail:
  "SELL at X₸ when price reaches target"  — clear exit level
  "UNCERTAIN — near breakeven, wait"       — spread too tight to act
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

from config import settings

logger = logging.getLogger(__name__)

_STEAM_FEE_DIV = settings.steam_fee_divisor  # 1.15 — overridable via .env
_STEAM_MIN_FEE = settings.steam_fee_fixed  # ~5₸ — overridable via .env
_KEY_PRICE = settings.key_price  # 1200₸ — overridable via .env
_CAPSULE_TYPES = {"Sticker Capsule", "Autograph Capsule", "Event Capsule"}
_MIN_NET_MARGIN = 0.05  # below 5% net → UNCERTAIN


def _percentile(sorted_vals: list[float], pct: float) -> float:
    """Linear interpolation percentile on a sorted list."""
    if not sorted_vals:
        return 0.0
    k = (len(sorted_vals) - 1) * pct / 100.0
    lo = int(k)
    hi = min(lo + 1, len(sorted_vals) - 1)
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (k - lo)


def compute_trade_advice(
    container_name: str,
    base_cost: float,
    container_type: str,
    price_history_rows: list[dict],  # from _get_price_history(): {timestamp, price, ...}
) -> dict:
    """
    Returns trade advice dict:
      buy_target, sell_target, net_margin_pct,
      hold_detail, data_source, baseline
    """
    # ── Baseline ──────────────────────────────────────────────────────────────
    is_weapon_case = container_type not in _CAPSULE_TYPES
    if is_weapon_case:
        baseline = max(base_cost - _KEY_PRICE, 25.0)
    else:
        baseline = max(base_cost, 25.0)

    # ── 90-day price history ───────────────────────────────────────────────────
    cutoff = datetime.now(UTC).replace(tzinfo=None) - timedelta(days=90)
    prices_90d: list[float] = []
    for row in price_history_rows:
        try:
            ts = row.get("timestamp", "")
            if len(ts) == 16:
                dt = datetime.strptime(ts, "%Y-%m-%d %H:%M")
            else:
                dt = datetime.fromisoformat(ts[:19])
            if dt >= cutoff and row.get("price"):
                prices_90d.append(float(row["price"]))
        except (ValueError, TypeError, KeyError):
            logger.debug("compute_trade_advice: skipping malformed row %r", row)
            continue

    prices_90d.sort()
    data_source = "90d_steam"

    if len(prices_90d) >= 5:
        buy_target = _percentile(prices_90d, 20)
        # 90th percentile gives enough headroom to cover Steam's 15% fee and
        # still yield a positive net margin (70th was too close to buy_target).
        sell_target = _percentile(prices_90d, 90)
        # Cap prevents chasing historical outliers; raised to 1.40 so the
        # breakeven spread (~17.25%) + 3% net margin is actually achievable.
        sell_target = min(sell_target, buy_target * settings.flip_sell_target_cap)
    else:
        # Fallback: use baseline with margins
        buy_target = round(baseline * 0.85)
        sell_target = round(baseline * 1.20)
        data_source = "baseline_fallback"

    # ── Net margin after Steam's 15% fee ──────────────────────────────────────
    # Steam formula: seller receives sell_target / 1.15 − fixed_fee
    # (platform 5% + CS2 game 10%, deducted from buyer's price)
    net_proceeds = sell_target / _STEAM_FEE_DIV - _STEAM_MIN_FEE
    net_margin_pct = (net_proceeds - buy_target) / buy_target * 100 if buy_target > 0 else 0.0

    # ── Hold detail ────────────────────────────────────────────────────────────
    if net_margin_pct < _MIN_NET_MARGIN * 100:
        hold_detail = "UNCERTAIN — spread too tight, wait for better entry"
    else:
        hold_detail = (
            f"SELL at {int(sell_target):,}{settings.currency_symbol} for +{net_margin_pct:.0f}% net (after Steam 15%)"
        )

    return {
        "buy_target": int(buy_target),
        "sell_target": int(sell_target),
        "net_margin_pct": round(net_margin_pct, 1),
        "hold_detail": hold_detail,
        "data_source": data_source,
        "baseline": round(baseline),
    }
