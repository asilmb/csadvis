"""
Oscillation pattern detector for CS2 containers.

Detects whether a container's price shows a regular oscillation pattern
suitable for flip trading. An oscillating asset moves up and down around
its mean with consistent swings — this is the core flip opportunity.

Key metric: direction reversals relative to mean.
"""

from __future__ import annotations

# Minimum % swing from mean to count as a real reversal (noise filter)
_MIN_SWING_PCT = 0.05  # 5%
# Minimum number of reversals in the window to qualify as "oscillating"
_MIN_REVERSALS = 3


def detect_oscillation(prices: list[float]) -> dict:
    """
    Analyze price history for oscillation pattern.

    Args:
        prices: chronologically ordered price list (at least 10 points recommended)

    Returns dict with:
        reversal_count   : int    — number of direction changes (above noise threshold)
        avg_swing_pct    : float  — average swing magnitude as fraction of mean price
        is_oscillating   : bool   — True if reversal_count >= _MIN_REVERSALS
        mean_price       : float  — mean price over the window

    >>> result = detect_oscillation([1.0, 1.2, 0.9, 1.15, 0.85, 1.1, 0.8, 1.2, 0.9, 1.1])
    >>> result['is_oscillating']
    True
    >>> result['reversal_count'] >= 3
    True
    >>> result = detect_oscillation([1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9])
    >>> result['is_oscillating']
    False
    """
    if not prices:
        return {
            "reversal_count": 0,
            "avg_swing_pct": 0.0,
            "is_oscillating": False,
            "mean_price": 0.0,
        }

    mean_price = sum(prices) / len(prices)

    if mean_price == 0.0:
        return {
            "reversal_count": 0,
            "avg_swing_pct": 0.0,
            "is_oscillating": False,
            "mean_price": 0.0,
        }

    # Determine above/below mean for each price, filtered by noise threshold.
    # None means the price is within the noise band — not classified.
    def _side(price: float) -> int | None:
        swing = (price - mean_price) / mean_price
        if swing > _MIN_SWING_PCT:
            return 1   # above mean
        if swing < -_MIN_SWING_PCT:
            return -1  # below mean
        return None    # inside noise band

    # Count reversals: transitions between above and below mean that exceed
    # the noise threshold. Skip points inside the noise band.
    reversal_count = 0
    last_side: int | None = None

    for p in prices:
        current_side = _side(p)
        if current_side is None:
            continue
        if last_side is not None and current_side != last_side:
            reversal_count += 1
        last_side = current_side

    # avg_swing_pct: mean of |price - mean| / mean across all points
    avg_swing_pct = sum(abs(p - mean_price) / mean_price for p in prices) / len(prices)

    return {
        "reversal_count": reversal_count,
        "avg_swing_pct": round(avg_swing_pct, 4),
        "is_oscillating": reversal_count >= _MIN_REVERSALS,
        "mean_price": round(mean_price, 4),
    }
