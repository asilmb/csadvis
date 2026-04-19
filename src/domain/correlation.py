"""
Correlation matrix for CS2 containers.

Computes pairwise Pearson correlation of daily prices using Steam Market
backfill history. Used to warn users when Portfolio Advisor recommends
assets that move together (concentrated risk).

Threshold: |r| > 0.70 = highly correlated (same market driver).
"""

from __future__ import annotations

import math
import threading
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta

_HIGH_CORR_THRESHOLD = 0.70
_CACHE_TTL_HOURS = 4
_MIN_SAMPLE_PEARSON = 30  # raised from 10: need 30 shared dates for reliable Pearson r

# In-process cache: (result, computed_at)
# Lock protects against race conditions when scheduler and Dash callbacks share the cache.
_matrix_cache: dict[int, tuple[dict, datetime]] = {}
_matrix_cache_lock = threading.Lock()


def _cache_key(price_history: dict[str, list[dict]]) -> int:
    """Stable cache key from container IDs + total row count."""
    return hash((frozenset(price_history.keys()), sum(len(v) for v in price_history.values())))


def _resample_pair(
    series_i: dict[str, float],
    series_j: dict[str, float],
) -> tuple[list[float], list[float]]:
    """
    Build forward-filled daily price vectors for two series over their overlap range.

    Only days where at least one series has a real DB entry are included — this
    prevents correlating pure forward-fill "silence" on both sides simultaneously.
    Returns two equal-length price lists suitable for _to_log_returns().
    """
    if not series_i or not series_j:
        return [], []

    dates_i = sorted(series_i)
    dates_j = sorted(series_j)

    start = max(dates_i[0], dates_j[0])
    end = min(dates_i[-1], dates_j[-1])
    if start > end:
        return [], []

    prices_i: list[float] = []
    prices_j: list[float] = []
    last_i: float | None = None
    last_j: float | None = None

    current = date.fromisoformat(start)
    end_date = date.fromisoformat(end)

    while current <= end_date:
        d = current.isoformat()
        real_i = d in series_i
        real_j = d in series_j

        if real_i:
            last_i = series_i[d]
        if real_j:
            last_j = series_j[d]

        # Include day only when at least one series has a real data point
        # and both have an established forward-filled value.
        if (real_i or real_j) and last_i is not None and last_j is not None:
            prices_i.append(last_i)
            prices_j.append(last_j)

        current += timedelta(days=1)

    return prices_i, prices_j


def _to_log_returns(prices: list[float]) -> list[float]:
    """Convert a price series to log-returns: ln(P_t / P_{t-1}).

    Points where either price is zero (bad data / delisted item) are dropped
    entirely rather than filled with 0.0 — a synthetic zero-return would
    bias the Pearson r toward zero and mask real correlations.
    """
    return [
        math.log(prices[i] / prices[i - 1])
        for i in range(1, len(prices))
        if prices[i - 1] > 0 and prices[i] > 0
    ]


def _pearson(xs: list[float], ys: list[float]) -> float | None:
    """Pearson r for two equal-length lists. Returns None if insufficient data."""
    n = len(xs)
    if n < _MIN_SAMPLE_PEARSON:
        return None
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    num = sum((xs[i] - mean_x) * (ys[i] - mean_y) for i in range(n))
    den_x = math.sqrt(sum((x - mean_x) ** 2 for x in xs))
    den_y = math.sqrt(sum((y - mean_y) ** 2 for y in ys))
    if den_x == 0 or den_y == 0:
        return None
    return num / (den_x * den_y)


def compute_correlation_matrix(
    price_history: dict[str, list[dict]],  # {container_id: [{timestamp, price}]}
    id_to_name: dict[str, str],  # {container_id: container_name}
    use_cache: bool = True,
) -> dict:
    """
    Compute pairwise Pearson correlation of daily prices.

    Returns:
        names  : list of container names (same order as matrix rows/cols)
        matrix : 2D list[list[float|None]]  — correlation values
        pairs  : list of (name1, name2, r) sorted by |r| descending
    """
    # Check in-process cache (4h TTL) before computing (lock for thread safety)
    if use_cache:
        key = _cache_key(price_history)
        with _matrix_cache_lock:
            if key in _matrix_cache:
                cached_result, cached_at = _matrix_cache[key]
                if datetime.now(UTC).replace(tzinfo=None) - cached_at < timedelta(
                    hours=_CACHE_TTL_HOURS
                ):
                    return cached_result

    # Build {container_id: {date_str: price}} from history
    series: dict[str, dict[str, float]] = {}
    for cid, rows in price_history.items():
        if not rows:
            continue
        daily: dict[str, float] = {}
        for h in rows:
            ts = h.get("timestamp", "")
            date_str = ts[:10] if ts else ""
            if date_str and h.get("price"):
                # Keep last price per day
                daily[date_str] = float(h["price"])
        if len(daily) >= _MIN_SAMPLE_PEARSON:
            series[cid] = daily

    # All dates that appear in at least 2 series
    date_count: dict[str, int] = defaultdict(int)
    for daily in series.values():
        for d in daily:
            date_count[d] += 1
    common_dates = sorted(d for d, cnt in date_count.items() if cnt >= 2)

    if not common_dates:
        return {"names": [], "matrix": [], "pairs": []}

    cids = list(series.keys())
    names = [id_to_name.get(cid, cid[:8]) for cid in cids]
    n = len(cids)

    # Matrix computation
    matrix: list[list[float | None]] = [[None] * n for _ in range(n)]
    pairs: list[tuple[str, str, float]] = []

    for i in range(n):
        matrix[i][i] = 1.0
        for j in range(i + 1, n):
            # Build forward-filled daily price vectors over the overlap range.
            # _MIN_SAMPLE_PEARSON+1 prices → _MIN_SAMPLE_PEARSON log-returns.
            pi, pj = _resample_pair(series[cids[i]], series[cids[j]])
            if len(pi) <= _MIN_SAMPLE_PEARSON:
                matrix[i][j] = matrix[j][i] = None
                continue
            xs = _to_log_returns(pi)
            ys = _to_log_returns(pj)
            r = _pearson(xs, ys)
            matrix[i][j] = matrix[j][i] = r
            if r is not None:
                pairs.append((names[i], names[j], round(r, 3)))

    pairs.sort(key=lambda p: abs(p[2]), reverse=True)

    result = {
        "names": names,
        "matrix": [[round(v, 3) if v is not None else None for v in row] for row in matrix],
        "pairs": pairs,
    }

    # Store in cache (lock for thread safety)
    if use_cache:
        with _matrix_cache_lock:
            _matrix_cache[_cache_key(price_history)] = (
                result,
                datetime.now(UTC).replace(tzinfo=None),
            )

    return result


def check_portfolio_correlation(
    name1: str | None,
    name2: str | None,
    pairs: list[tuple[str, str, float]],
    threshold: float = _HIGH_CORR_THRESHOLD,
) -> str | None:
    """Return a warning string when |r| >= threshold for (name1, name2), else None."""
    if not name1 or not name2 or name1 == name2:
        return None
    for n1, n2, r in pairs:
        if (n1 == name1 and n2 == name2) or (n1 == name2 and n2 == name1):
            if abs(r) >= threshold:
                return f"{name1} и {name2}: высокая корреляция (r = {r:.2f})"
    return None
