"""
LC-1: Statistical pruning of unviable containers.

Evaluates whether a container has crossed the 95 %-failure threshold and is a
candidate for `is_blacklisted = 1`. Designed to be called per-container, on
demand, from the existing manual analysis flow (CLI / API request) — no cron,
no bulk update.

Decision algebra (intersection of all three hard floors):
  1. Price floor:     P_t ≤ 1.05 × ATL
  2. Volatility:      σ_30 < R_target / Φ⁻¹(0.95)         ≈ 0.1145
                      where R_target = ln(1.2075) ≈ 0.1885 (20.75 % hurdle)
  3. Liquidity:       V_7d / V_30d < 0.50  AND  V_7d < LIQUIDITY_MIN_VOLUME

False-positive guards (any one cancels pruning):
  • MWSI < 0.80           — market-wide stagnation, not container-specific decay
  • Cyclical autocorr     — significant ACF peak in last 365 days projecting
                            into the next 60-day window
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum

import numpy as np
from sqlalchemy import text
from sqlalchemy.orm import Session

from config import settings

logger = logging.getLogger(__name__)


# ─── Math constants (research-derived; comment the formula) ───────────────────

# 20.75 % hurdle = (1.15 fee) × (1.05 min margin) − 1
_HURDLE_PCT = 0.2075
_R_TARGET = math.log(1.0 + _HURDLE_PCT)               # ≈ 0.1885
_PHI_INV_95 = 1.6449                                  # Φ⁻¹(0.95)
_SIGMA_FLOOR = _R_TARGET / _PHI_INV_95                # ≈ 0.1145

_PRICE_NEAR_ATL_FACTOR = 1.05                         # P_t ≤ 1.05 × ATL
_VOLUME_RATIO_FLOOR = 0.50                            # V_7d / V_30d
_MWSI_GUARD = 0.80                                    # market-wide stagnation cutoff
_AUTOCORR_LAG_DAYS = 60                               # cyclical look-ahead window
_AUTOCORR_SIGNIFICANCE = 0.30                         # |ACF| ≥ 0.3 → significant
_HISTORY_DAYS_REQUIRED = 30                           # need ≥ 30 days for σ_30


class PruneVerdict(StrEnum):
    PRUNE = "PRUNE"
    KEEP = "KEEP"
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"


@dataclass(frozen=True)
class PruneDecision:
    verdict: PruneVerdict
    reason: str
    metrics: dict


# ─── Public API ───────────────────────────────────────────────────────────────


def evaluate_prune_candidate(
    container_id: str,
    db: Session,
    *,
    liquidity_min_volume: float | None = None,
) -> PruneDecision:
    """Return PruneDecision for a single container. Read-only — caller decides
    whether to flip is_blacklisted = 1 based on verdict.
    """
    liq_floor = (
        liquidity_min_volume
        if liquidity_min_volume is not None
        else settings.liquidity_min_volume
    )

    metrics = _fetch_container_metrics(container_id, db)
    if metrics is None:
        return PruneDecision(
            verdict=PruneVerdict.INSUFFICIENT_DATA,
            reason="not_enough_history",
            metrics={"history_days": 0},
        )

    # Hard floor checks
    near_atl = (
        metrics["current_price"] is not None
        and metrics["all_time_low"] is not None
        and metrics["all_time_low"] > 0
        and metrics["current_price"] <= _PRICE_NEAR_ATL_FACTOR * metrics["all_time_low"]
    )
    low_volatility = (
        metrics["sigma_30"] is not None and metrics["sigma_30"] < _SIGMA_FLOOR
    )
    v7 = metrics["avg_vol_7d"]
    v30 = metrics["avg_vol_30d"]
    vol_ratio = v7 / v30 if v30 > 0 else 0.0
    # Zero v30 = strongest stagnation signal (not "no data"); v7 below absolute
    # liquidity floor combined with collapsed ratio confirms drying-up.
    low_liquidity = v7 < liq_floor and vol_ratio < _VOLUME_RATIO_FLOOR

    floors_failed = near_atl and low_volatility and low_liquidity

    metrics["near_atl"] = near_atl
    metrics["low_volatility"] = low_volatility
    metrics["low_liquidity"] = low_liquidity
    metrics["sigma_floor"] = _SIGMA_FLOOR
    metrics["r_target"] = _R_TARGET

    if not floors_failed:
        return PruneDecision(
            verdict=PruneVerdict.KEEP,
            reason="floors_not_breached",
            metrics=metrics,
        )

    # False-positive guards — only checked when pruning would otherwise fire
    mwsi = _compute_mwsi(db)
    metrics["mwsi"] = mwsi
    if mwsi is not None and mwsi < _MWSI_GUARD:
        return PruneDecision(
            verdict=PruneVerdict.KEEP,
            reason="market_wide_stagnation",
            metrics=metrics,
        )

    cyclical = _has_cyclical_autocorrelation(container_id, db)
    metrics["cyclical_protected"] = cyclical
    if cyclical:
        return PruneDecision(
            verdict=PruneVerdict.KEEP,
            reason="cyclical_history",
            metrics=metrics,
        )

    return PruneDecision(
        verdict=PruneVerdict.PRUNE,
        reason="all_floors_breached",
        metrics=metrics,
    )


# ─── Internal: fetch per-container metrics (one SELECT) ───────────────────────


def _fetch_container_metrics(container_id: str, db: Session) -> dict | None:
    """Return latest price, ATL, σ_30, V_7d, V_30d for a single container.

    Computes σ_30 and rolling means in Python rather than via window functions —
    one container × ~30–90 rows is negligible, and Python keeps the SQL portable
    across the SQLite/Postgres test split.
    """
    sql = text(
        """
        SELECT timestamp, price, volume_7d
        FROM fact_container_prices
        WHERE container_id = :cid
          AND timestamp >= :since
          AND price IS NOT NULL
        ORDER BY timestamp ASC
        """
    )
    cutoff = datetime.now(UTC).replace(tzinfo=None) - timedelta(days=120)
    rows = db.execute(sql, {"cid": container_id, "since": cutoff}).fetchall()
    if len(rows) < _HISTORY_DAYS_REQUIRED:
        return None

    prices = np.array([float(r.price) for r in rows], dtype=float)
    vols = np.array(
        [float(r.volume_7d) if r.volume_7d is not None else 0.0 for r in rows],
        dtype=float,
    )

    # ATL on full window we have (not "all time" strictly — bounded by 120d window;
    # acceptable trade-off vs. full table scan for one-shot manual analysis).
    atl = float(np.min(prices))
    current_price = float(prices[-1])

    # σ on log returns of last 30 obs
    last_30 = prices[-31:] if prices.size >= 31 else prices
    if last_30.size >= 2:
        log_ret = np.diff(np.log(last_30))
        sigma_30 = float(np.std(log_ret, ddof=1)) if log_ret.size >= 2 else 0.0
    else:
        sigma_30 = 0.0

    avg_vol_7d = float(np.mean(vols[-7:])) if vols[-7:].size else 0.0
    avg_vol_30d = float(np.mean(vols[-30:])) if vols[-30:].size else 0.0

    return {
        "history_days": int(prices.size),
        "current_price": current_price,
        "all_time_low": atl,
        "sigma_30": sigma_30,
        "avg_vol_7d": avg_vol_7d,
        "avg_vol_30d": avg_vol_30d,
    }


# ─── Internal: market-wide stagnation index ───────────────────────────────────


def _compute_mwsi(db: Session) -> float | None:
    """MWSI = median over containers of V_7d / V_30d (using volume_7d column).

    One SELECT pre-aggregating mean(V_7d) and mean(V_30d) per container, ratios
    medianed in Python.
    """
    sql = text(
        """
        WITH per_container AS (
            SELECT
                container_id,
                AVG(CASE WHEN timestamp >= :since_7d  THEN volume_7d END) AS v7,
                AVG(CASE WHEN timestamp >= :since_30d THEN volume_7d END) AS v30
            FROM fact_container_prices
            WHERE timestamp >= :since_30d
              AND volume_7d IS NOT NULL
            GROUP BY container_id
        )
        SELECT v7, v30 FROM per_container WHERE v30 IS NOT NULL AND v30 > 0
        """
    )
    now = datetime.now(UTC).replace(tzinfo=None)
    try:
        rows = db.execute(
            sql,
            {
                "since_7d": now - timedelta(days=7),
                "since_30d": now - timedelta(days=30),
            },
        ).fetchall()
    except Exception as e:
        logger.warning("MWSI query failed: %s — skipping macro guard", e)
        return None

    if not rows:
        return None
    ratios = [float(r.v7) / float(r.v30) for r in rows if r.v7 is not None and r.v30 > 0]
    if not ratios:
        return None
    return float(np.median(ratios))


# ─── Internal: cyclical autocorrelation guard ─────────────────────────────────


def _has_cyclical_autocorrelation(container_id: str, db: Session) -> bool:
    """Return True when a significant ACF peak (lag in [60, 365] days) suggests
    a near-future seasonal pop — cancels pruning per the research's cyclical
    immunity rule.
    """
    sql = text(
        """
        SELECT timestamp, price
        FROM fact_container_prices
        WHERE container_id = :cid
          AND price IS NOT NULL
        ORDER BY timestamp ASC
        """
    )
    rows = db.execute(sql, {"cid": container_id}).fetchall()
    if len(rows) < 365:
        return False  # not enough history for seasonality claim

    prices = np.array([float(r.price) for r in rows], dtype=float)
    log_ret = np.diff(np.log(np.clip(prices, a_min=1e-9, a_max=None)))
    n = log_ret.size
    if n < 365:
        return False

    log_ret_centered = log_ret - log_ret.mean()
    denom = float(np.dot(log_ret_centered, log_ret_centered))
    if denom <= 0:
        return False

    # Check ACF at lags [60, 365] for any |ρ| ≥ threshold
    for lag in range(60, min(365, n - 1)):
        a = log_ret_centered[:-lag]
        b = log_ret_centered[lag:]
        rho = float(np.dot(a, b) / denom)
        if abs(rho) >= _AUTOCORR_SIGNIFICANCE:
            return True
    return False
