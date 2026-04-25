"""LC-1: behavioral lifecycle classifier — unit tests.

Strategy: drive the classifier with synthetic price/volume series tuned to
each of the four states. We accept either source ("hmm" or "metric_fallback")
since hmmlearn convergence is data-dependent — both paths must converge to
the same semantic answer.
"""
from __future__ import annotations

import math

import numpy as np
import pytest

from src.domain.lifecycle import (
    LifecyclePhase,
    classify_lifecycle,
    compute_expected_returns,
    is_flip_eligible,
    is_invest_eligible,
)


# ─── Synthetic series factories ──────────────────────────────────────────────


def _gbm_series(
    n: int, mu: float, sigma: float, p0: float = 100.0, seed: int = 0
) -> list[float]:
    """Geometric Brownian motion price series with daily log returns ~ N(mu, sigma)."""
    rng = np.random.default_rng(seed)
    log_ret = rng.normal(loc=mu, scale=sigma, size=n - 1)
    log_p = np.concatenate(([math.log(p0)], math.log(p0) + np.cumsum(log_ret)))
    return [float(p) for p in np.exp(log_p)]


def _stable_volumes(n: int, mean: float = 50.0) -> list[int]:
    return [int(mean) for _ in range(n)]


def _declining_volumes(n: int, start: float = 50.0, end: float = 5.0) -> list[int]:
    return [int(round(v)) for v in np.linspace(start, end, n)]


# ─── State-specific tests ────────────────────────────────────────────────────


def test_stable_liquidity_when_low_sigma_and_steady_volume() -> None:
    prices = _gbm_series(n=90, mu=0.0, sigma=0.04, seed=1)
    vols = _stable_volumes(90, mean=50)
    phase, metrics = classify_lifecycle(prices, vols, prior_phase=None)
    assert phase == LifecyclePhase.STABLE_LIQUIDITY, metrics


def test_speculative_volatility_when_high_sigma() -> None:
    prices = _gbm_series(n=90, mu=0.0, sigma=0.20, seed=2)
    vols = _stable_volumes(90, mean=200)
    phase, metrics = classify_lifecycle(prices, vols, prior_phase=None)
    assert phase == LifecyclePhase.SPECULATIVE_VOLATILITY, metrics


def test_liquidity_stagnation_when_flat_and_volume_collapse() -> None:
    # Near-zero variance + sharply declining volume
    prices = _gbm_series(n=90, mu=0.0, sigma=0.005, seed=3)
    vols = _declining_volumes(90, start=80, end=2)
    phase, metrics = classify_lifecycle(prices, vols, prior_phase=None)
    assert phase == LifecyclePhase.LIQUIDITY_STAGNATION, metrics


def test_deflationary_growth_when_positive_drift_and_declining_volume() -> None:
    # Steady positive drift with mid σ — supply contraction signature
    prices = _gbm_series(n=90, mu=0.006, sigma=0.03, seed=4)
    vols = _declining_volumes(90, start=80, end=20)
    phase, metrics = classify_lifecycle(prices, vols, prior_phase=None)
    assert phase == LifecyclePhase.DEFLATIONARY_GROWTH, metrics


# ─── Hysteresis: pump-and-dump must not flip a stable container ──────────────


def test_hysteresis_blocks_single_day_pump() -> None:
    """A 1-day spike on top of a stable history must NOT switch the phase."""
    base = _gbm_series(n=89, mu=0.0, sigma=0.03, seed=5)
    # Inject a single-day +50 % spike — high σ for one observation only
    prices = base + [base[-1] * 1.5]
    vols = _stable_volumes(90, mean=50)
    phase, metrics = classify_lifecycle(
        prices, vols, prior_phase=LifecyclePhase.STABLE_LIQUIDITY
    )
    # σ over the last 30 obs should be elevated by the spike, but hysteresis
    # holds the prior STABLE state until σ clears the entry threshold
    # consistently OR the candidate aligns with prior. Either way, must NOT
    # promote the asset to SPECULATIVE on a one-day blip alone:
    if metrics["candidate_phase"] == LifecyclePhase.SPECULATIVE_VOLATILITY.value:
        # Hysteresis must override: confirmed must remain STABLE
        assert phase == LifecyclePhase.STABLE_LIQUIDITY, metrics


def test_hysteresis_holds_speculative_until_sigma_drops_below_exit_band() -> None:
    """A series that settles to mid-σ should hold SPECULATIVE per hysteresis."""
    # Mid σ ~ 0.08 — between SIGMA_HIGH_EXIT (0.07) and SIGMA_HIGH_ENTER (0.10)
    prices = _gbm_series(n=90, mu=0.0, sigma=0.085, seed=6)
    vols = _stable_volumes(90, mean=50)
    phase, _ = classify_lifecycle(
        prices, vols, prior_phase=LifecyclePhase.SPECULATIVE_VOLATILITY
    )
    # σ in the dead band → hysteresis holds prior SPECULATIVE
    assert phase == LifecyclePhase.SPECULATIVE_VOLATILITY


# ─── Eligibility gates ───────────────────────────────────────────────────────


def test_only_stable_is_flip_eligible() -> None:
    assert is_flip_eligible(LifecyclePhase.STABLE_LIQUIDITY) is True
    assert is_flip_eligible(LifecyclePhase.SPECULATIVE_VOLATILITY) is False
    assert is_flip_eligible(LifecyclePhase.DEFLATIONARY_GROWTH) is False
    assert is_flip_eligible(LifecyclePhase.LIQUIDITY_STAGNATION) is False
    assert is_flip_eligible(None) is False


def test_only_deflationary_is_invest_eligible() -> None:
    assert is_invest_eligible(LifecyclePhase.DEFLATIONARY_GROWTH) is True
    assert is_invest_eligible(LifecyclePhase.STABLE_LIQUIDITY) is False
    assert is_invest_eligible(None) is False


# ─── Insufficient data ───────────────────────────────────────────────────────


def test_returns_none_when_history_too_short() -> None:
    phase, metrics = classify_lifecycle([100.0] * 10, [10] * 10, None)
    assert phase is None
    assert metrics["reason"] == "insufficient_data"


# ─── Forecasts ───────────────────────────────────────────────────────────────


def test_compute_expected_returns_returns_three_horizons() -> None:
    prices = _gbm_series(n=90, mu=0.005, sigma=0.03, seed=7)
    vols = _declining_volumes(90, start=80, end=20)
    phase, metrics = classify_lifecycle(prices, vols, None)
    assert phase is not None
    er = compute_expected_returns(phase, metrics, horizons_days=(30, 90, 180))
    assert set(er.keys()) == {"30d", "90d", "180d"}
    # All values must be finite real numbers
    for k, v in er.items():
        assert math.isfinite(v), f"{k} = {v}"


def test_deflationary_growth_yields_positive_long_horizon_return() -> None:
    """π_t·A^k·μ on a confirmed DEFLATIONARY_GROWTH container with the default
    transition matrix should produce a positive 6-month return projection."""
    er = compute_expected_returns(
        LifecyclePhase.DEFLATIONARY_GROWTH,
        metrics={},  # forces default A and μ priors
    )
    assert er["180d"] > 0.0


# ─── Smoke: hmmlearn import path (skip when not installed) ───────────────────


@pytest.mark.parametrize("seed", [10, 11, 12])
def test_hmm_path_does_not_crash_on_random_inputs(seed: int) -> None:
    rng = np.random.default_rng(seed)
    prices = list(np.cumprod(1 + rng.normal(0, 0.05, size=90)) * 100.0)
    vols = list(rng.integers(1, 100, size=90))
    phase, metrics = classify_lifecycle(prices, vols, None)
    assert phase in set(LifecyclePhase) or phase is None
    assert "source" in metrics
