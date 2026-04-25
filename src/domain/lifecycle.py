"""
LC-1: Behavioral lifecycle classifier with HMM + mathematical hysteresis.

Replaces the legacy age-based classification (NEW/ACTIVE/AGING/LEGACY/DEAD by
first_seen_date) with a four-state behavioral model derived from price/volume
dynamics. Operates per-container, on demand — no scheduler, no global training.

States
------
SPECULATIVE_VOLATILITY (S1)  high σ, extreme volume spikes → release / pump-and-dump
STABLE_LIQUIDITY       (S2)  narrow σ, mean-reverting, V_7d ≈ V_30d → flip zone
DEFLATIONARY_GROWTH    (S3)  positive μ, declining σ and volume → rare-pool effect
LIQUIDITY_STAGNATION   (S4)  σ → 0, volume → 0 → pruning candidate

Approach
--------
Primary path: per-container Gaussian HMM (4 states) fitted via Baum-Welch on the
log-return + log-volume series. The hidden states are mapped to LifecyclePhase
by their emission moments (highest σ → S1, lowest σ + flat μ → S4, mid σ +
positive μ → S3, otherwise S2).

Defensive fallback: when the series is too short (<30 obs), HMM fit fails to
converge, or hmmlearn is unavailable, the classifier falls back to a metric
threshold rule that uses the same four-state ontology.

Hysteresis
----------
Direct Viterbi output is unstable on noisy crypto-like markets. To prevent
"flapping" (e.g. day-long pump misclassified as a regime change), every
candidate phase is filtered through a Schmitt-trigger style hysteresis: leaving
the prior state requires the controlling metric to cross a strict EXIT band,
not just enter another state's region.
"""

from __future__ import annotations

import logging
import math
import warnings
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)


# ─── State enum ───────────────────────────────────────────────────────────────


class LifecyclePhase(StrEnum):
    SPECULATIVE_VOLATILITY = "SPECULATIVE_VOLATILITY"
    STABLE_LIQUIDITY = "STABLE_LIQUIDITY"
    DEFLATIONARY_GROWTH = "DEFLATIONARY_GROWTH"
    LIQUIDITY_STAGNATION = "LIQUIDITY_STAGNATION"


# Backwards-compat alias for code not yet migrated off the old name.
LifecycleStage = LifecyclePhase


# ─── Thresholds ───────────────────────────────────────────────────────────────
# Daily log-return σ. Values empirically calibrated; tighten/relax via PRs only.

_MIN_OBSERVATIONS = 30          # below this → fallback only
_HMM_MIN_OBSERVATIONS = 45      # below this → skip HMM, go straight to metric
_HMM_N_STATES = 4
_HMM_N_ITER = 50
_HMM_RANDOM_STATE = 42

# Entry thresholds (used to decide candidate phase from metrics).
_SIGMA_HIGH_ENTER = 0.10        # σ ≥ 10 % → SPECULATIVE
_SIGMA_LOW_ENTER = 0.025        # σ ≤ 2.5 % → STAGNATION (when volume confirms)
_VOL_RATIO_LOW_ENTER = 0.50     # V7/V30 < 0.5 confirms stagnation
_DRIFT_POS_ENTER = 0.003        # μ ≥ 0.3 %/day → DEFLATIONARY_GROWTH
_SIGMA_DEFL_MAX = 0.07          # DG requires σ ≤ 7 %

# Exit thresholds (used by hysteresis when prior state is set; STRICTER than
# entry — must clearly cross to leave).
_SIGMA_HIGH_EXIT = 0.07         # leave SPECULATIVE only when σ ≤ 7 %
_SIGMA_LOW_EXIT = 0.04          # leave STAGNATION only when σ ≥ 4 %
_VOL_RATIO_LOW_EXIT = 0.70      # or vol_ratio ≥ 0.7
_DRIFT_POS_EXIT = 0.001         # leave DG when μ ≤ 0.1 %/day

# Default per-state daily expected log return (μ_i) used when HMM is unavailable.
# Conservative — calibrated against the research's qualitative state descriptions.
_DEFAULT_MU_PER_STATE: dict[LifecyclePhase, float] = {
    LifecyclePhase.SPECULATIVE_VOLATILITY: 0.000,
    LifecyclePhase.STABLE_LIQUIDITY:       0.000,
    LifecyclePhase.DEFLATIONARY_GROWTH:    0.004,
    LifecyclePhase.LIQUIDITY_STAGNATION:  -0.001,
}

# Default transition matrix A (rows = from, cols = to) used when HMM A is
# unavailable. Diagonal-heavy → states are sticky (consistent with hysteresis).
# Order: [SPECULATIVE, STABLE, DEFLATIONARY, STAGNATION]
_DEFAULT_TRANSITION_MATRIX = np.array(
    [
        [0.70, 0.20, 0.05, 0.05],
        [0.05, 0.85, 0.05, 0.05],
        [0.05, 0.10, 0.80, 0.05],
        [0.10, 0.10, 0.05, 0.75],
    ]
)
_PHASE_ORDER = [
    LifecyclePhase.SPECULATIVE_VOLATILITY,
    LifecyclePhase.STABLE_LIQUIDITY,
    LifecyclePhase.DEFLATIONARY_GROWTH,
    LifecyclePhase.LIQUIDITY_STAGNATION,
]


# ─── Feature engineering ──────────────────────────────────────────────────────


@dataclass(frozen=True)
class _Features:
    log_returns: np.ndarray         # 1-D, length = N-1
    log_volume: np.ndarray          # 1-D, length = N-1 (aligned with log_returns)
    sigma_30: float                 # std of last 30 log returns
    sigma_90: float                 # std of full window
    drift: float                    # mean of last 30 log returns
    vol_ratio: float                # mean(V_7d) / mean(V_30d)


def _compute_features(
    price_series: list[float],
    volume_series: list[int] | list[float],
) -> _Features | None:
    """Return engineered features or None when input is unusable."""
    prices = np.asarray([p for p in price_series if p and p > 0], dtype=float)
    vols = np.asarray([float(v) if v else 0.0 for v in volume_series], dtype=float)
    if prices.size < _MIN_OBSERVATIONS:
        return None
    # Align volumes to prices (truncate to min length)
    n = min(prices.size, vols.size)
    prices = prices[-n:]
    vols = vols[-n:]

    log_returns = np.diff(np.log(prices))
    # log volume — clip 0 to avoid -inf
    log_volume = np.log(np.clip(vols[1:], a_min=1.0, a_max=None))

    last_30 = log_returns[-30:]
    sigma_30 = float(np.std(last_30, ddof=1)) if last_30.size >= 2 else 0.0
    sigma_90 = float(np.std(log_returns, ddof=1)) if log_returns.size >= 2 else 0.0
    drift = float(np.mean(last_30)) if last_30.size else 0.0

    last_7d_vol = float(np.mean(vols[-7:])) if vols[-7:].size else 0.0
    last_30d_vol = float(np.mean(vols[-30:])) if vols[-30:].size else 0.0
    vol_ratio = last_7d_vol / last_30d_vol if last_30d_vol > 0 else 0.0

    return _Features(
        log_returns=log_returns,
        log_volume=log_volume,
        sigma_30=sigma_30,
        sigma_90=sigma_90,
        drift=drift,
        vol_ratio=vol_ratio,
    )


# ─── Metric-based classifier (fallback + HMM mapping reference) ───────────────


def _classify_by_metrics(f: _Features) -> LifecyclePhase:
    """Strict-threshold classifier (no hysteresis). Used as fallback and as the
    semantic anchor for mapping HMM hidden states → LifecyclePhase."""
    if f.sigma_30 <= _SIGMA_LOW_ENTER and f.vol_ratio < _VOL_RATIO_LOW_ENTER:
        return LifecyclePhase.LIQUIDITY_STAGNATION
    if f.sigma_30 >= _SIGMA_HIGH_ENTER:
        return LifecyclePhase.SPECULATIVE_VOLATILITY
    if (
        f.drift >= _DRIFT_POS_ENTER
        and f.sigma_30 <= _SIGMA_DEFL_MAX
        and f.vol_ratio < 1.05
    ):
        return LifecyclePhase.DEFLATIONARY_GROWTH
    return LifecyclePhase.STABLE_LIQUIDITY


# ─── HMM path ─────────────────────────────────────────────────────────────────


def _try_hmm_classify(f: _Features) -> tuple[LifecyclePhase, np.ndarray, dict] | None:
    """Fit per-container Gaussian HMM and return (last_phase, transmat, μ_map).

    Returns None on any failure (import, convergence, degenerate data).
    Caller is expected to fall back to metric classification.
    """
    if f.log_returns.size < _HMM_MIN_OBSERVATIONS:
        return None
    try:
        from hmmlearn.hmm import GaussianHMM
    except ImportError:
        logger.debug("hmmlearn not installed — falling back to metric classifier")
        return None

    X = np.column_stack([f.log_returns, f.log_volume])
    # Replace any non-finite values
    if not np.isfinite(X).all():
        return None
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            model = GaussianHMM(
                n_components=_HMM_N_STATES,
                covariance_type="diag",
                n_iter=_HMM_N_ITER,
                random_state=_HMM_RANDOM_STATE,
            )
            model.fit(X)
            hidden = model.predict(X)
    except Exception as e:  # numpy / hmmlearn convergence error
        logger.debug("HMM fit failed: %s — falling back to metric classifier", e)
        return None

    # Map hidden states {0..3} → LifecyclePhase using emission moments
    # (variance on log-return component is the dominant discriminator).
    # NB: with covariance_type="diag", model.covars_ is the FULL (n,F,F) matrix
    # in recent hmmlearn versions — extract the diagonal explicitly.
    means = model.means_[:, 0]                                  # μ on log_returns dim
    covars = np.asarray(model.covars_)
    if covars.ndim == 3:
        diagonals = np.array([np.diag(c) for c in covars])      # (n_states, n_features)
    else:
        diagonals = covars                                       # already (n_states, n_features)
    variances = diagonals[:, 0]                                 # σ² on log_returns dim
    sigmas = np.sqrt(np.clip(variances, a_min=0.0, a_max=None))

    # Sort indices by σ ascending: lowest σ → STAGNATION, highest → SPECULATIVE
    order_by_sigma = np.argsort(sigmas)
    state_to_phase: dict[int, LifecyclePhase] = {}
    state_to_phase[int(order_by_sigma[0])] = LifecyclePhase.LIQUIDITY_STAGNATION
    state_to_phase[int(order_by_sigma[-1])] = LifecyclePhase.SPECULATIVE_VOLATILITY
    # Of the two middle states, the one with higher μ → DEFLATIONARY_GROWTH
    mid_a, mid_b = int(order_by_sigma[1]), int(order_by_sigma[2])
    if means[mid_a] >= means[mid_b]:
        state_to_phase[mid_a] = LifecyclePhase.DEFLATIONARY_GROWTH
        state_to_phase[mid_b] = LifecyclePhase.STABLE_LIQUIDITY
    else:
        state_to_phase[mid_b] = LifecyclePhase.DEFLATIONARY_GROWTH
        state_to_phase[mid_a] = LifecyclePhase.STABLE_LIQUIDITY

    # Permute model.transmat_ into canonical _PHASE_ORDER order
    phase_to_state = {v: k for k, v in state_to_phase.items()}
    perm = np.array([phase_to_state[p] for p in _PHASE_ORDER])
    transmat = model.transmat_[np.ix_(perm, perm)]

    mu_per_phase = {
        _PHASE_ORDER[i]: float(means[perm[i]]) for i in range(len(_PHASE_ORDER))
    }

    last_phase = state_to_phase[int(hidden[-1])]
    return last_phase, transmat, mu_per_phase


# ─── Hysteresis (Schmitt-trigger style) ───────────────────────────────────────


def _apply_hysteresis(
    prior: LifecyclePhase | None,
    candidate: LifecyclePhase,
    f: _Features,
) -> LifecyclePhase:
    """Suppress regime transition until controlling metric clears the EXIT band
    of the prior state. When prior is None or matches candidate, pass through.
    """
    if prior is None or prior == candidate:
        return candidate

    if prior == LifecyclePhase.LIQUIDITY_STAGNATION:
        # Stay stagnant unless σ clearly recovers OR volume_ratio recovers
        if f.sigma_30 >= _SIGMA_LOW_EXIT or f.vol_ratio >= _VOL_RATIO_LOW_EXIT:
            return candidate
        return prior

    if prior == LifecyclePhase.SPECULATIVE_VOLATILITY:
        # Stay speculative until σ subsides under the exit floor
        if f.sigma_30 <= _SIGMA_HIGH_EXIT:
            return candidate
        return prior

    if prior == LifecyclePhase.DEFLATIONARY_GROWTH:
        # Stay in DG unless drift collapses OR volatility explodes
        if f.drift <= _DRIFT_POS_EXIT or f.sigma_30 >= _SIGMA_HIGH_ENTER:
            return candidate
        return prior

    # prior == STABLE_LIQUIDITY — neutral default; allow free transitions.
    return candidate


# ─── Public API ───────────────────────────────────────────────────────────────


def classify_lifecycle(
    price_series: list[float],
    volume_series: list[int] | list[float],
    prior_phase: LifecyclePhase | None = None,
) -> tuple[LifecyclePhase | None, dict[str, Any]]:
    """Classify a container's current behavioral phase.

    Args:
        price_series:   chronological prices (~90 daily points).
        volume_series:  matching volume (any length ≥ price; truncated to align).
        prior_phase:    last persisted phase from dim_containers (for hysteresis).
                        Pass None on first-ever classification.

    Returns:
        (phase, metrics). phase is None when input is too short for a reliable
        decision; metrics always carries best-effort debug info.
    """
    f = _compute_features(price_series, volume_series)
    if f is None:
        return None, {"reason": "insufficient_data"}

    # Phase classification: ALWAYS metric-based. Per-container HMM on a single
    # 90-day window of one regime is too unstable for instantaneous decisions
    # (hmmlearn forces 4 states even on single-regime data → spurious "last
    # state" assignment). HMM is used purely for forecasting A and μ.
    candidate = _classify_by_metrics(f)
    hmm_result = _try_hmm_classify(f)
    if hmm_result is not None:
        # Discard HMM's phase decision; keep its A and μ for compute_expected_returns.
        _, transmat, mu_per_phase = hmm_result
        source = "metric+hmm_forecast"
    else:
        transmat = _DEFAULT_TRANSITION_MATRIX
        mu_per_phase = dict(_DEFAULT_MU_PER_STATE)
        source = "metric_only"

    confirmed = _apply_hysteresis(prior_phase, candidate, f)

    metrics = {
        "source": source,
        "sigma_30": f.sigma_30,
        "sigma_90": f.sigma_90,
        "drift": f.drift,
        "vol_ratio": f.vol_ratio,
        "candidate_phase": candidate.value,
        "confirmed_phase": confirmed.value,
        "prior_phase": prior_phase.value if prior_phase else None,
        "_transmat": transmat,        # numpy array — not JSON-serialisable; for forecast()
        "_mu_per_phase": mu_per_phase,
    }
    return confirmed, metrics


def compute_expected_returns(
    confirmed_phase: LifecyclePhase,
    metrics: dict[str, Any],
    horizons_days: tuple[int, ...] = (30, 90, 180),
) -> dict[str, float]:
    """Project E[log return] per horizon via π_t · A^k · μ.

    Uses the transition matrix and per-phase μ extracted by classify_lifecycle()
    (HMM-derived when available, default priors otherwise).
    """
    transmat: np.ndarray = metrics.get("_transmat", _DEFAULT_TRANSITION_MATRIX)
    mu_per_phase: dict[LifecyclePhase, float] = metrics.get(
        "_mu_per_phase", dict(_DEFAULT_MU_PER_STATE)
    )

    # π_t — point mass on confirmed_phase
    pi_t = np.zeros(len(_PHASE_ORDER))
    pi_t[_PHASE_ORDER.index(confirmed_phase)] = 1.0
    mu_vec = np.array([mu_per_phase.get(p, 0.0) for p in _PHASE_ORDER])

    out: dict[str, float] = {}
    for k in horizons_days:
        # cumulative log return = Σ_{t=1..k} π_t A^t · μ
        cumulative = 0.0
        pi_step = pi_t.copy()
        for _ in range(k):
            pi_step = pi_step @ transmat
            cumulative += float(pi_step @ mu_vec)
        # Convert cumulative log-return to simple return
        out[f"{k}d"] = math.expm1(cumulative)
    return out


def is_flip_eligible(phase: LifecyclePhase | None) -> bool:
    """Flip = high-frequency narrow-spread trading. Only STABLE_LIQUIDITY qualifies."""
    return phase == LifecyclePhase.STABLE_LIQUIDITY


def is_invest_eligible(phase: LifecyclePhase | None) -> bool:
    """Long-term invest = supply-contraction uptrend. Only DEFLATIONARY_GROWTH qualifies."""
    return phase == LifecyclePhase.DEFLATIONARY_GROWTH
