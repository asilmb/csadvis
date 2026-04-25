"""
LC-1: synchronous per-container analysis facade.

This is the production entry point for the manual one-case-at-a-time workflow:
given a container, fetch its 90-day price/volume window, classify the
behavioral phase (with hysteresis against the previously persisted phase),
project expected returns at 1/3/6 months, and persist everything back to
``dim_containers`` so the next manual call has a meaningful ``prior_phase``.

Optionally (``apply_prune=True``) also evaluates the pruning floors and
flips ``is_blacklisted = 1`` when the container has crossed all three.

Designed to run inside one synchronous call (~50–300 ms per container).
No Celery, no schedulers — orchestrated by the caller (CLI or API endpoint).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.domain.lifecycle import (
    LifecyclePhase,
    classify_lifecycle,
    compute_expected_returns,
)
from src.domain.models import DimContainer
from src.domain.pruning import (
    PruneDecision,
    PruneVerdict,
    evaluate_prune_candidate,
)

logger = logging.getLogger(__name__)

_FETCH_WINDOW_DAYS = 120  # supplies enough rows for a 90-day classifier window


@dataclass
class AnalysisResult:
    container_id: str
    container_name: str
    phase: LifecyclePhase | None
    prior_phase: LifecyclePhase | None
    expected_returns: dict[str, float] = field(default_factory=dict)
    prune_decision: PruneDecision | None = None
    pruned: bool = False                       # True when is_blacklisted was set
    metrics: dict = field(default_factory=dict)
    reason: str = ""                            # human-readable summary


def analyze_container(
    container_id: str,
    db: Session,
    *,
    apply_prune: bool = False,
) -> AnalysisResult:
    """Classify, forecast, persist, and optionally prune a single container.

    Args:
        container_id:  UUID of the container to analyse.
        db:            Active SQLAlchemy session (caller owns commit/rollback).
        apply_prune:   When True and pruning floors are breached, sets
                       ``is_blacklisted = 1``. Read-only otherwise.

    Returns:
        AnalysisResult capturing the decision and the persisted state.
    """
    container: DimContainer | None = (
        db.query(DimContainer).filter(DimContainer.container_id == container_id).first()
    )
    if container is None:
        return AnalysisResult(
            container_id=container_id,
            container_name="<unknown>",
            phase=None,
            prior_phase=None,
            reason="container_not_found",
        )

    prior_phase = _read_prior_phase(container)
    prices, volumes = _fetch_series(container_id, db)

    phase, metrics = classify_lifecycle(prices, volumes, prior_phase)
    expected_returns: dict[str, float] = {}
    reason = "classified"

    if phase is None:
        reason = metrics.get("reason", "insufficient_data")
    else:
        expected_returns = compute_expected_returns(phase, metrics)

    # Persist phase + forecasts back to dim_containers (so next call has a
    # meaningful prior_phase for hysteresis).
    if phase is not None:
        container.current_lifecycle_phase = phase.value
        container.lifecycle_updated_at = datetime.now(UTC).replace(tzinfo=None)
        container.expected_return_1m = expected_returns.get("30d")
        container.expected_return_3m = expected_returns.get("90d")
        container.expected_return_6m = expected_returns.get("180d")
        db.flush()

    # Optional pruning evaluation
    prune_decision: PruneDecision | None = None
    pruned = False
    if apply_prune:
        prune_decision = evaluate_prune_candidate(container_id, db)
        if prune_decision.verdict == PruneVerdict.PRUNE:
            container.is_blacklisted = 1
            pruned = True
            reason = "pruned"
            db.flush()
        elif prune_decision.verdict == PruneVerdict.KEEP:
            reason = f"kept_{prune_decision.reason}"

    return AnalysisResult(
        container_id=container_id,
        container_name=str(container.container_name),
        phase=phase,
        prior_phase=prior_phase,
        expected_returns=expected_returns,
        prune_decision=prune_decision,
        pruned=pruned,
        metrics={k: v for k, v in metrics.items() if not k.startswith("_")},
        reason=reason,
    )


# ─── Internal helpers ─────────────────────────────────────────────────────────


def _read_prior_phase(container: DimContainer) -> LifecyclePhase | None:
    raw = getattr(container, "current_lifecycle_phase", None)
    if not raw:
        return None
    try:
        return LifecyclePhase(raw)
    except ValueError:
        # Legacy value (e.g. "ACTIVE") or unknown — treat as no prior.
        return None


def _fetch_series(
    container_id: str, db: Session
) -> tuple[list[float], list[int]]:
    """Return chronological (prices, volumes) over the last fetch window."""
    cutoff = datetime.now(UTC).replace(tzinfo=None) - timedelta(days=_FETCH_WINDOW_DAYS)
    sql = text(
        """
        SELECT price, volume_7d
        FROM fact_container_prices
        WHERE container_id = :cid
          AND timestamp >= :since
          AND price IS NOT NULL
        ORDER BY timestamp ASC
        """
    )
    rows = db.execute(sql, {"cid": container_id, "since": cutoff}).fetchall()
    prices = [float(r.price) for r in rows]
    volumes = [int(r.volume_7d) if r.volume_7d is not None else 0 for r in rows]
    return prices, volumes
