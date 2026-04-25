"""LC-1: lifecycle_service.analyze_container — persistence + apply-prune tests."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.domain.lifecycle import LifecyclePhase
from src.domain.lifecycle_service import analyze_container
from src.domain.models import (
    Base,
    ContainerType,
    DimContainer,
    FactContainerPrice,
)
from src.domain.pruning import PruneVerdict


@pytest.fixture
def db() -> Session:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as s:
        yield s


def _make_container(s: Session, name: str = "Test Case") -> str:
    c = DimContainer(
        container_name=name,
        container_type=ContainerType.Weapon_Case,
        base_cost=1000.0,
    )
    s.add(c)
    s.commit()
    return str(c.container_id)


def _seed_prices(
    s: Session, cid: str, *, days: int, price_fn, volume_7d: int = 50
) -> None:
    base = datetime.now(UTC).replace(tzinfo=None) - timedelta(days=days - 1)
    for i in range(days):
        s.add(
            FactContainerPrice(
                container_id=cid,
                timestamp=base + timedelta(days=i),
                price=price_fn(i),
                volume_7d=volume_7d,
            )
        )
    s.commit()


# ─── Happy path: phase classified + persisted + forecasts written ─────────────


def test_analyze_persists_phase_and_forecasts(db: Session) -> None:
    cid = _make_container(db, name="Stable Case")
    # Low-σ price series → STABLE_LIQUIDITY
    _seed_prices(db, cid, days=90, price_fn=lambda i: 100.0 + (1.0 if i % 2 else -1.0))

    result = analyze_container(cid, db)

    assert result.phase == LifecyclePhase.STABLE_LIQUIDITY
    assert result.prior_phase is None
    assert set(result.expected_returns.keys()) == {"30d", "90d", "180d"}

    db.commit()
    refreshed = db.query(DimContainer).filter(DimContainer.container_id == cid).one()
    assert refreshed.current_lifecycle_phase == "STABLE_LIQUIDITY"
    assert refreshed.lifecycle_updated_at is not None
    assert refreshed.expected_return_1m is not None
    assert refreshed.expected_return_3m is not None
    assert refreshed.expected_return_6m is not None


# ─── Hysteresis: prior phase from prior call buffers next decision ────────────


def test_second_call_picks_up_prior_phase_from_db(db: Session) -> None:
    cid = _make_container(db, name="Sticky Case")
    _seed_prices(db, cid, days=90, price_fn=lambda i: 100.0 + (1.0 if i % 2 else -1.0))

    first = analyze_container(cid, db)
    db.commit()
    second = analyze_container(cid, db)

    assert first.prior_phase is None
    assert second.prior_phase == first.phase  # persisted then read back


# ─── Insufficient data path ───────────────────────────────────────────────────


def test_short_history_returns_none_phase(db: Session) -> None:
    cid = _make_container(db, name="Brand New Case")
    _seed_prices(db, cid, days=10, price_fn=lambda i: 100.0)

    result = analyze_container(cid, db)
    assert result.phase is None
    assert result.reason == "insufficient_data"

    db.commit()
    refreshed = db.query(DimContainer).filter(DimContainer.container_id == cid).one()
    # Nothing should have been persisted when classification fails
    assert refreshed.current_lifecycle_phase is None
    assert refreshed.expected_return_1m is None


# ─── apply_prune: full breach → is_blacklisted set to 1 ───────────────────────


def test_apply_prune_sets_is_blacklisted_when_floors_breached(db: Session) -> None:
    # Seed healthy peers so MWSI guard does not trip.
    for i in range(10):
        peer_cid = _make_container(db, name=f"Peer {i}")
        _seed_prices(db, peer_cid, days=30, price_fn=lambda i: 200.0, volume_7d=100)

    cid = _make_container(db, name="Dead Case")
    # 60 days of dead-flat low price + zero volume → breaches all 3 floors
    _seed_prices(db, cid, days=60, price_fn=lambda i: 22.0, volume_7d=0)

    result = analyze_container(cid, db, apply_prune=True)

    assert result.prune_decision is not None
    assert result.prune_decision.verdict == PruneVerdict.PRUNE
    assert result.pruned is True

    db.commit()
    refreshed = db.query(DimContainer).filter(DimContainer.container_id == cid).one()
    assert refreshed.is_blacklisted == 1


# ─── apply_prune: healthy container → not blacklisted ─────────────────────────


def test_apply_prune_does_not_blacklist_healthy_container(db: Session) -> None:
    for i in range(10):
        peer_cid = _make_container(db, name=f"Peer {i}")
        _seed_prices(db, peer_cid, days=30, price_fn=lambda i: 200.0, volume_7d=100)

    cid = _make_container(db, name="Healthy Case")
    _seed_prices(db, cid, days=60, price_fn=lambda i: 200.0, volume_7d=100)

    result = analyze_container(cid, db, apply_prune=True)

    assert result.prune_decision is not None
    assert result.prune_decision.verdict == PruneVerdict.KEEP
    assert result.pruned is False

    db.commit()
    refreshed = db.query(DimContainer).filter(DimContainer.container_id == cid).one()
    assert refreshed.is_blacklisted == 0


# ─── Unknown container_id ─────────────────────────────────────────────────────


def test_unknown_container_id_returns_not_found(db: Session) -> None:
    result = analyze_container("00000000-0000-0000-0000-000000000000", db)
    assert result.phase is None
    assert result.reason == "container_not_found"
