"""LC-1: pruning.evaluate_prune_candidate — unit tests with in-memory SQLite."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.domain.models import (
    Base,
    ContainerType,
    DimContainer,
    FactContainerPrice,
)
from src.domain.pruning import (
    PruneVerdict,
    evaluate_prune_candidate,
)


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
    s: Session,
    cid: str,
    *,
    days: int,
    price: float,
    volume_7d: int,
) -> None:
    """Seed `days` daily price points ending at today, all with given price/vol."""
    base = datetime.now(UTC).replace(tzinfo=None) - timedelta(days=days - 1)
    for i in range(days):
        s.add(
            FactContainerPrice(
                container_id=cid,
                timestamp=base + timedelta(days=i),
                price=price,
                volume_7d=volume_7d,
            )
        )
    s.commit()


def _seed_other_active_universe(s: Session, n: int = 10) -> None:
    """Seed N healthy peers so MWSI guard does not trip during tests."""
    for i in range(n):
        cid = _make_container(s, name=f"Peer {i}")
        # Healthy: V_7d ≈ V_30d (ratio ~1.0) → MWSI well above 0.80
        _seed_prices(s, cid, days=30, price=200.0, volume_7d=100)


# ─── Direct stagnant container → PRUNE ────────────────────────────────────────


def test_flat_zero_volume_container_is_pruned(db: Session) -> None:
    """fd63a22e-style asset: dead flat price + zero volume → PRUNE."""
    _seed_other_active_universe(db, n=10)
    cid = _make_container(db, name="Dead Container")
    _seed_prices(db, cid, days=60, price=22.0, volume_7d=0)

    decision = evaluate_prune_candidate(cid, db)
    assert decision.verdict == PruneVerdict.PRUNE
    assert decision.metrics["near_atl"]
    assert decision.metrics["low_volatility"]
    assert decision.metrics["low_liquidity"]


# ─── Insufficient data path ───────────────────────────────────────────────────


def test_short_history_returns_insufficient_data(db: Session) -> None:
    cid = _make_container(db, name="New Container")
    _seed_prices(db, cid, days=10, price=100.0, volume_7d=20)
    decision = evaluate_prune_candidate(cid, db)
    assert decision.verdict == PruneVerdict.INSUFFICIENT_DATA


# ─── False-positive guard: MWSI ───────────────────────────────────────────────


def test_market_wide_stagnation_blocks_pruning(db: Session) -> None:
    """When the entire universe shows V_7d/V_30d collapse, individual pruning halts."""
    # Whole universe collapses: every peer shows declining volume (last 7d == 0,
    # full 30d ≈ 50). This is the "everyone is stagnant" macro condition.
    for i in range(10):
        peer_cid = _make_container(db, name=f"Peer {i}")
        # 23 days of healthy volume + 7 days of zero → V7=0, V30≈37 → ratio≈0
        base = datetime.now(UTC).replace(tzinfo=None) - timedelta(days=29)
        for d in range(30):
            db.add(
                FactContainerPrice(
                    container_id=peer_cid,
                    timestamp=base + timedelta(days=d),
                    price=200.0,
                    volume_7d=50 if d < 23 else 0,
                )
            )
    db.commit()

    cid = _make_container(db, name="Stagnant Target")
    _seed_prices(db, cid, days=60, price=22.0, volume_7d=0)

    decision = evaluate_prune_candidate(cid, db)
    assert decision.verdict == PruneVerdict.KEEP
    assert decision.reason == "market_wide_stagnation"


# ─── False-positive guard: healthy volume container is NOT pruned ────────────


def test_healthy_container_is_kept(db: Session) -> None:
    _seed_other_active_universe(db, n=10)
    cid = _make_container(db, name="Healthy Case")
    # Stable, well-traded asset
    _seed_prices(db, cid, days=60, price=200.0, volume_7d=100)
    decision = evaluate_prune_candidate(cid, db)
    assert decision.verdict == PruneVerdict.KEEP
    assert decision.reason == "floors_not_breached"
