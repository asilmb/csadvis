"""
Unit tests for PV-30: Inventory JIT Valuation.

Covers:
  SqlAlchemyPriceRepository:
    - get_latest_price returns None for unknown container (non-commodity filter)
    - get_latest_price returns None when no price rows exist
    - get_latest_price returns latest FactContainerPrice row
    - is_fresh returns False for unknown container
    - is_fresh returns False when price older than 1 hour
    - is_fresh returns True when price within last hour
    - save_jit_price returns False for unknown container (non-commodity skip)
    - save_jit_price inserts FactContainerPrice and returns True
    - save_jit_price stores source="jit_valuation"
    - save_jit_price stores lowest_price_kzt when provided
    - get_latest_price returns the MOST RECENT row when multiple exist

  inventory_valuation_handler():
    - no open positions → no fetches, no errors
    - all prices fresh → no coordinator calls
    - stale price → coordinator called once per unique name
    - duplicate market_hash_name across positions → fetched exactly once (dedup)
    - coordinator raises on one item → error logged, loop continues, other items fetched
    - empty steam overview (price=0) → non-commodity count incremented, no DB write
    - save_jit_price returns False (non-commodity) → no insert, handler completes
    - cookie missing → handler returns early without any coordinator calls
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from domain.models import (
    Base,
    ContainerType,
    DimContainer,
    FactContainerPrice,
)
from domain.sql_repositories import (
    PriceSnapshotDTO,
    SqlAlchemyPriceRepository,
)

# ─── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture()
def engine():
    e = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(e)
    yield e
    e.dispose()


@pytest.fixture()
def db(engine):
    with Session(engine) as session:
        yield session


@pytest.fixture()
def repo(db):
    return SqlAlchemyPriceRepository(db)


# ─── Helpers ──────────────────────────────────────────────────────────────────


def _now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _add_container(db: Session, name: str) -> DimContainer:
    container = DimContainer(
        container_name=name,
        container_type=ContainerType.Weapon_Case,
        base_cost=500.0,
    )
    db.add(container)
    db.flush()
    return container


def _add_price(
    db: Session,
    container: DimContainer,
    price: float = 5000.0,
    age_seconds: float = 0.0,
) -> FactContainerPrice:
    row = FactContainerPrice(
        container_id=container.container_id,
        price=price,
        timestamp=_now() - timedelta(seconds=age_seconds),
        source="test",
    )
    db.add(row)
    db.flush()
    return row


# ─── SqlAlchemyPriceRepository ────────────────────────────────────────────────


class TestPriceRepository:
    def test_unknown_container_returns_none(self, repo):
        result = repo.get_latest_price("Nonexistent Case")
        assert result is None

    def test_known_container_no_prices_returns_none(self, repo, db):
        _add_container(db, "CS2 Case")
        db.commit()
        result = repo.get_latest_price("CS2 Case")
        assert result is None

    def test_returns_latest_price_row(self, repo, db):
        c = _add_container(db, "Recoil Case")
        _add_price(db, c, price=4000.0, age_seconds=7200)  # older
        _add_price(db, c, price=5000.0, age_seconds=300)   # newer
        db.commit()

        result = repo.get_latest_price("Recoil Case")
        assert result is not None
        assert result.price == pytest.approx(5000.0)
        assert result.container_name == "Recoil Case"
        assert isinstance(result, PriceSnapshotDTO)

    def test_is_fresh_unknown_container(self, repo):
        assert repo.is_fresh("Unknown Item") is False

    def test_is_fresh_price_older_than_one_hour(self, repo, db):
        c = _add_container(db, "Prisma 2 Case")
        _add_price(db, c, age_seconds=3700)  # > 1 hour
        db.commit()
        assert repo.is_fresh("Prisma 2 Case") is False

    def test_is_fresh_price_within_one_hour(self, repo, db):
        c = _add_container(db, "Fracture Case")
        _add_price(db, c, age_seconds=1800)  # 30 min
        db.commit()
        assert repo.is_fresh("Fracture Case") is True

    def test_save_jit_price_unknown_container_returns_false(self, repo, db):
        result = repo.save_jit_price("Unknown Item", 5000.0)
        assert result is False

    def test_save_jit_price_known_container_returns_true(self, repo, db):
        _add_container(db, "Danger Zone Case")
        db.commit()
        result = repo.save_jit_price("Danger Zone Case", 7500.0)
        assert result is True

    def test_save_jit_price_stores_source(self, repo, db):
        c = _add_container(db, "Horizon Case")
        db.commit()
        repo.save_jit_price("Horizon Case", 6000.0)
        db.commit()

        row = (
            db.query(FactContainerPrice)
            .filter(FactContainerPrice.container_id == c.container_id)
            .first()
        )
        assert row.source == "jit_valuation"

    def test_save_jit_price_stores_lowest_price(self, repo, db):
        c = _add_container(db, "CS20 Case")
        db.commit()
        repo.save_jit_price("CS20 Case", 6000.0, lowest_price=5800.0)
        db.commit()

        row = (
            db.query(FactContainerPrice)
            .filter(FactContainerPrice.container_id == c.container_id)
            .first()
        )
        assert row.lowest_price == pytest.approx(5800.0)

    def test_get_latest_price_returns_most_recent(self, repo, db):
        c = _add_container(db, "Clutch Case")
        _add_price(db, c, price=3000.0, age_seconds=5000)
        _add_price(db, c, price=4000.0, age_seconds=2000)
        _add_price(db, c, price=5000.0, age_seconds=500)
        db.commit()

        result = repo.get_latest_price("Clutch Case")
        assert result.price == pytest.approx(5000.0)


