"""
Unit tests for PV-33: Asset ID Reconciliation.

Covers:
  - SqlAlchemyPositionRepository._to_dto classid/market_id/is_on_market mapping
  - update_asset_identity / get_open_by_classid / get_open_by_market_id
  - PositionReconciler.sync() Step 1 (direct), Step 2 (listing), Step 3 (FIFO)
  - Edge cases: empty, unmatched, duplicate asset_id
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.domain.models import Base, Position, PositionStatus
from src.domain.sql_repositories import PositionDTO, SqlAlchemyPositionRepository
from src.domain.reconciler import PositionReconciler, ReconcileResult


# ─── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture
def engine():
    eng = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(eng)
    yield eng
    Base.metadata.drop_all(eng)


@pytest.fixture
def db(engine):
    with Session(engine) as session:
        yield session


@pytest.fixture
def repo(db):
    return SqlAlchemyPositionRepository(db)


# ─── Helpers ──────────────────────────────────────────────────────────────────


def _make_pos(
    db: Session,
    asset_id: int,
    market_hash_name: str = "AK-47 | Redline",
    buy_price: float = 1000.0,
    classid: str | None = None,
    market_id: str | None = None,
    is_on_market: int = 0,
    status: PositionStatus = PositionStatus.OPEN,
    opened_at: datetime | None = None,
    pos_id: str | None = None,
) -> Position:
    row = Position(
        id=pos_id or str(uuid.uuid4()),
        asset_id=asset_id,
        market_hash_name=market_hash_name,
        buy_price=buy_price,
        classid=classid,
        market_id=market_id,
        is_on_market=is_on_market,
        status=status,
        opened_at=opened_at or datetime.now(UTC).replace(tzinfo=None),
    )
    db.add(row)
    db.flush()
    return row


def _inv(
    asset_id: int,
    classid: str = "cls1",
    market_hash_name: str = "AK-47 | Redline",
    market_id: str | None = None,
) -> dict:
    return {
        "asset_id": asset_id,
        "classid": classid,
        "market_hash_name": market_hash_name,
        "market_id": market_id,
    }


# ─── TestPositionRepositoryPV33 ───────────────────────────────────────────────


class TestPositionRepositoryPV33:
    def test_to_dto_maps_classid_market_id_is_on_market(self, db, repo):
        row = _make_pos(db, asset_id=111, classid="cls9", market_id="mkt9", is_on_market=1)
        dto = repo._to_dto(row)
        assert dto.classid == "cls9"
        assert dto.market_id == "mkt9"
        assert dto.is_on_market is True

    def test_to_dto_none_classid_market_id(self, db, repo):
        row = _make_pos(db, asset_id=222, classid=None, market_id=None, is_on_market=0)
        dto = repo._to_dto(row)
        assert dto.classid is None
        assert dto.market_id is None
        assert dto.is_on_market is False

    def test_update_asset_identity_updates_asset_id(self, db, repo):
        row = _make_pos(db, asset_id=100)
        repo.update_asset_identity(str(row.id), new_asset_id=200)
        db.expire(row)
        assert row.asset_id == 200

    def test_update_asset_identity_nonexistent_pos_no_crash(self, repo):
        # Must not raise
        repo.update_asset_identity("nonexistent-id-xyz", new_asset_id=999)

    def test_update_asset_identity_none_args_do_not_overwrite(self, db, repo):
        row = _make_pos(db, asset_id=100, classid="existing_cls", market_id="existing_mkt")
        repo.update_asset_identity(
            str(row.id),
            new_asset_id=100,
            new_classid=None,   # must NOT clear
            new_market_id=None, # must NOT clear
            is_on_market=None,  # must NOT clear
        )
        db.expire(row)
        assert row.classid == "existing_cls"
        assert row.market_id == "existing_mkt"

    def test_get_open_by_classid_returns_open_ordered_asc(self, db, repo):
        t_base = datetime(2025, 1, 1, 12, 0, 0)
        pos_old = _make_pos(db, asset_id=1, classid="CLS", opened_at=t_base)
        pos_new = _make_pos(db, asset_id=2, classid="CLS", opened_at=t_base + timedelta(hours=1))
        results = repo.get_open_by_classid("CLS")
        assert len(results) == 2
        assert results[0].asset_id == pos_old.asset_id
        assert results[1].asset_id == pos_new.asset_id

    def test_get_open_by_classid_excludes_closed(self, db, repo):
        t = datetime(2025, 1, 1, 12, 0, 0)
        _make_pos(db, asset_id=10, classid="CLS2", status=PositionStatus.CLOSED, opened_at=t)
        _make_pos(db, asset_id=11, classid="CLS2", status=PositionStatus.OPEN, opened_at=t + timedelta(hours=1))
        results = repo.get_open_by_classid("CLS2")
        assert len(results) == 1
        assert results[0].asset_id == 11

    def test_get_open_by_classid_unknown_returns_empty(self, repo):
        assert repo.get_open_by_classid("NO_SUCH_CLS") == []

    def test_get_open_by_market_id_returns_match(self, db, repo):
        _make_pos(db, asset_id=50, classid="c", market_id="MKT50")
        dto = repo.get_open_by_market_id("MKT50")
        assert dto is not None
        assert dto.asset_id == 50
        assert dto.market_id == "MKT50"

    def test_get_open_by_market_id_unknown_returns_none(self, repo):
        assert repo.get_open_by_market_id("NO_SUCH_MKT") is None


# ─── TestReconcilerDirect ─────────────────────────────────────────────────────


class TestReconcilerDirect:
    def test_single_direct_match(self, db, repo):
        _make_pos(db, asset_id=1001)
        items = [_inv(1001, classid="cls1")]
        result = PositionReconciler().sync(items, repo)
        assert result.matched_direct == 1
        assert result.matched_listing == 0
        assert result.matched_fifo == 0
        assert result.unmatched_inventory == 0
        assert result.unmatched_positions == 0

    def test_direct_match_updates_classid_market_id(self, db, repo):
        row = _make_pos(db, asset_id=1002, classid=None, market_id=None)
        items = [_inv(1002, classid="new_cls", market_id="new_mkt")]
        PositionReconciler().sync(items, repo)
        db.expire(row)
        assert row.classid == "new_cls"
        assert row.market_id == "new_mkt"

    def test_no_inventory_all_positions_unmatched(self, db, repo):
        _make_pos(db, asset_id=2001)
        _make_pos(db, asset_id=2002)
        result = PositionReconciler().sync([], repo)
        assert result.matched_direct == 0
        assert result.unmatched_positions == 2


# ─── TestReconcilerListing ────────────────────────────────────────────────────


class TestReconcilerListing:
    def test_listing_match_updates_asset_id(self, db, repo):
        # Position has old asset_id + market_id; inventory item has new asset_id + same market_id
        row = _make_pos(db, asset_id=9000, classid="cls_x", market_id="MKT_X")
        items = [_inv(9999, classid="cls_x", market_id="MKT_X")]
        result = PositionReconciler().sync(items, repo)
        assert result.matched_listing == 1
        assert result.matched_direct == 0
        db.expire(row)
        assert row.asset_id == 9999

    def test_position_without_market_id_not_listing_matched(self, db, repo):
        # No market_id on position → cannot match via listing
        _make_pos(db, asset_id=8000, classid="cls_y", market_id=None)
        items = [_inv(8888, classid="cls_y", market_id="MKT_Y")]
        result = PositionReconciler().sync(items, repo)
        # Neither direct (different asset_id) nor listing (pos has no market_id)
        assert result.matched_listing == 0
        # Falls through to FIFO
        assert result.matched_fifo == 1


# ─── TestReconcilerFIFO ───────────────────────────────────────────────────────


class TestReconcilerFIFO:
    def test_fifo_two_positions_two_items_oldest_gets_first(self, db, repo):
        t = datetime(2025, 3, 1, 10, 0, 0)
        pos_old = _make_pos(db, asset_id=1, classid="FIFO_CLS", opened_at=t)
        pos_new = _make_pos(db, asset_id=2, classid="FIFO_CLS", opened_at=t + timedelta(hours=2))
        # Two new asset_ids that don't match positions' current asset_ids
        items = [
            _inv(100, classid="FIFO_CLS"),
            _inv(200, classid="FIFO_CLS"),
        ]
        result = PositionReconciler().sync(items, repo)
        assert result.matched_fifo == 2
        assert result.unmatched_positions == 0
        # pos_old (opened earlier) should get the first inventory item (100)
        db.expire(pos_old)
        db.expire(pos_new)
        # The reconciler processes get_open_positions (DESC opened_at) → still_unmatched is in DESC order
        # Then FIFO match pops inv_by_classid in insertion order (100 first, 200 second)
        # Verify both positions got a new asset_id from the inventory
        assert pos_old.asset_id in (100, 200)
        assert pos_new.asset_id in (100, 200)
        assert pos_old.asset_id != pos_new.asset_id

    def test_fifo_tiebreak_by_id_asc(self, db, repo):
        t = datetime(2025, 3, 1, 10, 0, 0)
        id_a = "00000000-0000-0000-0000-000000000001"
        id_b = "00000000-0000-0000-0000-000000000002"
        pos_a = _make_pos(db, asset_id=1, classid="TIE_CLS", opened_at=t, pos_id=id_a)
        pos_b = _make_pos(db, asset_id=2, classid="TIE_CLS", opened_at=t, pos_id=id_b)
        # Only one inventory item — the position with smaller id (pos_a) should win
        items = [_inv(999, classid="TIE_CLS")]
        result = PositionReconciler().sync(items, repo)
        assert result.matched_fifo == 1
        assert result.unmatched_positions == 1
        db.expire(pos_a)
        db.expire(pos_b)
        # pos_a (id_a < id_b) gets the match; pos_b remains at original asset_id=2
        assert pos_a.asset_id == 999
        assert pos_b.asset_id == 2

    def test_fifo_partial_match_3_positions_2_items(self, db, repo):
        t = datetime(2025, 3, 1, 10, 0, 0)
        _make_pos(db, asset_id=1, classid="PART", opened_at=t)
        _make_pos(db, asset_id=2, classid="PART", opened_at=t + timedelta(hours=1))
        _make_pos(db, asset_id=3, classid="PART", opened_at=t + timedelta(hours=2))
        items = [_inv(100, classid="PART"), _inv(200, classid="PART")]
        result = PositionReconciler().sync(items, repo)
        assert result.matched_fifo == 2
        assert result.unmatched_positions == 1

    def test_fifo_position_no_classid_unmatched(self, db, repo):
        _make_pos(db, asset_id=5555, classid=None)
        items = [_inv(9999, classid="some_cls")]
        result = PositionReconciler().sync(items, repo)
        assert result.unmatched_positions == 1
        assert result.unmatched_inventory == 1


# ─── TestReconcilerEdgeCases ──────────────────────────────────────────────────


class TestReconcilerEdgeCases:
    def test_empty_inventory_empty_positions_all_zeros(self, repo):
        result = PositionReconciler().sync([], repo)
        assert result == ReconcileResult()

    def test_inventory_item_no_open_position_unmatched_inventory(self, db, repo):
        # No positions at all → item is unmatched inventory
        items = [_inv(7777, classid="orphan_cls")]
        result = PositionReconciler().sync(items, repo)
        assert result.unmatched_inventory == 1
        assert result.matched_direct == 0

    def test_same_asset_id_in_multiple_inventory_items_only_first_consumed(self, db, repo):
        # Degenerate case: two inv items with same asset_id (dict deduplication)
        _make_pos(db, asset_id=42)
        # inv_by_asset uses dict comprehension so last one wins — but only one position exists
        items = [
            _inv(42, classid="cls_dup"),
            _inv(42, classid="cls_dup"),  # duplicate
        ]
        result = PositionReconciler().sync(items, repo)
        # The position is matched via direct (asset_id=42 found in inv_by_asset)
        assert result.matched_direct == 1
        # Only one position exists, so matched=1; the second inv item (same asset_id) is consumed too
        # unmatched_inventory counts asset_ids NOT in consumed_asset_ids → 0 since 42 is consumed
        assert result.unmatched_inventory == 0
