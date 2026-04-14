"""
Unit tests for the Position domain entity (src/domain/models.py).

Covers the entity-level business rules introduced by the Anemic-Domain-Model
refactoring (task 3):

  Position.open() factory
    - returns a Position with status=OPEN
    - raises InvalidPositionError for non-positive buy_price
    - raises InvalidPositionError for quantity < 1
    - quantity defaults to 1

  position.close()
    - transitions OPEN → CLOSED
    - sets closed_at to a datetime
    - raises PositionAlreadyClosedError when called twice (double-close guard)

  position.update_identity()
    - updates asset_id unconditionally
    - updates classid / market_id only when not None
    - sets is_on_market as int (0/1)

  position.list_on_market()
    - sets is_on_market = 1 and records market_id
    - raises PositionAlreadyClosedError on a CLOSED position

  position.delist_from_market()
    - sets is_on_market = 0 (does not clear market_id)

These tests are intentionally free of SQLAlchemy sessions — the entity methods
must be exercisable in pure Python so they remain unit-testable without a DB.
"""

from __future__ import annotations

import pytest

from src.domain.models import (
    InvalidPositionError,
    Position,
    PositionAlreadyClosedError,
    PositionStatus,
)


# ─── Position.open() ──────────────────────────────────────────────────────────


class TestPositionOpen:
    def test_returns_open_status(self):
        p = Position.open(asset_id=1, market_hash_name="AK-47", buy_price=5000.0)
        assert p.status == PositionStatus.OPEN

    def test_default_quantity_is_one(self):
        p = Position.open(asset_id=1, market_hash_name="AK-47", buy_price=5000.0)
        assert p.quantity == 1

    def test_custom_quantity_stored(self):
        p = Position.open(asset_id=1, market_hash_name="AK-47", buy_price=5000.0, quantity=3)
        assert p.quantity == 3

    def test_fields_stored_correctly(self):
        p = Position.open(asset_id=99, market_hash_name="M4A1-S", buy_price=1234.5, quantity=2)
        assert p.asset_id == 99
        assert p.market_hash_name == "M4A1-S"
        assert p.buy_price == pytest.approx(1234.5)

    def test_zero_buy_price_raises(self):
        with pytest.raises(InvalidPositionError, match="buy_price"):
            Position.open(asset_id=1, market_hash_name="Item", buy_price=0.0)

    def test_negative_buy_price_raises(self):
        with pytest.raises(InvalidPositionError, match="buy_price"):
            Position.open(asset_id=1, market_hash_name="Item", buy_price=-100.0)

    def test_zero_quantity_raises(self):
        with pytest.raises(InvalidPositionError, match="quantity"):
            Position.open(asset_id=1, market_hash_name="Item", buy_price=1000.0, quantity=0)

    def test_negative_quantity_raises(self):
        with pytest.raises(InvalidPositionError, match="quantity"):
            Position.open(asset_id=1, market_hash_name="Item", buy_price=1000.0, quantity=-1)

    def test_closed_at_is_none(self):
        p = Position.open(asset_id=1, market_hash_name="Item", buy_price=1000.0)
        assert p.closed_at is None


# ─── position.close() ─────────────────────────────────────────────────────────


class TestPositionClose:
    def _open_position(self) -> Position:
        return Position.open(asset_id=1, market_hash_name="AK-47", buy_price=5000.0)

    def test_transitions_to_closed(self):
        p = self._open_position()
        p.close()
        assert p.status == PositionStatus.CLOSED

    def test_sets_closed_at(self):
        from datetime import datetime
        p = self._open_position()
        p.close()
        assert isinstance(p.closed_at, datetime)

    def test_double_close_raises(self):
        p = self._open_position()
        p.close()
        with pytest.raises(PositionAlreadyClosedError):
            p.close()

    def test_double_close_error_message_contains_id(self):
        p = self._open_position()
        p.id = "test-uuid-123"
        p.close()
        with pytest.raises(PositionAlreadyClosedError, match="test-uuid-123"):
            p.close()

    def test_close_preserves_other_fields(self):
        p = self._open_position()
        p.close()
        assert p.asset_id == 1
        assert p.market_hash_name == "AK-47"
        assert p.buy_price == pytest.approx(5000.0)


# ─── position.update_identity() ───────────────────────────────────────────────


class TestUpdateIdentity:
    def _open_position(self) -> Position:
        p = Position.open(asset_id=1, market_hash_name="AK-47", buy_price=5000.0)
        p.classid = "old-class"
        p.market_id = None
        p.is_on_market = 0
        return p

    def test_updates_asset_id(self):
        p = self._open_position()
        p.update_identity(new_asset_id=999)
        assert p.asset_id == 999

    def test_updates_classid_when_provided(self):
        p = self._open_position()
        p.update_identity(new_asset_id=999, new_classid="new-class")
        assert p.classid == "new-class"

    def test_does_not_update_classid_when_none(self):
        p = self._open_position()
        p.update_identity(new_asset_id=999, new_classid=None)
        assert p.classid == "old-class"

    def test_updates_market_id_when_provided(self):
        p = self._open_position()
        p.update_identity(new_asset_id=999, new_market_id="listing-42")
        assert p.market_id == "listing-42"

    def test_does_not_update_market_id_when_none(self):
        p = self._open_position()
        p.market_id = "existing-listing"
        p.update_identity(new_asset_id=999, new_market_id=None)
        assert p.market_id == "existing-listing"

    def test_sets_is_on_market_as_int(self):
        p = self._open_position()
        p.update_identity(new_asset_id=999, is_on_market=True)
        assert p.is_on_market == 1

    def test_sets_is_on_market_false(self):
        p = self._open_position()
        p.is_on_market = 1
        p.update_identity(new_asset_id=999, is_on_market=False)
        assert p.is_on_market == 0

    def test_is_on_market_unchanged_when_none(self):
        p = self._open_position()
        p.is_on_market = 1
        p.update_identity(new_asset_id=999, is_on_market=None)
        assert p.is_on_market == 1


# ─── position.list_on_market() ────────────────────────────────────────────────


class TestListOnMarket:
    def test_sets_is_on_market_to_one(self):
        p = Position.open(asset_id=1, market_hash_name="AK", buy_price=1000.0)
        p.list_on_market("listing-99")
        assert p.is_on_market == 1

    def test_records_market_id(self):
        p = Position.open(asset_id=1, market_hash_name="AK", buy_price=1000.0)
        p.list_on_market("listing-99")
        assert p.market_id == "listing-99"

    def test_raises_on_closed_position(self):
        p = Position.open(asset_id=1, market_hash_name="AK", buy_price=1000.0)
        p.close()
        with pytest.raises(PositionAlreadyClosedError):
            p.list_on_market("listing-99")


# ─── position.delist_from_market() ───────────────────────────────────────────


class TestDelistFromMarket:
    def test_sets_is_on_market_to_zero(self):
        p = Position.open(asset_id=1, market_hash_name="AK", buy_price=1000.0)
        p.is_on_market = 1
        p.delist_from_market()
        assert p.is_on_market == 0

    def test_preserves_market_id(self):
        p = Position.open(asset_id=1, market_hash_name="AK", buy_price=1000.0)
        p.market_id = "listing-old"
        p.is_on_market = 1
        p.delist_from_market()
        assert p.market_id == "listing-old"  # history preserved

    def test_idempotent_on_unlisted_position(self):
        p = Position.open(asset_id=1, market_hash_name="AK", buy_price=1000.0)
        p.is_on_market = 0
        p.delist_from_market()  # no-op, must not raise
        assert p.is_on_market == 0
