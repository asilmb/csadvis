"""
Unit tests for trade-ban parsing logic (PV-29).

Tests:
  _extract_trade_unlock_at():
    - cache_expiration in owner_descriptions → future datetime returned
    - cache_expiration in descriptions (fallback key) → future datetime returned
    - cache_expiration in the past → None (already unlocked)
    - "Tradable After: ..." text pattern → correct datetime
    - "Tradable After: ..." in the past → None
    - tradable=1 item (no lock fields) → None
    - empty desc dict → None
    - malformed cache_expiration (non-numeric) → None (no crash)
    - malformed date text → None (no crash)
    - multiple entries, first has bad data, second has good → good value returned
    - cache_expiration takes priority over text pattern

  _parse_page():
    - trade_unlock_at present for locked item
    - trade_unlock_at=None for freely tradable item
    - empty assets list → empty result
    - asset with no matching description skipped

  _aggregate():
    - latest trade_unlock_at wins across duplicates
    - None trade_unlock_at not overwriting a real value
    - all None → grouped item has None

  update_trade_unlock_at() (SQLAlchemy repository):
    - matching position updated
    - multiple positions with same name all updated
    - no matching position → no-op (no crash)
    - unlock_at=None clears the field
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.domain.models import Base, DimUserPosition
from src.domain.sql_repositories import SqlAlchemyInventoryRepository
from scrapper.steam_inventory import _aggregate, _extract_trade_unlock_at, _parse_page

# ─── Helpers ──────────────────────────────────────────────────────────────────


def _future(seconds: int = 86400) -> datetime:
    """UTC datetime that is `seconds` in the future (naive)."""
    return datetime.now(UTC).replace(tzinfo=None) + timedelta(seconds=seconds)


def _past(seconds: int = 86400) -> datetime:
    """UTC datetime that is `seconds` in the past (naive)."""
    return datetime.now(UTC).replace(tzinfo=None) - timedelta(seconds=seconds)


def _ts(dt: datetime) -> str:
    """Convert naive UTC datetime to Unix timestamp string."""
    return str(int(dt.replace(tzinfo=UTC).timestamp()))


def _future_text(dt: datetime | None = None) -> str:
    """Format a datetime as Steam's 'Tradable After:' text."""
    if dt is None:
        dt = _future()
    return f"Tradable After: {dt.strftime('%b %d, %Y (%H:%M:%S) GMT')}"


# ─── _extract_trade_unlock_at ─────────────────────────────────────────────────


class TestExtractTradeUnlockAt:
    def test_cache_expiration_in_owner_descriptions(self):
        future = _future()
        desc = {"owner_descriptions": [{"app_data": {"cache_expiration": _ts(future)}}]}
        result = _extract_trade_unlock_at(desc)
        assert result is not None
        assert abs((result - future).total_seconds()) < 2

    def test_cache_expiration_in_descriptions_fallback(self):
        future = _future()
        desc = {"descriptions": [{"app_data": {"cache_expiration": _ts(future)}}]}
        result = _extract_trade_unlock_at(desc)
        assert result is not None

    def test_past_cache_expiration_returns_none(self):
        desc = {"owner_descriptions": [{"app_data": {"cache_expiration": _ts(_past())}}]}
        assert _extract_trade_unlock_at(desc) is None

    def test_text_pattern_future(self):
        future = _future(seconds=7 * 86400)
        desc = {"owner_descriptions": [{"value": _future_text(future)}]}
        result = _extract_trade_unlock_at(desc)
        assert result is not None
        # Allow ±60s because strftime second-level precision
        assert abs((result - future).total_seconds()) < 60

    def test_text_pattern_past_returns_none(self):
        past_dt = _past()
        past_text = f"Tradable After: {past_dt.strftime('%b %d, %Y (%H:%M:%S) GMT')}"
        desc = {"owner_descriptions": [{"value": past_text}]}
        assert _extract_trade_unlock_at(desc) is None

    def test_freely_tradable_item_returns_none(self):
        # No owner_descriptions, no trade lock in descriptions
        desc = {
            "tradable": 1,
            "descriptions": [{"value": "Exterior: Field-Tested", "type": "html"}],
        }
        assert _extract_trade_unlock_at(desc) is None

    def test_empty_desc_returns_none(self):
        assert _extract_trade_unlock_at({}) is None

    def test_malformed_cache_expiration_non_numeric(self):
        desc = {"owner_descriptions": [{"app_data": {"cache_expiration": "not_a_number"}}]}
        assert _extract_trade_unlock_at(desc) is None  # no crash

    def test_malformed_date_text_returns_none(self):
        desc = {"owner_descriptions": [{"value": "Tradable After: NotADate GMT"}]}
        assert _extract_trade_unlock_at(desc) is None  # no crash

    def test_multiple_entries_bad_then_good(self):
        future = _future()
        desc = {
            "owner_descriptions": [
                {"app_data": {"cache_expiration": "bad"}},
                {"app_data": {"cache_expiration": _ts(future)}},
            ]
        }
        result = _extract_trade_unlock_at(desc)
        assert result is not None

    def test_cache_expiration_takes_priority_over_text(self):
        """cache_expiration is checked before text in each entry."""
        future_ts = _future(seconds=2 * 86400)
        future_text_only = _future(seconds=10 * 86400)
        desc = {
            "owner_descriptions": [
                {
                    "app_data": {"cache_expiration": _ts(future_ts)},
                    "value": _future_text(future_text_only),
                }
            ]
        }
        result = _extract_trade_unlock_at(desc)
        # Should return the timestamp from cache_expiration, not the text
        assert result is not None
        assert abs((result - future_ts).total_seconds()) < 2

    def test_none_app_data_graceful(self):
        desc = {"owner_descriptions": [{"app_data": None, "value": ""}]}
        assert _extract_trade_unlock_at(desc) is None

    def test_non_dict_entry_in_list_skipped(self):
        future = _future()
        desc = {
            "owner_descriptions": [
                "not a dict",
                {"app_data": {"cache_expiration": _ts(future)}},
            ]
        }
        result = _extract_trade_unlock_at(desc)
        assert result is not None

    def test_owner_descriptions_checked_before_descriptions(self):
        """If owner_descriptions has a valid expiry, descriptions should not matter."""
        future_owner = _future(seconds=1 * 86400)
        future_desc = _future(seconds=5 * 86400)
        desc = {
            "owner_descriptions": [{"app_data": {"cache_expiration": _ts(future_owner)}}],
            "descriptions": [{"app_data": {"cache_expiration": _ts(future_desc)}}],
        }
        result = _extract_trade_unlock_at(desc)
        assert result is not None
        assert abs((result - future_owner).total_seconds()) < 2


# ─── _parse_page ──────────────────────────────────────────────────────────────


class TestParsePage:
    def _make_page(self, *, tradable: int = 1, owner_descriptions: list | None = None) -> dict:
        """Minimal Steam inventory page for one item."""
        desc: dict = {
            "classid": "1",
            "instanceid": "0",
            "market_hash_name": "AK-47 | Redline (Field-Tested)",
            "name": "AK-47 | Redline",
            "type": "Classified Rifle",
            "tradable": tradable,
            "marketable": 1,
            "icon_url": "abc123",
            "descriptions": [],
            "tags": [],
        }
        if owner_descriptions is not None:
            desc["owner_descriptions"] = owner_descriptions
        return {
            "assets": [{"assetid": "111", "classid": "1", "instanceid": "0", "amount": "1"}],
            "descriptions": [desc],
        }

    def test_freely_tradable_item_has_none_unlock(self):
        page = self._make_page(tradable=1)
        items = _parse_page(page)
        assert len(items) == 1
        assert items[0]["trade_unlock_at"] is None

    def test_trade_locked_item_has_unlock_date(self):
        future = _future()
        owner_descs = [{"app_data": {"cache_expiration": _ts(future)}}]
        page = self._make_page(tradable=0, owner_descriptions=owner_descs)
        items = _parse_page(page)
        assert len(items) == 1
        assert items[0]["trade_unlock_at"] is not None

    def test_empty_assets_returns_empty(self):
        page = {"assets": [], "descriptions": []}
        assert _parse_page(page) == []

    def test_asset_without_matching_description_skipped(self):
        page = {
            "assets": [{"assetid": "999", "classid": "99", "instanceid": "0"}],
            "descriptions": [],  # no matching description
        }
        # No market_hash_name → item skipped
        assert _parse_page(page) == []

    def test_item_dict_contains_trade_unlock_at_key(self):
        """trade_unlock_at key must always be present (even if None)."""
        page = self._make_page()
        items = _parse_page(page)
        assert "trade_unlock_at" in items[0]


# ─── _aggregate ───────────────────────────────────────────────────────────────


class TestAggregate:
    def _item(self, mhn: str, asset_id: str, unlock: datetime | None) -> dict:
        return {
            "asset_id": asset_id,
            "market_hash_name": mhn,
            "name": mhn,
            "item_type": "Rifle",
            "rarity": "",
            "tradable": 0 if unlock else 1,
            "marketable": 1,
            "icon_url": "",
            "count": 1,
            "trade_unlock_at": unlock,
        }

    def test_all_none_yields_none(self):
        items = [self._item("AK", "1", None), self._item("AK", "2", None)]
        result = _aggregate(items)
        assert result[0]["trade_unlock_at"] is None

    def test_latest_unlock_wins(self):
        earlier = _future(seconds=86400)
        later = _future(seconds=2 * 86400)
        items = [self._item("AK", "1", earlier), self._item("AK", "2", later)]
        result = _aggregate(items)
        assert abs((result[0]["trade_unlock_at"] - later).total_seconds()) < 2

    def test_none_does_not_overwrite_real_value(self):
        future = _future()
        items = [self._item("AK", "1", future), self._item("AK", "2", None)]
        result = _aggregate(items)
        assert result[0]["trade_unlock_at"] is not None

    def test_count_summed(self):
        items = [self._item("AK", "1", None), self._item("AK", "2", None)]
        result = _aggregate(items)
        assert result[0]["count"] == 2


# ─── update_trade_unlock_at (repository) ──────────────────────────────────────


@pytest.fixture()
def db():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


@pytest.fixture()
def repo(db):
    return SqlAlchemyInventoryRepository(db)


class TestUpdateTradeUnlockAt:
    def _add_position(self, db: Session, name: str) -> DimUserPosition:
        pos = DimUserPosition(container_name=name, buy_price=1000.0, quantity=1)
        db.add(pos)
        db.flush()
        return pos

    def test_updates_matching_position(self, repo, db):
        self._add_position(db, "AK-47 | Redline (Field-Tested)")
        unlock = _future()
        repo.update_trade_unlock_at("AK-47 | Redline (Field-Tested)", unlock)
        db.commit()

        row = db.query(DimUserPosition).first()
        assert row.trade_unlock_at is not None
        assert abs((row.trade_unlock_at - unlock).total_seconds()) < 2

    def test_updates_multiple_positions_with_same_name(self, repo, db):
        for _ in range(3):
            self._add_position(db, "AK-47 | Vulcan (Factory New)")
        unlock = _future()
        repo.update_trade_unlock_at("AK-47 | Vulcan (Factory New)", unlock)
        db.commit()

        rows = db.query(DimUserPosition).all()
        assert all(r.trade_unlock_at is not None for r in rows)

    def test_no_matching_position_is_noop(self, repo, db):
        self._add_position(db, "Other Item")
        repo.update_trade_unlock_at("AK-47 | Redline", _future())
        db.commit()  # must not raise

        row = db.query(DimUserPosition).first()
        assert row.trade_unlock_at is None  # untouched

    def test_none_clears_field(self, repo, db):
        pos = self._add_position(db, "AK-47 | Redline (Field-Tested)")
        pos.trade_unlock_at = _future()
        db.commit()

        repo.update_trade_unlock_at("AK-47 | Redline (Field-Tested)", None)
        db.commit()

        db.expire_all()
        row = db.query(DimUserPosition).first()
        assert row.trade_unlock_at is None
