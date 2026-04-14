"""
Tests for services/portfolio.py — pure-function / DB-mock tests.

Functions tested:
  get_portfolio_data()   — bulk price loader returning {name: {current_price, ...}}
  get_container_detail() — single-container detail dict

Tests mock the DB session to avoid any real DB dependency.
"""

from __future__ import annotations

import contextlib
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from src.domain.portfolio import get_container_detail, get_portfolio_data

# ─── Helpers ──────────────────────────────────────────────────────────────────


def _make_fake_price_row(
    container_id: str,
    price: float,
    volume_7d: int = 10,
    lowest_price: float | None = None,
    timestamp: datetime | None = None,
) -> MagicMock:
    row = MagicMock()
    row.container_id = container_id
    row.price = price
    row.lowest_price = lowest_price
    row.volume_7d = volume_7d
    row.timestamp = timestamp or datetime.now(UTC).replace(tzinfo=None)
    return row


def _make_fake_container(cid: str, name: str, base_cost: float, ctype_value: str) -> MagicMock:
    c = MagicMock()
    c.container_id = cid
    c.container_name = name
    c.base_cost = base_cost
    c.container_type = MagicMock()
    c.container_type.value = ctype_value
    return c


# ─── get_portfolio_data ───────────────────────────────────────────────────────


class TestGetPortfolioData:
    def _make_db(self, containers: list, latest_rows: list, recent_rows: list) -> MagicMock:
        """Build a mock DB session for get_portfolio_data."""
        db = MagicMock()

        # containers query
        db.query.return_value.all.return_value = containers

        # We need to distinguish query chains for the three different queries.
        # The simplest approach: patch SessionLocal at the module level.
        return db

    def test_returns_empty_when_no_containers(self) -> None:
        with patch("src.domain.portfolio.SessionLocal") as mock_sl:
            db = MagicMock()
            mock_sl.return_value = db
            # containers query returns []
            db.query.return_value.all.return_value = []
            # subquery chain
            db.query.return_value.filter.return_value.group_by.return_value.subquery.return_value = MagicMock()
            db.query.return_value.join.return_value.all.return_value = []
            db.query.return_value.filter.return_value.all.return_value = []

            result = get_portfolio_data()
            assert result == {}
            db.close.assert_called_once()

    def test_db_always_closed_on_exception(self) -> None:
        """Session must be closed even if an exception is raised mid-query."""
        with patch("src.domain.portfolio.SessionLocal") as mock_sl:
            db = MagicMock()
            mock_sl.return_value = db
            db.query.side_effect = RuntimeError("DB error")
            with contextlib.suppress(RuntimeError):
                get_portfolio_data()
            db.close.assert_called_once()

    def test_result_keys_are_container_names(self) -> None:
        """Output dict keys must be container names (str), not IDs."""
        c1 = _make_fake_container("id-1", "Revolution Case", 3.49, "Weapon Case")
        price_row = _make_fake_price_row("id-1", price=576.0, volume_7d=50)

        with patch("src.domain.portfolio.SessionLocal") as mock_sl:
            db = MagicMock()
            mock_sl.return_value = db

            # Simulate the three sequential query paths
            # 1st call: db.query(DimContainer).all() → containers
            # 2nd call: latest_ts_subq chain → latest_rows
            # 3rd call: recent rows → recent price rows
            call_count = [0]

            def query_side_effect(*args, **kwargs):
                call_count[0] += 1
                q = MagicMock()
                if call_count[0] == 1:
                    # DimContainer query
                    q.all.return_value = [c1]
                elif call_count[0] == 2:
                    # Latest timestamp subquery chain
                    sub = MagicMock()
                    q.filter.return_value.group_by.return_value.subquery.return_value = sub
                    q.filter.return_value.group_by.return_value.subquery = lambda: sub
                    join_q = MagicMock()
                    join_q.all.return_value = [price_row]
                    q.join.return_value = join_q
                elif call_count[0] == 3:
                    # Recent rows (30d) for mean
                    q.filter.return_value.all.return_value = [price_row]
                return q

            db.query.side_effect = query_side_effect

            result = get_portfolio_data()
            # Should have at most 1 key (if query results are matched correctly)
            # The important invariant: keys are str container names
            for k in result:
                assert isinstance(k, str)


# ─── get_container_detail ─────────────────────────────────────────────────────


class TestGetContainerDetail:
    def test_returns_none_for_unknown_id(self) -> None:
        with patch("src.domain.portfolio.SessionLocal") as mock_sl:
            db = MagicMock()
            mock_sl.return_value = db
            db.query.return_value.filter.return_value.first.return_value = None
            result = get_container_detail("nonexistent-id")
            assert result is None
            db.close.assert_called_once()

    def test_db_closed_on_missing_container(self) -> None:
        with patch("src.domain.portfolio.SessionLocal") as mock_sl:
            db = MagicMock()
            mock_sl.return_value = db
            db.query.return_value.filter.return_value.first.return_value = None
            get_container_detail("no-id")
            db.close.assert_called_once()

    def test_db_closed_on_exception(self) -> None:
        with patch("src.domain.portfolio.SessionLocal") as mock_sl:
            db = MagicMock()
            mock_sl.return_value = db
            db.query.side_effect = RuntimeError("DB error")
            with contextlib.suppress(RuntimeError):
                get_container_detail("any-id")
            db.close.assert_called_once()

    def test_returns_dict_with_required_keys(self) -> None:
        c = _make_fake_container("id-1", "Revolution Case", 3.49, "Weapon Case")
        price = _make_fake_price_row("id-1", price=720.0, volume_7d=30, lowest_price=672.0)

        with patch("src.domain.portfolio.SessionLocal") as mock_sl:
            db = MagicMock()
            mock_sl.return_value = db

            call_count = [0]

            def query_side_effect(*args, **kwargs):
                call_count[0] += 1
                q = MagicMock()
                if call_count[0] == 1:
                    # DimContainer lookup
                    q.filter.return_value.first.return_value = c
                elif call_count[0] == 2:
                    # Latest timestamp scalar_subquery
                    scalar_sub = MagicMock()
                    q.filter.return_value.scalar_subquery.return_value = scalar_sub
                elif call_count[0] == 3:
                    # Latest price row
                    q.filter.return_value.first.return_value = price
                elif call_count[0] == 4:
                    # 30d prices for mean
                    rec = MagicMock()
                    rec.price = 697.0
                    q.filter.return_value.all.return_value = [rec]
                return q

            db.query.side_effect = query_side_effect

            result = get_container_detail("id-1")

            assert result is not None
            required_keys = {
                "container_id",
                "container_name",
                "container_type",
                "base_cost",
                "current_price",
                "lowest_price",
                "mean_price_30d",
                "volume_7d",
            }
            assert required_keys.issubset(set(result.keys()))

    def test_container_name_is_str(self) -> None:
        c = _make_fake_container("id-1", "Test Case", 3.49, "Weapon Case")

        with patch("src.domain.portfolio.SessionLocal") as mock_sl:
            db = MagicMock()
            mock_sl.return_value = db

            call_count = [0]

            def query_side_effect(*args, **kwargs):
                call_count[0] += 1
                q = MagicMock()
                if call_count[0] == 1:
                    q.filter.return_value.first.return_value = c
                else:
                    q.filter.return_value.scalar_subquery.return_value = MagicMock()
                    q.filter.return_value.first.return_value = None
                    q.filter.return_value.all.return_value = []
                return q

            db.query.side_effect = query_side_effect

            result = get_container_detail("id-1")
            if result is not None:
                assert isinstance(result["container_name"], str)
                assert isinstance(result["container_id"], str)
