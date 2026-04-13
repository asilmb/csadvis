"""
Tests for engine/wall_filter.py — pure wall filter functions.

All tests are pure unit tests — no DB, no network, no mocking required.
"""

from __future__ import annotations

import math

import pytest

from domain.wall_filter import compute_wall_metrics, get_best_buy_order

# ─── compute_wall_metrics ─────────────────────────────────────────────────────


class TestComputeWallMetrics:
    def test_empty_graph_passes(self) -> None:
        """Empty sell_order_graph → no wall → passes filter."""
        result = compute_wall_metrics(
            sell_order_graph=[],
            current_price=100.0,
            target_price=200.0,
            avg_daily_vol=10.0,
        )
        assert result["volume_to_target"] == 0
        assert result["estimated_days"] == pytest.approx(0.0)
        assert result["passes_wall_filter"] is True

    def test_all_orders_above_target_passes(self) -> None:
        """All sell orders priced above target → nothing to absorb → passes."""
        graph = [[300.0, 50, "300 ₸"], [400.0, 30, "400 ₸"]]
        result = compute_wall_metrics(
            sell_order_graph=graph,
            current_price=100.0,
            target_price=200.0,
            avg_daily_vol=10.0,
        )
        assert result["volume_to_target"] == 0
        assert result["passes_wall_filter"] is True

    def test_all_orders_in_range_counted(self) -> None:
        """All sell orders within [current, target] → full volume counted."""
        graph = [[110.0, 20, "110 ₸"], [150.0, 30, "150 ₸"], [190.0, 10, "190 ₸"]]
        result = compute_wall_metrics(
            sell_order_graph=graph,
            current_price=100.0,
            target_price=200.0,
            avg_daily_vol=10.0,
        )
        assert result["volume_to_target"] == 60  # 20 + 30 + 10

    def test_mixed_range_only_in_range_counted(self) -> None:
        """Only entries within [current, target] are counted; others ignored."""
        graph = [
            [50.0, 100, "50 ₸"],  # below current — excluded
            [110.0, 20, "110 ₸"],  # in range — included
            [190.0, 10, "190 ₸"],  # in range — included
            [250.0, 40, "250 ₸"],  # above target — excluded
        ]
        result = compute_wall_metrics(
            sell_order_graph=graph,
            current_price=100.0,
            target_price=200.0,
            avg_daily_vol=10.0,
        )
        assert result["volume_to_target"] == 30  # 20 + 10

    def test_zero_avg_daily_vol_with_volume_fails(self) -> None:
        """avg_daily_vol=0 and volume>0 → estimated_days=inf → fails filter."""
        graph = [[150.0, 50, "150 ₸"]]
        result = compute_wall_metrics(
            sell_order_graph=graph,
            current_price=100.0,
            target_price=200.0,
            avg_daily_vol=0.0,
        )
        assert result["volume_to_target"] == 50
        assert math.isinf(result["estimated_days"])
        assert result["passes_wall_filter"] is False

    def test_zero_avg_daily_vol_with_zero_volume_passes(self) -> None:
        """avg_daily_vol=0 and volume=0 → estimated_days=0 → passes filter."""
        result = compute_wall_metrics(
            sell_order_graph=[],
            current_price=100.0,
            target_price=200.0,
            avg_daily_vol=0.0,
        )
        assert result["volume_to_target"] == 0
        assert result["estimated_days"] == pytest.approx(0.0)
        assert result["passes_wall_filter"] is True

    def test_estimated_days_exactly_at_wall_max_passes(self) -> None:
        """estimated_days exactly == wall_max_days → boundary → passes."""
        # wall_max_days default is 7; volume=70, avg_daily_vol=10 → 7 days exactly
        graph = [[150.0, 70, "150 ₸"]]
        result = compute_wall_metrics(
            sell_order_graph=graph,
            current_price=100.0,
            target_price=200.0,
            avg_daily_vol=10.0,
        )
        assert result["estimated_days"] == pytest.approx(7.0)
        assert result["passes_wall_filter"] is True

    def test_estimated_days_just_above_wall_max_fails(self) -> None:
        """estimated_days just above wall_max_days → boundary + 1 → fails."""
        # volume=71, avg_daily_vol=10 → 7.1 days > 7
        graph = [[150.0, 71, "150 ₸"]]
        result = compute_wall_metrics(
            sell_order_graph=graph,
            current_price=100.0,
            target_price=200.0,
            avg_daily_vol=10.0,
        )
        assert result["estimated_days"] == pytest.approx(7.1)
        assert result["passes_wall_filter"] is False

    def test_estimated_days_computed_correctly(self) -> None:
        """estimated_days = volume_to_target / avg_daily_vol."""
        graph = [[120.0, 30, "120 ₸"]]
        result = compute_wall_metrics(
            sell_order_graph=graph,
            current_price=100.0,
            target_price=200.0,
            avg_daily_vol=5.0,
        )
        assert result["estimated_days"] == pytest.approx(6.0)

    def test_malformed_entries_skipped(self) -> None:
        """Malformed entries in graph are silently skipped, valid ones counted."""
        graph = [
            [150.0, 10, "150 ₸"],
            None,  # type: ignore[list-item]  # malformed
            [None, 5, ""],  # type: ignore[list-item]  # malformed price
            [160.0, 20, "160 ₸"],
        ]
        result = compute_wall_metrics(
            sell_order_graph=graph,
            current_price=100.0,
            target_price=200.0,
            avg_daily_vol=5.0,
        )
        assert result["volume_to_target"] == 30  # 10 + 20


# ─── get_best_buy_order ───────────────────────────────────────────────────────


class TestGetBestBuyOrder:
    def test_empty_graph_returns_zero(self) -> None:
        assert get_best_buy_order([]) == pytest.approx(0.0)

    def test_returns_first_entry_price(self) -> None:
        """buy_order_graph is sorted descending — first entry is best bid."""
        graph = [[500.0, 10, "500 ₸"], [400.0, 20, "400 ₸"], [300.0, 5, "300 ₸"]]
        assert get_best_buy_order(graph) == pytest.approx(500.0)

    def test_single_entry(self) -> None:
        graph = [[250.0, 3, "250 ₸"]]
        assert get_best_buy_order(graph) == pytest.approx(250.0)

    def test_malformed_entry_returns_zero(self) -> None:
        assert get_best_buy_order([[None]]) == pytest.approx(0.0)  # type: ignore[list-item]
