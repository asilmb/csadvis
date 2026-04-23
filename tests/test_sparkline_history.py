"""
Tests for 90-day history rendering — _build_sparkline and _build_price_chart.

Synthetic data mimics the real Fever Case DB snapshot:
  - 365 daily entries, 2025-04-19 → 2026-04-19
  - Price ~430 ₸, gentle upward drift
  - Buy target ~208 ₸, sell target ~294 ₸  (both well below current price)

The key regression covered here: after the sparkline fix the y-axis range must
include buy/sell targets even when they fall outside the price data range.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

# ── Synthetic Fever Case history ──────────────────────────────────────────────

_DAYS = 365
_BASE_PRICE = 395.0       # start of year
_CURRENT_PRICE = 432.0    # last entry (~+9% drift)
_BUY_TARGET = 208.0       # well below all prices
_SELL_TARGET = 294.0      # also below all prices

_TODAY = datetime.now(UTC).replace(tzinfo=None)


def _make_history(days: int = _DAYS) -> list[dict]:
    """Generate `days` daily price entries ending on _TODAY."""
    entries = []
    for i in range(days):
        dt = _TODAY - timedelta(days=days - 1 - i)
        price = _BASE_PRICE + (_CURRENT_PRICE - _BASE_PRICE) * i / max(days - 1, 1)
        entries.append({
            "timestamp": dt.strftime("%Y-%m-%d %H:%M:%S"),
            "price": round(price, 2),
            "mean_price": round(price * 1.02, 2),
            "volume_7d": 50 + i % 20,
        })
    return entries


# ── _build_sparkline ──────────────────────────────────────────────────────────


class TestSparkline90DayWindow:
    """Sparkline must show at most 90 days regardless of history length."""

    def test_full_year_data_limited_to_90_days(self) -> None:
        from ui.helpers import _build_sparkline

        history = _make_history(365)
        fig = _build_sparkline(history)

        assert len(fig.data) == 1
        xs = fig.data[0].x
        assert len(xs) <= 90, f"Expected ≤90 x-values, got {len(xs)}"

    def test_short_history_shows_all_entries(self) -> None:
        from ui.helpers import _build_sparkline

        history = _make_history(30)
        fig = _build_sparkline(history)

        assert len(fig.data) == 1
        assert len(fig.data[0].x) == 30

    def test_exactly_90_days_are_included(self) -> None:
        from ui.helpers import _build_sparkline

        history = _make_history(90)
        fig = _build_sparkline(history)

        assert len(fig.data[0].x) == 90


class TestSparklineYAxisIncludesTargets:
    """
    Regression: before the fix the y-axis was scaled to price data only.
    Buy/sell targets below the price range were clipped off-screen.
    After the fix y_range must contain both targets.
    """

    def test_buy_target_below_prices_included_in_yrange(self) -> None:
        from ui.helpers import _build_sparkline

        history = _make_history(90)
        fig = _build_sparkline(history, buy_target=_BUY_TARGET)

        y_range = fig.layout.yaxis.range
        assert y_range is not None, "y_range must be set when history is non-empty"
        assert y_range[0] <= _BUY_TARGET, (
            f"y_range[0]={y_range[0]} must be ≤ buy_target={_BUY_TARGET}"
        )

    def test_sell_target_below_prices_included_in_yrange(self) -> None:
        from ui.helpers import _build_sparkline

        history = _make_history(90)
        fig = _build_sparkline(history, sell_target=_SELL_TARGET)

        y_range = fig.layout.yaxis.range
        assert y_range is not None
        assert y_range[0] <= _SELL_TARGET, (
            f"y_range[0]={y_range[0]} must be ≤ sell_target={_SELL_TARGET}"
        )

    def test_both_targets_in_yrange(self) -> None:
        from ui.helpers import _build_sparkline

        history = _make_history(90)
        fig = _build_sparkline(history, buy_target=_BUY_TARGET, sell_target=_SELL_TARGET)

        y_range = fig.layout.yaxis.range
        assert y_range is not None
        assert y_range[0] <= _BUY_TARGET
        assert y_range[0] <= _SELL_TARGET
        assert y_range[1] >= _CURRENT_PRICE

    def test_target_above_prices_expands_yrange_upward(self) -> None:
        from ui.helpers import _build_sparkline

        high_target = _CURRENT_PRICE * 2
        history = _make_history(90)
        fig = _build_sparkline(history, sell_target=high_target)

        y_range = fig.layout.yaxis.range
        assert y_range[1] >= high_target


class TestSparklineLineColor:
    def test_upward_trend_is_green(self) -> None:
        from ui.helpers import _GREEN, _build_sparkline

        history = _make_history(90)  # _BASE_PRICE → _CURRENT_PRICE, upward
        fig = _build_sparkline(history)
        assert fig.data[0].line.color == _GREEN

    def test_downward_trend_is_red(self) -> None:
        from ui.helpers import _RED, _build_sparkline

        # Reverse: start high, end low
        history = list(reversed(_make_history(90)))
        # Fix timestamps so they're still chronological with reversed prices
        for i, entry in enumerate(history):
            dt = _TODAY - timedelta(days=89 - i)
            entry["timestamp"] = dt.strftime("%Y-%m-%d %H:%M:%S")
        fig = _build_sparkline(history)
        assert fig.data[0].line.color == _RED


class TestSparklineEmptyHistory:
    def test_no_data_annotation_when_history_empty(self) -> None:
        from ui.helpers import _build_sparkline

        fig = _build_sparkline([])

        assert len(fig.data) == 0
        annotations = fig.layout.annotations
        assert len(annotations) == 1
        assert "No data" in annotations[0].text

    def test_no_data_yrange_is_none(self) -> None:
        from ui.helpers import _build_sparkline

        fig = _build_sparkline([])
        assert fig.layout.yaxis.range is None

    def test_history_with_no_price_field_treated_as_empty(self) -> None:
        from ui.helpers import _build_sparkline

        history = [{"timestamp": "2026-01-01", "price": None, "mean_price": None}]
        fig = _build_sparkline(history)
        assert len(fig.data) == 0


# ── _build_price_chart ────────────────────────────────────────────────────────


class TestPriceChartFeverCase:
    """Full price chart — uses entire history, not just 90 days."""

    def test_full_year_shows_all_entries(self) -> None:
        from ui.helpers import _build_price_chart

        history = _make_history(365)
        fig = _build_price_chart(history, "Fever Case")

        median_trace = fig.data[0]
        assert len(median_trace.x) == 365

    def test_has_median_and_mean_traces(self) -> None:
        from ui.helpers import _build_price_chart

        history = _make_history(365)
        fig = _build_price_chart(history, "Fever Case")

        assert len(fig.data) == 2
        names = [t.name for t in fig.data]
        assert "Median Price" in names
        assert "Mean Price" in names

    def test_title_matches_container_name(self) -> None:
        from ui.helpers import _build_price_chart

        fig = _build_price_chart(_make_history(10), "Fever Case")
        assert "Fever Case" in fig.layout.title.text

    def test_empty_history_shows_annotation(self) -> None:
        from ui.helpers import _build_price_chart

        fig = _build_price_chart([], "Fever Case")

        assert len(fig.data) == 0
        assert len(fig.layout.annotations) == 1
        assert "No price history" in fig.layout.annotations[0].text

    def test_no_mean_trace_when_all_mean_prices_are_none(self) -> None:
        from ui.helpers import _build_price_chart

        history = [
            {"timestamp": f"2026-01-{i:02d}", "price": 430.0, "mean_price": None}
            for i in range(1, 11)
        ]
        fig = _build_price_chart(history, "Fever Case")

        assert len(fig.data) == 1
        assert fig.data[0].name == "Median Price"
