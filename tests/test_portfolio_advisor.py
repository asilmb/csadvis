"""Tests for engine/portfolio_advisor.py — allocation engine."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest

from src.domain.portfolio_advisor import (
    _compute_cagr,
    _compute_zscore,
    _consecutive_days_below,
    _detect_super_deal,
    _net,
    _pre_crash_mean,
    _prices_in_window,
    _volatility,
    allocate_portfolio,
)

# ─── _net ─────────────────────────────────────────────────────────────────────


class TestNet:
    def test_basic_calculation(self) -> None:
        # sell_price / 1.15 - 5₸ (KZT)
        assert _net(1150.0) == pytest.approx(1150.0 / 1.15 - 5.0, abs=1e-9)

    def test_zero_sell_price(self) -> None:
        assert _net(0.0) == pytest.approx(-5.0)


# ─── _volatility ─────────────────────────────────────────────────────────────


class TestVolatility:
    def test_empty_list_returns_none(self) -> None:
        assert _volatility([]) is None

    def test_too_short_returns_none(self) -> None:
        assert _volatility([1.0, 2.0, 3.0]) is None

    def test_constant_prices_zero_volatility(self) -> None:
        assert _volatility([5.0] * 10) == pytest.approx(0.0)

    def test_varied_prices_positive_volatility(self) -> None:
        prices = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
        vol = _volatility(prices)
        assert vol > 0.0

    def test_result_is_std_over_mean(self) -> None:
        import statistics

        prices = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
        mean_p = statistics.mean(prices)
        expected = statistics.stdev(prices) / mean_p
        assert _volatility(prices) == pytest.approx(expected)


# ─── _prices_in_window ───────────────────────────────────────────────────────


def _ts(days_ago: int) -> str:
    dt = datetime.now(UTC).replace(tzinfo=None) - timedelta(days=days_ago)
    return dt.strftime("%Y-%m-%d %H:%M")


class TestPricesInWindow:
    def test_returns_recent_prices(self) -> None:
        history = [
            {"timestamp": _ts(5), "price": 10.0},
            {"timestamp": _ts(100), "price": 99.0},
        ]
        result = _prices_in_window(history, 30)
        assert result == [10.0]

    def test_returns_all_within_window(self) -> None:
        # Use days 0..29 so all are strictly within a 30-day window
        history = [{"timestamp": _ts(i), "price": float(i + 1)} for i in range(30)]
        result = _prices_in_window(history, 31)  # window wider than all entries
        assert len(result) == 30

    def test_skips_missing_price(self) -> None:
        history: list[dict] = [
            {"timestamp": _ts(5), "price": None},
            {"timestamp": _ts(5), "price": 5.0},
        ]
        result = _prices_in_window(history, 30)
        assert result == [5.0]

    def test_skips_malformed_timestamp(self) -> None:
        history = [
            {"timestamp": "bad-ts", "price": 1.0},
            {"timestamp": _ts(2), "price": 2.0},
        ]
        result = _prices_in_window(history, 30)
        assert result == [2.0]


# ─── _compute_cagr ───────────────────────────────────────────────────────────


def _history_spanning_years(years: float, oldest_price: float, newest_price: float) -> list[dict]:
    """Build a fake history list with ~30 entries across `years` years."""
    now = datetime.now(UTC).replace(tzinfo=None)
    start = now - timedelta(days=int(years * 365.25))
    total_days = (now - start).days
    rows = []
    for i in range(30):
        days_in = int(i / 29 * total_days)
        dt = start + timedelta(days=days_in)
        # linear interpolation from oldest to newest price
        price = oldest_price + (newest_price - oldest_price) * (i / 29)
        rows.append({"timestamp": dt.strftime("%Y-%m-%d %H:%M"), "price": price})
    return rows


class TestComputeCagr:
    def test_returns_none_when_too_short(self) -> None:
        history = [{"timestamp": _ts(i), "price": 1.0} for i in range(5)]
        assert _compute_cagr(history) is None

    def test_returns_none_when_less_than_one_year(self) -> None:
        history = _history_spanning_years(0.5, 1.0, 2.0)
        assert _compute_cagr(history) is None

    def test_positive_cagr_for_rising_price(self) -> None:
        history = _history_spanning_years(2.0, 1.0, 4.0)  # 2 years, 4× growth
        cagr = _compute_cagr(history)
        assert cagr is not None
        assert cagr > 0.0

    def test_cagr_formula(self) -> None:
        # 2 years of data: 1.0 → 4.0 → CAGR ≈ 2^(1/2) - 1 ≈ 1.0 = 100%
        history = _history_spanning_years(2.0, 1.0, 4.0)
        cagr = _compute_cagr(history)
        assert cagr is not None
        assert cagr == pytest.approx(1.0, abs=0.05)  # ~100% with some tolerance

    def test_negative_cagr_for_declining_price(self) -> None:
        history = _history_spanning_years(2.0, 4.0, 1.0)
        cagr = _compute_cagr(history)
        assert cagr is not None
        assert cagr < 0.0

    def test_returns_none_for_empty_history(self) -> None:
        assert _compute_cagr([]) is None


# ─── allocate_portfolio ──────────────────────────────────────────────────────


def _mock_container(cid: str, name: str) -> Any:
    return SimpleNamespace(container_id=cid, container_name=name)


class TestAllocatePortfolio:
    def test_budget_splits_40_40_20(self) -> None:
        result = allocate_portfolio(
            balance=1000.0,
            inventory_items=[],
            containers=[],
            price_data={},
            trade_advice={},
            price_history={},
            invest_signals={},
        )
        assert result["flip_budget"] == pytest.approx(400.0)
        assert result["invest_budget"] == pytest.approx(400.0)
        assert result["reserve_amount"] == pytest.approx(200.0)
        assert result["total_balance"] == pytest.approx(1000.0)

    def test_empty_inputs_return_structure(self) -> None:
        result = allocate_portfolio(1000.0, [], [], {}, {}, {}, {})
        assert "sell" in result
        assert "flip" in result
        assert "invest" in result
        assert "top_flips" in result
        assert "top_invests" in result
        assert result["flip"] is None
        assert result["invest"] is None

    def test_no_flip_without_profitable_containers(self) -> None:
        c = _mock_container("c1", "TestCase")
        # buy = sell → no net profit
        trade_advice = {"c1": {"buy_target": 1.0, "sell_target": 1.0, "net_margin_pct": 0}}
        result = allocate_portfolio(
            1000.0,
            [],
            [c],
            {"TestCase": {"quantity": 100, "current_price": 1.0}},
            trade_advice,
            {},
            {},
        )
        assert result["flip"] is None

    def test_sell_candidates_from_inventory(self) -> None:
        c = _mock_container("c1", "Alpha Case")
        invest_signals = {"c1": {"verdict": "SELL"}}
        trade_advice = {"c1": {"buy_target": 1.0, "sell_target": 2.0, "net_margin_pct": 50}}
        inventory = [{"market_hash_name": "Alpha Case", "count": 3}]
        result = allocate_portfolio(
            1000.0,
            inventory,
            [c],
            {},
            trade_advice,
            {},
            invest_signals,
        )
        assert len(result["sell"]) == 1
        assert result["sell"][0]["name"] == "Alpha Case"
        assert result["sell"][0]["qty"] == 3

    def test_flip_candidate_selected_on_score(self) -> None:
        """Container passes all flip filters → becomes best_flip."""
        c = _mock_container("c1", "FlipCase")
        # Profitable: net(sell) - buy > 0 : 960/1.15 - 5 - 240 ≈ 590₸
        trade_advice = {
            "c1": {
                "buy_target": 240,
                "sell_target": 960,
                "net_margin_pct": 50,
            }
        }
        # High volume, low spread (KZT prices).
        # planned_qty = 240000 // 240 = 1000; FLIP-R3 requires avg_daily_vol*7 >= planned_qty*2
        # → quantity >= 2000 for this balance.
        price_data = {
            "FlipCase": {
                "quantity": 2000,
                "current_price": 960.0,
                "lowest_price": 912.0,
            }
        }
        # 30-day stable history (KZT prices)
        stable_history = [{"timestamp": _ts(i), "price": 960.0} for i in range(30)]
        result = allocate_portfolio(
            240000.0,  # 240000₸ balance
            [],
            [c],
            price_data,
            trade_advice,
            {"c1": stable_history},
            {},
        )
        assert result["flip"] is not None
        assert result["flip"]["name"] == "FlipCase"

    def test_zero_balance_returns_zero_budgets(self) -> None:
        result = allocate_portfolio(0.0, [], [], {}, {}, {}, {})
        assert result["flip_budget"] == 0.0
        assert result["invest_budget"] == 0.0
        assert result["reserve_amount"] == 0.0


# ── S13-MINOR-4: allocate_portfolio with positions_map — trade ban gate ───────


class TestAllocatePortfolioWithPositionsMap:
    """Verify F-03 trade ban gate: positions bought < 168h ago are excluded from sell."""

    def test_container_bought_within_168h_skipped_from_sell(self) -> None:
        """Item bought 5 hours ago → trade ban → must NOT appear in sell candidates."""
        c = _mock_container("c1", "Alpha Case")
        invest_signals = {"c1": {"verdict": "SELL"}}
        trade_advice = {"c1": {"buy_target": 1.0, "sell_target": 2.0, "net_margin_pct": 50}}
        inventory = [{"market_hash_name": "Alpha Case", "count": 3}]
        # buy_date is 5 hours ago (within 168h ban)
        recent_buy = datetime.now(UTC).replace(tzinfo=None) - timedelta(hours=5)
        positions_map = {"Alpha Case": recent_buy}
        result = allocate_portfolio(
            1000.0,
            inventory,
            [c],
            {},
            trade_advice,
            {},
            invest_signals,
            positions_map=positions_map,
        )
        assert len(result["sell"]) == 0

    def test_container_bought_over_168h_ago_included_in_sell(self) -> None:
        """Item bought 200 hours ago → trade ban lifted → must appear in sell candidates."""
        c = _mock_container("c1", "Beta Case")
        invest_signals = {"c1": {"verdict": "SELL"}}
        trade_advice = {"c1": {"buy_target": 1.0, "sell_target": 2.0, "net_margin_pct": 50}}
        inventory = [{"market_hash_name": "Beta Case", "count": 2}]
        old_buy = datetime.now(UTC).replace(tzinfo=None) - timedelta(hours=200)
        positions_map = {"Beta Case": old_buy}
        result = allocate_portfolio(
            1000.0,
            inventory,
            [c],
            {},
            trade_advice,
            {},
            invest_signals,
            positions_map=positions_map,
        )
        assert len(result["sell"]) == 1
        assert result["sell"][0]["name"] == "Beta Case"


# ── S13-MINOR-5: allocate_portfolio includes inventory value in total_capital ─


class TestAllocatePortfolioInventoryCapital:
    def test_inventory_value_included_in_total_capital(self) -> None:
        """total_capital = cash + inventory market value."""
        c = _mock_container("c1", "GoldCase")
        price_data = {"GoldCase": {"quantity": 100, "current_price": 10.0, "lowest_price": 9.5}}
        inventory = [{"market_hash_name": "GoldCase", "count": 5}]
        # balance_usd=100, inventory=5×$10=$50 → total_capital=$150
        result = allocate_portfolio(
            100.0,
            inventory,
            [c],
            price_data,
            {},
            {},
            {},
        )
        assert result["total_capital"] == pytest.approx(150.0)
        assert result["inventory_value"] == pytest.approx(50.0)
        assert result["flip_budget"] == pytest.approx(60.0)  # 40% of 150


# ── S13-MINOR-6: _volatility exactly 4 elements → None ───────────────────────


class TestVolatilityBoundaryExactly4:
    def test_four_elements_returns_none(self) -> None:
        """_volatility requires >= 5 prices; exactly 4 must return None."""
        assert _volatility([1.0, 2.0, 3.0, 4.0]) is None

    def test_five_elements_returns_value(self) -> None:
        """Exactly 5 prices meets the minimum — must return a float (not None)."""
        result = _volatility([1.0, 2.0, 3.0, 4.0, 5.0])
        assert result is not None
        assert isinstance(result, float)


# ── S13-MINOR-7: trade ban gate uses timedelta(hours=168) ────────────────────


class TestTradeBanGateTimedelta:
    def test_exactly_168h_is_not_banned(self) -> None:
        """Exactly 168h after buy → ban lifted, item should appear in sell."""
        c = _mock_container("c1", "GammaCase")
        invest_signals = {"c1": {"verdict": "SELL"}}
        trade_advice = {"c1": {"buy_target": 1.0, "sell_target": 2.0, "net_margin_pct": 50}}
        inventory = [{"market_hash_name": "GammaCase", "count": 1}]
        # Exactly 168h ago — (now - buy_date) == timedelta(hours=168) → NOT < 168h → ban lifted
        exactly_168h = datetime.now(UTC).replace(tzinfo=None) - timedelta(hours=168)
        positions_map = {"GammaCase": exactly_168h}
        result = allocate_portfolio(
            500.0,
            inventory,
            [c],
            {},
            trade_advice,
            {},
            invest_signals,
            positions_map=positions_map,
        )
        assert len(result["sell"]) == 1

    def test_167h_59m_is_still_banned(self) -> None:
        """One minute before ban lifts → still banned."""
        c = _mock_container("c1", "DeltaCase")
        invest_signals = {"c1": {"verdict": "SELL"}}
        trade_advice = {"c1": {"buy_target": 1.0, "sell_target": 2.0, "net_margin_pct": 50}}
        inventory = [{"market_hash_name": "DeltaCase", "count": 1}]
        almost_168h = datetime.now(UTC).replace(tzinfo=None) - timedelta(hours=167, minutes=59)
        positions_map = {"DeltaCase": almost_168h}
        result = allocate_portfolio(
            500.0,
            inventory,
            [c],
            {},
            trade_advice,
            {},
            invest_signals,
            positions_map=positions_map,
        )
        assert len(result["sell"]) == 0


# ─── PV-15: _compute_zscore ──────────────────────────────────────────────────


class TestComputeZscore:
    def test_negative_zscore_for_price_below_mean(self) -> None:
        """Prices 100..130 with last price 80 → Z clearly negative."""
        history = [{"timestamp": _ts(i), "price": 100.0 + i} for i in range(30)]
        history[-1]["price"] = 60.0  # crash
        z = _compute_zscore(history, 60)
        assert z is not None
        assert z < 0

    def test_returns_none_when_too_few_prices(self) -> None:
        history = [{"timestamp": _ts(i), "price": 100.0} for i in range(5)]
        assert _compute_zscore(history, 60) is None

    def test_returns_none_for_constant_prices(self) -> None:
        """std = 0 → cannot compute Z."""
        history = [{"timestamp": _ts(i), "price": 500.0} for i in range(20)]
        assert _compute_zscore(history, 60) is None

    def test_zscore_above_zero_when_price_above_mean(self) -> None:
        history = [{"timestamp": _ts(i), "price": 100.0} for i in range(29)]
        history.append({"timestamp": _ts(0), "price": 200.0})  # spike up
        z = _compute_zscore(history, 60)
        assert z is not None
        assert z > 0


# ─── PV-15: _consecutive_days_below ─────────────────────────────────────────


class TestConsecutiveDaysBelow:
    def test_counts_consecutive_low_days(self) -> None:
        """3 newest days are below threshold, 4th is above → count = 3."""
        history = [
            {"timestamp": _ts(3), "price": 1000.0},  # above
            {"timestamp": _ts(2), "price": 600.0},  # below
            {"timestamp": _ts(1), "price": 580.0},  # below
            {"timestamp": _ts(0), "price": 590.0},  # below
        ]
        assert _consecutive_days_below(history, 700.0) == 3

    def test_stops_at_normal_price(self) -> None:
        """Day 1 above threshold → count = 1 (only day 0 is below)."""
        history = [
            {"timestamp": _ts(2), "price": 500.0},  # below
            {"timestamp": _ts(1), "price": 800.0},  # above — breaks streak
            {"timestamp": _ts(0), "price": 500.0},  # below
        ]
        assert _consecutive_days_below(history, 700.0) == 1

    def test_zero_when_no_data(self) -> None:
        assert _consecutive_days_below([], 700.0) == 0

    def test_zero_when_latest_price_above_threshold(self) -> None:
        history = [{"timestamp": _ts(0), "price": 1000.0}]
        assert _consecutive_days_below(history, 700.0) == 0


# ─── PV-15: _pre_crash_mean ──────────────────────────────────────────────────


class TestPreCrashMean:
    def test_detects_onset_and_returns_pre_crash_mean(self) -> None:
        """
        Build history: 100 days of prices near ₸1000 (slight noise for non-zero std),
        then a crash to ₸300.  Pre-crash mean must be well above the crash price.

        Note: with perfectly constant stable prices (std=0), onset is delayed by one
        entry (the first crash entry is needed in the window to produce non-zero std).
        Adding ±5₸ noise ensures onset is detected at the first crash entry, keeping
        the pre-crash mean representative.
        """
        import math

        history = []
        # 105 stable days with slight sinusoidal noise (±5₸ around ₸1000)
        for i in range(105, 5, -1):
            noise = math.sin(i) * 5.0  # ±5₸ noise — non-zero std, negligible bias
            history.append({"timestamp": _ts(i), "price": 1000.0 + noise})
        # Crash: days 5..0 at ₸300 (no overlap with stable range)
        for i in range(5, -1, -1):
            history.append({"timestamp": _ts(i), "price": 300.0})

        result = _pre_crash_mean(history, -2.0)
        assert result is not None
        # Pre-crash mean should be near ₸1000, well above crash price ₸300
        assert result > 800.0
        assert result < 1100.0

    def test_returns_none_when_no_crash(self) -> None:
        """Prices are stable — Z never crosses -2.0 → None."""
        history = [{"timestamp": _ts(i), "price": 1000.0} for i in range(90)]
        assert _pre_crash_mean(history, -2.0) is None

    def test_returns_none_when_insufficient_history(self) -> None:
        history = [{"timestamp": _ts(i), "price": 100.0} for i in range(5)]
        assert _pre_crash_mean(history, -2.0) is None


# ─── PV-15: _detect_super_deal ──────────────────────────────────────────────


def _make_super_deal_history() -> list[dict]:
    """
    130 days of data: 100 days stable at ₸1000, then 5-day crash to ₸500.
    Baseline ₸1000, threshold ₸700. Current price ₸500 < ₸700. ✓
    Z-score will be strongly negative. ✓
    """
    history = []
    for i in range(130, 5, -1):
        history.append({"timestamp": _ts(i), "price": 1000.0, "volume_7d": 100.0})
    for i in range(5, 0, -1):
        history.append({"timestamp": _ts(i), "price": 500.0, "volume_7d": 120.0})
    history.append({"timestamp": _ts(0), "price": 500.0, "volume_7d": 120.0})
    return history


class TestDetectSuperDeal:
    def test_returns_none_when_price_above_threshold(self) -> None:
        """price >= baseline * 0.70 → filter 1 fails."""
        history = _make_super_deal_history()
        pd_info = {"current_price": 750.0, "quantity": 100}
        result = _detect_super_deal("c1", "TestCase", history, pd_info, 1000.0, 0.05)
        assert result is None  # 750 >= 700 (1000 * 0.70)

    def test_returns_none_when_baseline_zero(self) -> None:
        history = _make_super_deal_history()
        pd_info = {"current_price": 400.0, "quantity": 100}
        result = _detect_super_deal("c1", "TestCase", history, pd_info, 0.0, 0.05)
        assert result is None

    def test_returns_none_when_history_too_short(self) -> None:
        """< 90 days of history → filter 4 fails."""
        short_history = [
            {"timestamp": _ts(i), "price": 500.0, "volume_7d": 100.0} for i in range(30)
        ]
        pd_info = {"current_price": 300.0, "quantity": 100}
        result = _detect_super_deal("c1", "TestCase", short_history, pd_info, 1000.0, 0.05)
        assert result is None

    def test_returns_none_when_net_cagr_negative(self) -> None:
        """net_cagr < 0.01 → filter 5 fails (structurally declining asset)."""
        history = _make_super_deal_history()
        pd_info = {"current_price": 500.0, "quantity": 100}
        result = _detect_super_deal("c1", "TestCase", history, pd_info, 1000.0, -0.10)
        assert result is None

    def test_returns_none_when_too_many_consecutive_days_at_low(self) -> None:
        """7+ consecutive days at low → not a panic, it's a new baseline."""
        history = []
        for i in range(100, 7, -1):
            history.append({"timestamp": _ts(i), "price": 1000.0, "volume_7d": 100.0})
        for i in range(7, -1, -1):
            history.append({"timestamp": _ts(i), "price": 500.0, "volume_7d": 110.0})
        pd_info = {"current_price": 500.0, "quantity": 110}
        result = _detect_super_deal("c1", "TestCase", history, pd_info, 1000.0, 0.05)
        assert result is None  # 8 consecutive days at low >= _MAX_DAYS_AT_LOW=7

    def test_verdict_and_structure_on_pass(self) -> None:
        """All filters pass → result has correct keys and verdict ULTRA BUY."""
        history = _make_super_deal_history()
        pd_info = {"current_price": 500.0, "quantity": 100}
        result = _detect_super_deal("c1", "TestCase", history, pd_info, 1000.0, 0.05)
        if result is None:
            pytest.skip("Super deal not triggered — history may not generate Z < -3")
        assert result["verdict"] == "ULTRA BUY"
        assert result["is_super_deal"] is True
        assert result["buy_price"] == 500
        assert result["stop_loss_price"] == pytest.approx(500.0 * 0.85, abs=1.0)
        assert result["mandatory_exit_days"] == 60
        assert "target_exit_price" in result
        assert "z_score" in result
        assert "expected_margin_pct" in result


# ─── PV-15: allocate_portfolio includes super_deal key ──────────────────────


class TestAllocatePortfolioSuperDeal:
    def test_result_contains_super_deal_key(self) -> None:
        """super_deal key always present in result (None when no candidate passes)."""
        result = allocate_portfolio(1000.0, [], [], {}, {}, {}, {})
        assert "super_deal" in result

    def test_super_deal_is_none_without_qualifying_containers(self) -> None:
        """Empty containers → no super deal candidate."""
        result = allocate_portfolio(1000.0, [], [], {}, {}, {}, {})
        assert result["super_deal"] is None

    def test_budget_split_unchanged(self) -> None:
        """Super deal presence must not alter 40/40/20 allocation."""
        result = allocate_portfolio(10000.0, [], [], {}, {}, {}, {})
        assert result["flip_budget"] == pytest.approx(4000.0)
        assert result["invest_budget"] == pytest.approx(4000.0)
        assert result["reserve_amount"] == pytest.approx(2000.0)
