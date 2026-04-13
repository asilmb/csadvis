"""Tests for engine/trade_advisor.py — buy/sell targets and margins."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from domain.trade_advisor import _percentile, compute_trade_advice


def _ts(days_ago: int) -> str:
    dt = datetime.now(UTC).replace(tzinfo=None) - timedelta(days=days_ago)
    return dt.strftime("%Y-%m-%d %H:%M")


# ─── helpers ──────────────────────────────────────────────────────────────────


def _patched_advice(
    container_name: str,
    base_cost: float,
    container_type: str,
    price_history_rows: list,
) -> dict:
    """Call compute_trade_advice (no EUR rate needed)."""
    return compute_trade_advice(
        container_name,
        base_cost,
        container_type,
        price_history_rows,
    )


def _stable_history(n: int = 30, price: float = 900.0) -> list[dict]:
    return [{"timestamp": _ts(i), "price": price} for i in range(n)]


# ─── compute_trade_advice — basic structure ───────────────────────────────────


class TestComputeTradeAdvice:
    def test_returns_expected_keys(self) -> None:
        result = _patched_advice("TestCase", 960.0, "Weapon Case", _stable_history())
        expected_keys = {
            "buy_target",
            "sell_target",
            "net_margin_pct",
            "hold_detail",
            "data_source",
            "baseline",
        }
        assert expected_keys.issubset(result.keys())

    def test_sell_target_above_buy_target(self) -> None:
        # 70th percentile > 20th percentile on any non-trivial history
        history = [{"timestamp": _ts(i), "price": float((i + 1) * 50)} for i in range(30)]
        result = _patched_advice("TestCase", 960.0, "Weapon Case", history)
        assert result["sell_target"] >= result["buy_target"]

    def test_fallback_when_insufficient_history(self) -> None:
        # Fewer than 5 snapshots → baseline fallback
        history = [{"timestamp": _ts(i), "price": 900.0} for i in range(3)]
        result = _patched_advice("TestCase", 960.0, "Weapon Case", history)
        assert result["data_source"] == "baseline_fallback"

    def test_history_source_when_enough_data(self) -> None:
        result = _patched_advice("TestCase", 960.0, "Weapon Case", _stable_history(30))
        assert result["data_source"] == "90d_steam"

    def test_baseline_subtracts_key_for_weapon_case(self) -> None:
        # Weapon Case baseline = cost_kzt - key_price_kzt (1200)
        result = _patched_advice("TestCase", 2400.0, "Weapon Case", [])
        assert result["baseline"] == pytest.approx(2400.0 - 1200.0, abs=1.0)

    def test_baseline_full_price_for_capsule(self) -> None:
        result = _patched_advice("TestCapsule", 240.0, "Sticker Capsule", [])
        assert result["baseline"] == pytest.approx(240.0, abs=1.0)

    def test_net_margin_formula(self) -> None:
        result = _patched_advice("TestCase", 960.0, "Weapon Case", _stable_history(30))
        sell = result["sell_target"]
        buy = result["buy_target"]
        # Steam: seller receives sell / 1.15 - 5₸
        net_proceeds = sell / 1.15 - 5.0
        expected_margin = (net_proceeds - buy) / buy * 100 if buy > 0 else 0.0
        assert result["net_margin_pct"] == pytest.approx(expected_margin, abs=0.5)

    def test_hold_detail_uncertain_when_margin_too_low(self) -> None:
        # Use tiny prices so margin is negative
        history = [{"timestamp": _ts(i), "price": 10.0} for i in range(30)]
        result = _patched_advice("TestCase", 10.0, "Weapon Case", history)
        assert "UNCERTAIN" in result["hold_detail"]

    def test_hold_detail_has_sell_price_when_profitable(self) -> None:
        # Large price spread → profitable
        history = [{"timestamp": _ts(i), "price": float((i + 1) * 50)} for i in range(30)]
        result = _patched_advice("TestCase", 50.0, "Sticker Capsule", history)
        if "UNCERTAIN" not in result["hold_detail"]:
            assert "SELL at" in result["hold_detail"]

    def test_old_prices_excluded_from_90d_window(self) -> None:
        # Mix: 5 recent prices at 2400₸ + 20 old prices at 50₸
        recent = [{"timestamp": _ts(i), "price": 2400.0} for i in range(5)]
        old = [{"timestamp": _ts(100 + i), "price": 50.0} for i in range(20)]
        result = _patched_advice("TestCase", 480.0, "Weapon Case", recent + old)
        # 5 >= 5 → uses history
        assert result["data_source"] == "90d_steam"
        # Targets should reflect the 2400₸ recent prices, not 50₸ old ones
        assert result["buy_target"] > 480

    def test_minimum_baseline_is_25_kzt(self) -> None:
        # Even if base_cost_kzt is very small, baseline floor is 25
        result = _patched_advice("TestCase", 0.5, "Sticker Capsule", [])
        assert result["baseline"] >= 25.0


# ─── _percentile (additional edge cases) ─────────────────────────────────────


class TestPercentileEdgeCases:
    def test_all_same_values(self) -> None:
        vals = [3.0] * 10
        assert _percentile(sorted(vals), 50) == pytest.approx(3.0)

    def test_two_element_p50(self) -> None:
        assert _percentile([1.0, 2.0], 50) == pytest.approx(1.5)
