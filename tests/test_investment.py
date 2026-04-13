"""
Tests for engine/investment.py — compute_investment_signal() and compute_all_investment_signals().

Covers:
  - NO DATA verdict when no price
  - Ratio-based buy/sell points
  - Momentum-based buy/sell points
  - All 5 verdict outcomes (BUY, LEAN BUY, HOLD, LEAN SELL, SELL)
  - Weapon case vs capsule baseline calculation
  - compute_all_investment_signals() aggregation

All price values are in KZT. Reference rate: ~481₸/$.
"""

from __future__ import annotations

from engine.investment import compute_all_investment_signals, compute_investment_signal

# ── Helpers ───────────────────────────────────────────────────────────────────

_CASE_NAME = "Revolution Case"
_CASE_TYPE = "Weapon Case"
# key_price_kzt=1200; base_cost_kzt=1680 (≈$3.49) → baseline = 1680−1200 = 480₸
_BASE_COST = 1680

_CAPSULE_NAME = "Paris 2023 Legends Sticker Capsule"
_CAPSULE_TYPE = "Sticker Capsule"
# No key needed → baseline = base_cost_kzt = 2400₸ (≈$5.00)
_CAP_BASE = 2400


# ── NO DATA ───────────────────────────────────────────────────────────────────


class TestNoData:
    def test_none_price_returns_no_data(self) -> None:
        sig = compute_investment_signal(_CASE_NAME, _BASE_COST, _CASE_TYPE, None)
        assert sig["verdict"] == "NO DATA"

    def test_zero_price_returns_no_data(self) -> None:
        sig = compute_investment_signal(_CASE_NAME, _BASE_COST, _CASE_TYPE, 0.0)
        assert sig["verdict"] == "NO DATA"

    def test_no_data_has_expected_keys(self) -> None:
        sig = compute_investment_signal(_CASE_NAME, _BASE_COST, _CASE_TYPE, None)
        assert set(sig.keys()) == {
            "verdict",
            "current_price",
            "baseline_price",
            "price_ratio_pct",
            "momentum_pct",
            "quantity",
            "score",
        }

    def test_no_data_current_price_is_none(self) -> None:
        sig = compute_investment_signal(_CASE_NAME, _BASE_COST, _CASE_TYPE, None)
        assert sig["current_price"] is None
        assert sig["score"] == 0


# ── Baseline: weapon case vs capsule ─────────────────────────────────────────


class TestBaseline:
    def test_weapon_case_subtracts_key(self) -> None:
        # baseline = base_cost_kzt − key_price_kzt = 1680 − 1200 = 480₸
        sig = compute_investment_signal(_CASE_NAME, _BASE_COST, _CASE_TYPE, 480)
        assert abs(sig["baseline_price"] - 480) < 1

    def test_capsule_uses_full_base_cost(self) -> None:
        sig = compute_investment_signal(_CAPSULE_NAME, _CAP_BASE, _CAPSULE_TYPE, 2400)
        assert abs(sig["baseline_price"] - _CAP_BASE) < 1

    def test_baseline_minimum_is_25_kzt(self) -> None:
        # base_cost = 960 → baseline = 960 − 1200 = −240 → clamped to 25₸
        sig = compute_investment_signal(_CASE_NAME, 960, _CASE_TYPE, 480)
        assert sig["baseline_price"] == 25.0


# ── Ratio buy/sell points ─────────────────────────────────────────────────────


class TestRatioPoints:
    def test_cheap_price_gives_buy_point(self) -> None:
        # ratio < 0.85: current = 360₸, baseline = 480₸ → ratio = 0.75 < 0.85
        sig = compute_investment_signal(_CASE_NAME, _BASE_COST, _CASE_TYPE, 360, mean_price=360)
        assert sig["score"] >= 1

    def test_expensive_price_gives_sell_point(self) -> None:
        # ratio > 1.20: current = 700₸, baseline = 480₸ → ratio = 1.46 > 1.20
        sig = compute_investment_signal(_CASE_NAME, _BASE_COST, _CASE_TYPE, 700, mean_price=700)
        assert sig["score"] <= -1

    def test_fair_price_no_ratio_point(self) -> None:
        # ratio = 1.00: current == baseline, no ratio point
        baseline = max(_BASE_COST - 1200, 25.0)  # 480
        sig = compute_investment_signal(
            _CASE_NAME, _BASE_COST, _CASE_TYPE, baseline, mean_price=baseline
        )
        assert sig["score"] == 0


# ── Momentum buy/sell points ──────────────────────────────────────────────────


class TestMomentumPoints:
    def test_falling_momentum_gives_buy_point(self) -> None:
        # momentum < -5%: current well below mean
        sig = compute_investment_signal(
            _CASE_NAME,
            _BASE_COST,
            _CASE_TYPE,
            current_price=480,
            mean_price=580,  # -17% momentum
        )
        assert sig["momentum_pct"] < -5.0

    def test_rising_momentum_gives_sell_point(self) -> None:
        # momentum > 8%: current above mean
        sig = compute_investment_signal(
            _CASE_NAME,
            _BASE_COST,
            _CASE_TYPE,
            current_price=580,
            mean_price=480,  # +20.8% momentum
        )
        assert sig["momentum_pct"] > 8.0

    def test_no_mean_gives_zero_momentum(self) -> None:
        sig = compute_investment_signal(_CASE_NAME, _BASE_COST, _CASE_TYPE, 480)
        assert sig["momentum_pct"] == 0.0


# ── All 5 verdict transitions ─────────────────────────────────────────────────


class TestVerdicts:
    def test_buy_two_buy_points(self) -> None:
        # cheap (ratio < 0.85) + falling (momentum < -5%) → BUY
        # current=360 < ratio_floor_kzt=120? No, 360 > 120 → ratio fires
        # baseline=480; ratio=360/480=0.75 < 0.85 → buy point
        # mean=440; momentum=(360-440)/440=-18% < -5% → buy point
        sig = compute_investment_signal(
            _CASE_NAME,
            _BASE_COST,
            _CASE_TYPE,
            current_price=360,
            mean_price=440,
        )
        assert sig["verdict"] == "BUY"
        assert sig["score"] == 2

    def test_lean_buy_one_buy_point(self) -> None:
        # only cheap (ratio < 0.85), neutral momentum → LEAN BUY
        sig = compute_investment_signal(
            _CASE_NAME,
            _BASE_COST,
            _CASE_TYPE,
            current_price=360,
            mean_price=360,  # cheap, no momentum signal
        )
        assert sig["verdict"] == "LEAN BUY"
        assert sig["score"] == 1

    def test_hold_zero_points(self) -> None:
        # fair ratio, neutral momentum → HOLD
        baseline = max(_BASE_COST - 1200, 25.0)  # 480
        sig = compute_investment_signal(
            _CASE_NAME,
            _BASE_COST,
            _CASE_TYPE,
            current_price=baseline,
            mean_price=baseline,
        )
        assert sig["verdict"] == "HOLD"
        assert sig["score"] == 0

    def test_lean_sell_one_sell_point(self) -> None:
        # only expensive (ratio > 1.20), neutral momentum → LEAN SELL
        sig = compute_investment_signal(
            _CASE_NAME,
            _BASE_COST,
            _CASE_TYPE,
            current_price=700,
            mean_price=700,  # expensive, no momentum
        )
        assert sig["verdict"] == "LEAN SELL"
        assert sig["score"] == -1

    def test_sell_two_sell_points(self) -> None:
        # expensive (ratio > 1.20) + rising (momentum > 8%) → SELL
        sig = compute_investment_signal(
            _CASE_NAME,
            _BASE_COST,
            _CASE_TYPE,
            current_price=700,
            mean_price=580,  # expensive + rising +20.7%
        )
        assert sig["verdict"] == "SELL"
        assert sig["score"] == -2


# ── Output fields ─────────────────────────────────────────────────────────────


class TestOutputFields:
    def test_all_keys_present(self) -> None:
        sig = compute_investment_signal(_CASE_NAME, _BASE_COST, _CASE_TYPE, 480)
        assert set(sig.keys()) == {
            "verdict",
            "current_price",
            "baseline_price",
            "price_ratio_pct",
            "momentum_pct",
            "quantity",
            "score",
        }

    def test_quantity_passed_through(self) -> None:
        sig = compute_investment_signal(_CASE_NAME, _BASE_COST, _CASE_TYPE, 480, quantity=42)
        assert sig["quantity"] == 42

    def test_current_price_rounded(self) -> None:
        sig = compute_investment_signal(_CASE_NAME, _BASE_COST, _CASE_TYPE, 480.456789)
        assert sig["current_price"] == round(480.456789, 2)


# ── compute_all_investment_signals ────────────────────────────────────────────


class _FakeContainer:
    def __init__(self, cid: str, name: str, base_cost: float, ctype: str) -> None:
        self.container_id = cid
        self.container_name = name
        self.base_cost = base_cost

        class _Type:
            value = ctype

        self.container_type = _Type()


class TestComputeAllSignals:
    def test_returns_dict_keyed_by_container_id(self) -> None:
        containers = [
            _FakeContainer("id-1", "Revolution Case", 1680, "Weapon Case"),
            _FakeContainer("id-2", "Paris Capsule", 2400, "Sticker Capsule"),
        ]
        price_data = {
            "Revolution Case": {"current_price": 480, "mean_price": 480, "quantity": 10},
            "Paris Capsule": {"current_price": 1920, "mean_price": 1920, "quantity": 5},
        }
        result = compute_all_investment_signals(containers, price_data)
        assert set(result.keys()) == {"id-1", "id-2"}

    def test_missing_price_gives_no_data(self) -> None:
        containers = [_FakeContainer("id-1", "Mystery Case", 1680, "Weapon Case")]
        result = compute_all_investment_signals(containers, {})
        assert result["id-1"]["verdict"] == "NO DATA"

    def test_each_signal_has_verdict(self) -> None:
        containers = [_FakeContainer("id-1", "Revolution Case", 1680, "Weapon Case")]
        price_data = {"Revolution Case": {"current_price": 480, "mean_price": 480, "quantity": 0}}
        result = compute_all_investment_signals(containers, price_data)
        assert "verdict" in result["id-1"]

    def test_empty_containers_returns_empty(self) -> None:
        assert compute_all_investment_signals([], {}) == {}


# ── sell_at_loss annotation ───────────────────────────────────────────────────


class TestSellAtLoss:
    """compute_all_investment_signals() must annotate sell_at_loss on SELL/LEAN SELL signals."""

    def test_sell_at_loss_true_when_net_proceeds_below_cost_basis(self) -> None:
        # base_cost_kzt = 4800₸; baseline = 4800 (capsule, no key)
        # current = 4600₸ → ratio = 4600/4800 = 0.958 (HOLD territory alone)
        # Use a weapon case: base_cost=4810, baseline=4810-1200=3610
        # current=5000 → ratio=5000/3610=1.385 > 1.20 → sell point
        # mean=3500 → momentum=(5000-3500)/3500=+42.9% > 8% → sell point → SELL
        # net = 5000/1.15 − 5 = 4348 − 5 = 4343₸ < base_cost=4810₸ → sell_at_loss=True
        containers = [_FakeContainer("id-1", "Old Case", 4810, "Weapon Case")]
        price_data = {"Old Case": {"current_price": 5000, "mean_price": 3500, "quantity": 5}}
        result = compute_all_investment_signals(containers, price_data)
        sig = result["id-1"]
        assert sig["verdict"] == "SELL"
        assert sig.get("sell_at_loss") is True

    def test_sell_at_loss_false_when_net_proceeds_above_cost_basis(self) -> None:
        # base_cost_kzt = 960₸ (capsule); baseline = 960
        # current = 1500₸ → ratio = 1500/960 = 1.56 > 1.20 → sell point
        # mean = 1100₸ → momentum = +36% → sell point → SELL
        # net = 1500/1.15 − 5 = 1304 − 5 = 1299₸ > 960₸ → sell_at_loss=False
        containers = [_FakeContainer("id-1", "Cheap Capsule", 960, "Sticker Capsule")]
        price_data = {"Cheap Capsule": {"current_price": 1500, "mean_price": 1100, "quantity": 5}}
        result = compute_all_investment_signals(containers, price_data)
        sig = result["id-1"]
        assert sig["verdict"] == "SELL"
        assert sig.get("sell_at_loss") is False

    def test_sell_at_loss_not_present_on_buy_signal(self) -> None:
        # BUY signal should have no sell_at_loss key
        containers = [_FakeContainer("id-1", "Bargain Case", 1680, "Weapon Case")]
        # baseline=480; current=360 → ratio=0.75 < 0.85; mean=440 → momentum=-18% → BUY
        price_data = {"Bargain Case": {"current_price": 360, "mean_price": 440, "quantity": 50}}
        result = compute_all_investment_signals(containers, price_data)
        sig = result["id-1"]
        assert sig["verdict"] == "BUY"
        assert "sell_at_loss" not in sig

    def test_sell_at_loss_not_present_on_hold_signal(self) -> None:
        containers = [_FakeContainer("id-1", "Stable Case", 1680, "Weapon Case")]
        baseline = max(1680 - 1200, 25.0)  # = 480
        price_data = {
            "Stable Case": {"current_price": baseline, "mean_price": baseline, "quantity": 5}
        }
        result = compute_all_investment_signals(containers, price_data)
        sig = result["id-1"]
        assert sig["verdict"] == "HOLD"
        assert "sell_at_loss" not in sig


# ── S13-MINOR-2: Off-by-one boundary tests for signal thresholds ──────────────


class TestThresholdBoundaries:
    """
    Verify exact boundary behaviour for the 4 signal thresholds.
    These guard against accidental < vs <= changes in the comparison operators.

    All prices in KZT. Capsule baseline = base_cost_kzt = 2000₸ (well above ratio_floor_kzt=120).
    """

    # Capsule baseline = base_cost directly; use 2000₸ for easy arithmetic.
    _BASE = 2000.0
    _TYPE = "Sticker Capsule"
    _NAME = "TestCapsule"

    def test_ratio_exactly_0_85_is_buy_point(self) -> None:
        # ratio < 0.85 → buy; ratio == 0.85 → NOT a buy (strict <)
        price_at_085 = self._BASE * 0.85  # exactly on threshold → no buy point
        sig = compute_investment_signal(
            self._NAME, self._BASE, self._TYPE, price_at_085, mean_price=price_at_085
        )
        assert sig["score"] >= 0  # no buy point from ratio alone at exact boundary

    def test_ratio_just_below_0_85_is_buy_point(self) -> None:
        price = self._BASE * 0.849  # just below 0.85
        sig = compute_investment_signal(self._NAME, self._BASE, self._TYPE, price, mean_price=price)
        assert sig["score"] >= 1  # ratio buy point triggered

    def test_ratio_exactly_1_20_is_no_sell_point(self) -> None:
        # ratio > 1.20 → sell; ratio == 1.20 → NOT a sell (strict >)
        price_at_120 = self._BASE * 1.20
        sig = compute_investment_signal(
            self._NAME, self._BASE, self._TYPE, price_at_120, mean_price=price_at_120
        )
        assert sig["score"] <= 0  # no sell point at exact boundary

    def test_ratio_just_above_1_20_is_sell_point(self) -> None:
        price = self._BASE * 1.201
        sig = compute_investment_signal(self._NAME, self._BASE, self._TYPE, price, mean_price=price)
        assert sig["score"] <= -1  # ratio sell point triggered

    def test_momentum_exactly_minus_5_is_no_buy_point(self) -> None:
        # momentum < -5% → buy; momentum == -5% → NOT a buy (strict <)
        # mean=2000, current=1900 → momentum = -5.0% exactly
        sig = compute_investment_signal(
            self._NAME, self._BASE, self._TYPE, current_price=1900, mean_price=2000
        )
        # ratio 1900/2000 = 0.95 → no ratio point; momentum = -5.0% → no buy point
        assert sig["score"] == 0

    def test_momentum_just_below_minus_5_is_buy_point(self) -> None:
        # current=1898 → momentum = (1898-2000)/2000 = -5.1%
        sig = compute_investment_signal(
            self._NAME, self._BASE, self._TYPE, current_price=1898, mean_price=2000
        )
        assert sig["score"] >= 1

    def test_momentum_exactly_plus_8_is_no_sell_point(self) -> None:
        # momentum > 8% → sell; momentum == 8% → NOT a sell (strict >)
        # mean=2000, current=2160 → momentum = 8.0% exactly
        # ratio=2160/2000=1.08 < 1.20 → no ratio point
        sig = compute_investment_signal(
            self._NAME, self._BASE, self._TYPE, current_price=2160, mean_price=2000
        )
        # ratio=1.08 → no ratio sell; momentum=8.0% exactly → NOT > threshold → no sell
        assert sig["score"] == 0

    def test_momentum_just_above_plus_8_is_sell_point_contributing(self) -> None:
        # current=2180: momentum=(2180-2000)/2000*100 = 9% > 8% → sell point
        # ratio=2180/2000=1.09 < 1.20 → no ratio point
        sig = compute_investment_signal(
            self._NAME, self._BASE, self._TYPE, current_price=2180, mean_price=2000
        )
        assert sig["score"] == -1  # only momentum sell point triggered


# ── T13-T3-1: Ratio floor for cheap containers ────────────────────────────────


class TestRatioFloor:
    """T13-T3-1: ratio signal skipped when current_price < ratio_floor_kzt (default 120₸)."""

    def test_cheap_case_below_floor_returns_hold_despite_high_ratio(self) -> None:
        # current = 80₸ < floor 120₸; baseline = max(1220-1200, 25) = 25₸
        # ratio = 80/25 = 3.2 > 1.20 — would be LEAN SELL without floor
        # With floor: ratio skipped → score=0 → HOLD (assuming no momentum signal)
        sig = compute_investment_signal("Cheap Case", 1220, "Weapon Case", 80, mean_price=80)
        assert sig["verdict"] == "HOLD"
        assert sig["score"] == 0

    def test_case_above_floor_still_triggers_ratio_sell(self) -> None:
        # current = 200₸ > floor 120₸; baseline = max(1220-1200, 25) = 25₸
        # ratio = 200/25 = 8.0 > 1.20 → sell point fires
        sig = compute_investment_signal("Normal Case", 1220, "Weapon Case", 200, mean_price=200)
        assert sig["score"] <= -1  # ratio sell point fires above floor

    def test_cheap_case_with_falling_momentum_still_gets_buy_point(self) -> None:
        # current = 80₸ < floor 120₸; ratio skipped
        # momentum = (80-140)/140 = -42.9% < -5% → buy point
        sig = compute_investment_signal("Cheap Case", 1220, "Weapon Case", 80, mean_price=140)
        assert sig["momentum_pct"] < -5.0
        assert sig["verdict"] == "LEAN BUY"
        assert sig["score"] == 1


# ── T13-T3-3: Event momentum threshold ───────────────────────────────────────


class TestEventMomentumThreshold:
    """T13-T3-3: event-matched containers use momentum_event_threshold (default +15%) not +8%."""

    # Capsule baseline = 2000₸ (well above ratio_floor 120₸)
    _BASE = 2000.0
    _TYPE = "Sticker Capsule"
    _NAME = "IEM Katowice 2026 Capsule"

    def test_momentum_10pct_no_sell_when_event_matched(self) -> None:
        # momentum = (2200-2000)/2000*100 = 10% > 8% (standard) but < 15% (event)
        # ratio = 2200/2000 = 1.10 < 1.20 → no ratio point
        # With is_event_matched=True: no sell point → HOLD
        sig = compute_investment_signal(
            self._NAME,
            self._BASE,
            self._TYPE,
            current_price=2200,
            mean_price=2000,
            is_event_matched=True,
        )
        assert sig["score"] == 0
        assert sig["verdict"] == "HOLD"

    def test_momentum_10pct_gives_sell_point_without_event_match(self) -> None:
        # Same values, no event match → standard +8% threshold → sell point
        sig = compute_investment_signal(
            self._NAME,
            self._BASE,
            self._TYPE,
            current_price=2200,
            mean_price=2000,
            is_event_matched=False,
        )
        assert sig["score"] <= -1

    def test_momentum_16pct_gives_sell_point_when_event_matched(self) -> None:
        # momentum = (2320-2000)/2000*100 = 16% > 15% event threshold → sell point
        # ratio = 2320/2000 = 1.16 < 1.20 → no ratio point
        sig = compute_investment_signal(
            self._NAME,
            self._BASE,
            self._TYPE,
            current_price=2320,
            mean_price=2000,
            is_event_matched=True,
        )
        assert sig["score"] <= -1


# ── T13-T3-2: sell_at_loss uses actual buy_price_kzt from positions ───────────


class TestSellAtLossWithPositionBuyPrice:
    """T13-T3-2: compute_all_investment_signals() uses positions_buy_price for sell_at_loss."""

    def test_sell_at_loss_true_when_position_buy_price_above_net_proceeds(self) -> None:
        # Capsule base_cost_kzt = 960₸; baseline = 960₸
        # User actually bought at 3840₸ (positions_buy_price)
        # current = 1500₸ → ratio = 1500/960 = 1.56 > 1.20 → sell point
        # momentum = (1500-1100)/1100 = +36% > 8% → sell point → SELL
        # net_proceeds = 1500/1.15 − 5 ≈ 1299₸
        # vs base_cost_kzt (960₸): 1299 > 960 → sell_at_loss=False (MSRP)
        # vs buy_price_kzt (3840₸): 1299 < 3840 → sell_at_loss=True (actual cost)
        containers = [_FakeContainer("id-1", "Cheap Capsule", 960, "Sticker Capsule")]
        price_data = {"Cheap Capsule": {"current_price": 1500, "mean_price": 1100, "quantity": 5}}
        result = compute_all_investment_signals(
            containers, price_data, positions_buy_price={"Cheap Capsule": 3840}
        )
        assert result["id-1"]["verdict"] == "SELL"
        assert result["id-1"]["sell_at_loss"] is True

    def test_sell_at_loss_falls_back_to_base_cost_when_no_positions(self) -> None:
        # Same container, no positions_buy_price → uses base_cost_kzt=960₸
        # net_proceeds≈1299₸ > 960₸ → not at loss
        containers = [_FakeContainer("id-1", "Cheap Capsule", 960, "Sticker Capsule")]
        price_data = {"Cheap Capsule": {"current_price": 1500, "mean_price": 1100, "quantity": 5}}
        result = compute_all_investment_signals(containers, price_data)
        assert result["id-1"]["verdict"] == "SELL"
        assert result["id-1"]["sell_at_loss"] is False

    def test_sell_at_loss_false_when_position_buy_price_below_net_proceeds(self) -> None:
        # User bought cheap at 480₸; net_proceeds≈1299₸ > 480₸ → not at loss
        containers = [_FakeContainer("id-1", "Cheap Capsule", 960, "Sticker Capsule")]
        price_data = {"Cheap Capsule": {"current_price": 1500, "mean_price": 1100, "quantity": 5}}
        result = compute_all_investment_signals(
            containers, price_data, positions_buy_price={"Cheap Capsule": 480}
        )
        assert result["id-1"]["sell_at_loss"] is False
