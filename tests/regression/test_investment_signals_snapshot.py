"""
Regression Guard — Golden Master Snapshot Tests (Critical Path: Investment Signals).

Philosophy
----------
These tests fix the *current* output of compute_investment_signal() and
compute_all_investment_signals() as the authoritative reference ("golden master").
Any future change to the formulas in investment.py MUST either:
  (a) produce identical output on all golden-master scenarios, OR
  (b) come with a deliberate snapshot update + a review comment explaining why.

HOW TO UPDATE SNAPSHOTS:
  If you intentionally change the model, update the expected dicts in
  _GOLDEN_SIGNALS below and leave a comment with the date and reason.

Etalon dataset
--------------
Based on real Steam Market scenarios (KZT prices, April 2026 reference):
  - Prisma 2 Case: cheap weapon case → BUY territory
  - Revolution Case: expensive weapon case → SELL territory
  - Snakebite Case: fair price, stable → HOLD
  - Gamma Case: falling price momentum → LEAN BUY / BUY
  - Falchion Case: rising price spike → LEAN SELL / SELL
  - Sticker Capsule (capsule type): uses base_cost as baseline (no key)
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

# ── Constants mirroring investment.py (must stay in sync) ─────────────────────
_KEY_PRICE = 481.0       # settings.key_price — standard KZT value
_RATIO_FLOOR = 50.0      # settings.ratio_floor — skip ratio signal below this
_LIQ_MIN = 5.0           # settings.liquidity_min_volume
_FEE_DIV = 1.15
_FEE_FIXED = 5.0
_MOMENTUM_EVENT_THR = 12.0

# ── Etalon dataset ─────────────────────────────────────────────────────────────

# Each entry: (container_name, base_cost, container_type, current_price, mean_price, quantity)
_ETALON: list[tuple] = [
    # Weapon case, current << baseline  → ratio BUY + neutral momentum = BUY
    ("Prisma 2 Case",     1445.0, "Weapon Case", 900.0,  950.0,  120),
    # Weapon case, current >> baseline  → ratio SELL + rising momentum = SELL
    ("Revolution Case",   1445.0, "Weapon Case", 2500.0, 2000.0, 80),
    # Weapon case, fair price, stable   → no ratio signal, no momentum = HOLD
    ("Snakebite Case",    1445.0, "Weapon Case", 1000.0, 1010.0, 60),
    # Weapon case, falling price        → momentum BUY
    ("Gamma Case",        1445.0, "Weapon Case", 800.0,  920.0,  45),
    # Weapon case, spiking price        → momentum SELL
    ("Falchion Case",     1445.0, "Weapon Case", 1300.0, 1000.0, 90),
    # Sticker capsule, no key deducted  → baseline = base_cost
    ("2020 RMR Legends",  480.0,  "Sticker Capsule", 300.0, 310.0, 30),
    # Missing price data                → NO DATA
    ("Unknown Container", 1445.0, "Weapon Case", None,   None,   0),
]

# ── Golden master snapshots (computed from current model, April 2026) ─────────
# Format: {field: expected_value}  — None means "any value" (not checked)
_GOLDEN_SIGNALS: dict[str, dict] = {
    "Prisma 2 Case": {
        # ratio = 900/max(1445-481, 25)=900/964=0.933 → neutral (> 0.85)
        # momentum = (900-950)/950*100=-5.26% → momentum BUY
        "verdict": "LEAN BUY",
        "score": 1,
    },
    "Revolution Case": {
        "verdict": "SELL",
        "score": -2,
    },
    "Snakebite Case": {
        "verdict": "HOLD",
        "score": 0,
    },
    "Gamma Case": {
        # momentum = (800-920)/920*100 = -13.0% < -5% → momentum BUY
        # ratio = 800 / max(1445-481, 25) = 800/964 = 0.83 < 0.85 → ratio BUY
        "verdict": "BUY",
        "score": 2,
    },
    "Falchion Case": {
        # momentum = (1300-1000)/1000*100 = 30% > 8% → momentum SELL
        # ratio = 1300/964 = 1.35 > 1.20 → ratio SELL
        "verdict": "SELL",
        "score": -2,
    },
    "2020 RMR Legends": {
        # capsule: baseline = max(480, 25) = 480
        # ratio = 300/480 = 0.625 < 0.85 → ratio BUY
        # momentum = (300-310)/310*100 = -3.2% → neutral
        "verdict": "LEAN BUY",
        "score": 1,
    },
    "Unknown Container": {
        "verdict": "NO DATA",
        "score": 0,
    },
}

# ── Helpers ────────────────────────────────────────────────────────────────────

def _settings_patch():
    """Patch settings to use deterministic test constants."""
    mock = type("S", (), {
        "key_price": _KEY_PRICE,
        "ratio_floor": _RATIO_FLOOR,
        "liquidity_min_volume": _LIQ_MIN,
        "steam_fee_divisor": _FEE_DIV,
        "steam_fee_fixed": _FEE_FIXED,
        "momentum_event_threshold": _MOMENTUM_EVENT_THR,
    })()
    return patch("src.domain.investment.settings", mock)


# ── Snapshot tests ─────────────────────────────────────────────────────────────

class TestGoldenMasterSignals:
    """Golden Master: every etalon scenario must produce the expected verdict and score."""

    @pytest.mark.parametrize("name,base_cost,ctype,current,mean,qty", _ETALON)
    def test_verdict_matches_snapshot(self, name, base_cost, ctype, current, mean, qty):
        from src.domain.investment import compute_investment_signal

        with _settings_patch():
            result = compute_investment_signal(
                container_name=name,
                base_cost=base_cost,
                container_type=ctype,
                current_price=current,
                mean_price=mean,
                quantity=qty,
            )

        expected = _GOLDEN_SIGNALS[name]
        assert result["verdict"] == expected["verdict"], (
            f"{name}: expected verdict={expected['verdict']!r} "
            f"but got {result['verdict']!r}\n"
            f"Full result: {result}"
        )

    @pytest.mark.parametrize("name,base_cost,ctype,current,mean,qty", _ETALON)
    def test_score_matches_snapshot(self, name, base_cost, ctype, current, mean, qty):
        from src.domain.investment import compute_investment_signal

        with _settings_patch():
            result = compute_investment_signal(
                container_name=name,
                base_cost=base_cost,
                container_type=ctype,
                current_price=current,
                mean_price=mean,
                quantity=qty,
            )

        expected = _GOLDEN_SIGNALS[name]
        assert result["score"] == expected["score"], (
            f"{name}: expected score={expected['score']} "
            f"but got {result['score']}\n"
            f"Full result: {result}"
        )


class TestGoldenMasterOutputShape:
    """Every result must have the required keys with correct types."""

    _REQUIRED_KEYS = {
        "verdict", "current_price", "baseline_price",
        "price_ratio_pct", "momentum_pct", "quantity", "score",
    }

    @pytest.mark.parametrize("name,base_cost,ctype,current,mean,qty", _ETALON)
    def test_output_has_all_required_keys(self, name, base_cost, ctype, current, mean, qty):
        from src.domain.investment import compute_investment_signal

        with _settings_patch():
            result = compute_investment_signal(
                container_name=name,
                base_cost=base_cost,
                container_type=ctype,
                current_price=current,
                mean_price=mean,
                quantity=qty,
            )

        missing = self._REQUIRED_KEYS - set(result.keys())
        assert not missing, f"{name}: missing keys {missing}"

    @pytest.mark.parametrize("name,base_cost,ctype,current,mean,qty", _ETALON)
    def test_numeric_fields_are_floats_or_none(self, name, base_cost, ctype, current, mean, qty):
        from src.domain.investment import compute_investment_signal

        with _settings_patch():
            result = compute_investment_signal(
                container_name=name,
                base_cost=base_cost,
                container_type=ctype,
                current_price=current,
                mean_price=mean,
                quantity=qty,
            )

        for field in ("current_price", "baseline_price", "price_ratio_pct", "momentum_pct"):
            val = result[field]
            assert val is None or isinstance(val, float), (
                f"{name}.{field}: expected float|None, got {type(val).__name__}"
            )


class TestGoldenMasterRatioCalculation:
    """Spot-check ratio and momentum arithmetic against hand-computed values."""

    def test_weapon_case_baseline_subtracts_key_price(self):
        from src.domain.investment import compute_investment_signal

        with _settings_patch():
            result = compute_investment_signal(
                container_name="Test Case",
                base_cost=1445.0,
                container_type="Weapon Case",
                current_price=964.0,   # exactly baseline
                mean_price=964.0,
            )

        # baseline = max(1445 - 481, 25) = 964
        # ratio = 964/964 = 1.0 → ratio_pct = 0.0
        assert result["baseline_price"] == pytest.approx(964.0)
        assert result["price_ratio_pct"] == pytest.approx(0.0, abs=0.5)

    def test_capsule_baseline_does_not_subtract_key(self):
        from src.domain.investment import compute_investment_signal

        with _settings_patch():
            result = compute_investment_signal(
                container_name="Test Capsule",
                base_cost=480.0,
                container_type="Sticker Capsule",
                current_price=480.0,
                mean_price=480.0,
            )

        # baseline = max(480, 25) = 480  (no key deduction)
        assert result["baseline_price"] == pytest.approx(480.0)
        assert result["price_ratio_pct"] == pytest.approx(0.0, abs=0.5)

    def test_momentum_formula_correct(self):
        """momentum_pct = (current - mean) / mean * 100"""
        from src.domain.investment import compute_investment_signal

        with _settings_patch():
            result = compute_investment_signal(
                container_name="Momentum Test",
                base_cost=1445.0,
                container_type="Weapon Case",
                current_price=1100.0,
                mean_price=1000.0,
            )

        # (1100-1000)/1000 * 100 = 10.0
        assert result["momentum_pct"] == pytest.approx(10.0, abs=0.2)

    def test_ratio_floor_suppresses_ratio_signal(self):
        """Containers below ratio_floor must not generate a ratio signal."""
        from src.domain.investment import compute_investment_signal

        with _settings_patch():
            # current = 40 < ratio_floor=50 → ratio signal skipped
            # momentum: (40-35)/35*100 = 14.3% > 8% → sell point only
            result = compute_investment_signal(
                container_name="Cheap Capsule",
                base_cost=100.0,
                container_type="Weapon Case",
                current_price=40.0,
                mean_price=35.0,
            )

        # Only momentum sell (-1), no ratio signal → LEAN SELL
        assert result["score"] == -1
        assert result["verdict"] == "LEAN SELL"


class TestGoldenMasterComputeAll:
    """compute_all_investment_signals produces correct verdicts for a batch."""

    def _make_container(self, cid, name, base_cost, ctype_str):
        from unittest.mock import MagicMock

        from src.domain.models import ContainerType
        c = MagicMock()
        c.container_id = cid
        c.container_name = name
        c.base_cost = base_cost
        # Map string to enum
        _map = {
            "Weapon Case": ContainerType.Weapon_Case,
            "Sticker Capsule": ContainerType.Sticker_Capsule,
        }
        c.container_type = _map.get(ctype_str, ContainerType.Weapon_Case)
        return c

    def test_all_verdicts_present_in_result(self):
        from src.domain.investment import compute_all_investment_signals

        containers = [
            self._make_container("c1", "Prisma 2 Case", 1445.0, "Weapon Case"),
            self._make_container("c2", "Revolution Case", 1445.0, "Weapon Case"),
        ]
        price_data = {
            "Prisma 2 Case":  {"current_price": 900.0, "mean_price": 950.0,  "quantity": 120},
            "Revolution Case": {"current_price": 2500.0, "mean_price": 2000.0, "quantity": 80},
        }

        mock_settings = type("S", (), {
            "key_price": _KEY_PRICE, "ratio_floor": _RATIO_FLOOR,
            "liquidity_min_volume": _LIQ_MIN, "steam_fee_divisor": _FEE_DIV,
            "steam_fee_fixed": _FEE_FIXED, "momentum_event_threshold": _MOMENTUM_EVENT_THR,
        })()

        with patch("src.domain.investment.settings", mock_settings), \
             patch("infra.signal_handler.notify_liquidity_warning"):
            result = compute_all_investment_signals(containers, price_data)

        assert result["c1"]["verdict"] == "LEAN BUY"
        assert result["c2"]["verdict"] == "SELL"

    def test_sell_verdict_annotated_with_sell_at_loss(self):
        from src.domain.investment import compute_all_investment_signals

        containers = [
            self._make_container("c1", "Revolution Case", 1445.0, "Weapon Case"),
        ]
        price_data = {
            "Revolution Case": {"current_price": 2500.0, "mean_price": 2000.0, "quantity": 80},
        }

        mock_settings = type("S", (), {
            "key_price": _KEY_PRICE, "ratio_floor": _RATIO_FLOOR,
            "liquidity_min_volume": _LIQ_MIN, "steam_fee_divisor": _FEE_DIV,
            "steam_fee_fixed": _FEE_FIXED, "momentum_event_threshold": _MOMENTUM_EVENT_THR,
        })()

        with patch("src.domain.investment.settings", mock_settings), \
             patch("infra.signal_handler.notify_liquidity_warning"):
            result = compute_all_investment_signals(containers, price_data)

        assert "sell_at_loss" in result["c1"]
        # net = 2500/1.15 - 5 = 2169.6 > 1445 → not at loss
        assert result["c1"]["sell_at_loss"] is False

    def test_missing_price_returns_no_data(self):
        from src.domain.investment import compute_all_investment_signals

        containers = [
            self._make_container("c1", "Orphan Case", 1445.0, "Weapon Case"),
        ]
        price_data = {}  # no price entry

        mock_settings = type("S", (), {
            "key_price": _KEY_PRICE, "ratio_floor": _RATIO_FLOOR,
            "liquidity_min_volume": _LIQ_MIN, "steam_fee_divisor": _FEE_DIV,
            "steam_fee_fixed": _FEE_FIXED, "momentum_event_threshold": _MOMENTUM_EVENT_THR,
        })()

        with patch("src.domain.investment.settings", mock_settings), \
             patch("infra.signal_handler.notify_liquidity_warning"):
            result = compute_all_investment_signals(containers, price_data)

        assert result["c1"]["verdict"] == "NO DATA"
