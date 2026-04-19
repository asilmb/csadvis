"""
Regression Guard — Property-Based Tests (Hypothesis).

Invariants that must hold for ALL valid inputs, regardless of specific values:

  Investment signal engine
  ─────────────────────────
  P1: verdict ∈ {"BUY","LEAN BUY","HOLD","LEAN SELL","SELL","NO DATA"}
  P2: score ∈ {−2, −1, 0, 1, 2}
  P3: BUY  ↔ score ≥ 1    (BUY or LEAN BUY only when score > 0)
  P4: SELL ↔ score ≤ −1   (SELL or LEAN SELL only when score < 0)
  P5: HOLD ↔ score = 0
  P6: result is deterministic (same input → same output)
  P7: numeric fields are finite floats or None (no NaN/Inf in output)

  Armory Pass
  ────────────
  P8: recommendation ∈ {"MARKET", "PASS"}
  P9: breakeven > 0 for any valid input
  P10: if net_proceeds > effective_pass_cost → recommendation == "MARKET"
  P11: if net_proceeds ≤ effective_pass_cost → recommendation == "PASS"
  P12: margin_pct sign matches recommendation:
        MARKET → margin_pct ≥ 0 (net > pass_cost)
        PASS   → margin_pct ≤ 0

  ArmoryAdvisor
  ─────────────
  P13: net_proceeds = price * STEAM_NET_MULTIPLIER (always)
  P14: confidence ∈ {"HIGH","MEDIUM","LOW","UNKNOWN"}
  P15: overall_confidence is the minimum across all rewards
  P16: total_roi is None when ANY reward has no price
"""
from __future__ import annotations

import math
from unittest.mock import MagicMock, patch

import pytest
from hypothesis import assume, given
from hypothesis import settings as h_settings
from hypothesis import strategies as st

# ── Shared settings mock ───────────────────────────────────────────────────────

_MOCK_S = type("S", (), {
    "key_price": 481.0,
    "ratio_floor": 50.0,
    "liquidity_min_volume": 5.0,
    "steam_fee_divisor": 1.15,
    "steam_fee_fixed": 5.0,
    "momentum_event_threshold": 12.0,
    "currency_symbol": "₸",
})()

# ── Hypothesis strategies ──────────────────────────────────────────────────────

_pos_price = st.floats(min_value=0.01, max_value=500_000.0, allow_nan=False, allow_infinity=False)
_nullable_price = st.one_of(st.none(), _pos_price)
_base_cost = st.floats(min_value=25.0, max_value=10_000.0, allow_nan=False, allow_infinity=False)
_quantity = st.integers(min_value=0, max_value=100_000)
_container_type = st.sampled_from(["Weapon Case", "Sticker Capsule", "Autograph Capsule", "Event Capsule"])
_pass_cost = st.floats(min_value=0.0, max_value=50_000.0, allow_nan=False, allow_infinity=False)
_stars = st.integers(min_value=1, max_value=100)

# ── P1–P7: Investment signal invariants ───────────────────────────────────────

_VALID_VERDICTS = {"BUY", "LEAN BUY", "HOLD", "LEAN SELL", "SELL", "NO DATA"}


@given(
    current_price=_nullable_price,
    mean_price=_nullable_price,
    base_cost=_base_cost,
    container_type=_container_type,
    quantity=_quantity,
)
@h_settings(max_examples=300, deadline=500)
def test_P1_verdict_always_valid(current_price, mean_price, base_cost, container_type, quantity):
    """P1: verdict is always one of the six defined strings."""
    from src.domain.investment import compute_investment_signal
    with patch("src.domain.investment.settings", _MOCK_S):
        result = compute_investment_signal(
            container_name="HypTest",
            base_cost=base_cost,
            container_type=container_type,
            current_price=current_price,
            mean_price=mean_price,
            quantity=quantity,
        )
    assert result["verdict"] in _VALID_VERDICTS


@given(
    current_price=_nullable_price,
    mean_price=_nullable_price,
    base_cost=_base_cost,
    container_type=_container_type,
)
@h_settings(max_examples=300, deadline=500)
def test_P2_score_always_in_range(current_price, mean_price, base_cost, container_type):
    """P2: score ∈ {−2, −1, 0, 1, 2}."""
    from src.domain.investment import compute_investment_signal
    with patch("src.domain.investment.settings", _MOCK_S):
        result = compute_investment_signal(
            container_name="HypTest",
            base_cost=base_cost,
            container_type=container_type,
            current_price=current_price,
            mean_price=mean_price,
        )
    assert result["score"] in (-2, -1, 0, 1, 2)


@given(
    current_price=_pos_price,
    mean_price=_nullable_price,
    base_cost=_base_cost,
    container_type=_container_type,
)
@h_settings(max_examples=200, deadline=500)
def test_P3_P4_verdict_score_consistency(current_price, mean_price, base_cost, container_type):
    """P3+P4: BUY/LEAN BUY ↔ score>0; SELL/LEAN SELL ↔ score<0; HOLD ↔ score=0."""
    from src.domain.investment import compute_investment_signal
    with patch("src.domain.investment.settings", _MOCK_S):
        result = compute_investment_signal(
            container_name="HypTest",
            base_cost=base_cost,
            container_type=container_type,
            current_price=current_price,
            mean_price=mean_price,
        )

    verdict = result["verdict"]
    score = result["score"]

    if verdict in ("BUY", "LEAN BUY"):
        assert score > 0, f"verdict={verdict} but score={score}"
    elif verdict in ("SELL", "LEAN SELL"):
        assert score < 0, f"verdict={verdict} but score={score}"
    elif verdict == "HOLD":
        assert score == 0, f"verdict=HOLD but score={score}"


@given(
    current_price=_nullable_price,
    mean_price=_nullable_price,
    base_cost=_base_cost,
    container_type=_container_type,
)
@h_settings(max_examples=100, deadline=500)
def test_P6_determinism(current_price, mean_price, base_cost, container_type):
    """P6: same inputs always produce identical outputs."""
    from src.domain.investment import compute_investment_signal
    kwargs = dict(
        container_name="HypTest",
        base_cost=base_cost,
        container_type=container_type,
        current_price=current_price,
        mean_price=mean_price,
    )
    with patch("src.domain.investment.settings", _MOCK_S):
        r1 = compute_investment_signal(**kwargs)
        r2 = compute_investment_signal(**kwargs)
    assert r1 == r2


@given(
    current_price=_pos_price,
    mean_price=_pos_price,
    base_cost=_base_cost,
    container_type=_container_type,
)
@h_settings(max_examples=200, deadline=500)
def test_P7_no_nan_or_inf_in_output(current_price, mean_price, base_cost, container_type):
    """P7: numeric fields in output are always finite or None — never NaN/Inf."""
    from src.domain.investment import compute_investment_signal
    with patch("src.domain.investment.settings", _MOCK_S):
        result = compute_investment_signal(
            container_name="HypTest",
            base_cost=base_cost,
            container_type=container_type,
            current_price=current_price,
            mean_price=mean_price,
        )
    for field in ("current_price", "baseline_price", "price_ratio_pct", "momentum_pct"):
        val = result[field]
        if val is not None:
            assert math.isfinite(val), f"{field}={val} is not finite"


# ── P8–P12: Armory Pass invariants ────────────────────────────────────────────

@given(
    market_price=_pos_price,
    pass_cost=_pass_cost,
    stars_in_pass=_stars,
    stars_per_case=st.integers(min_value=1, max_value=100),
)
@h_settings(max_examples=300, deadline=500)
def test_P8_recommendation_always_valid(market_price, pass_cost, stars_in_pass, stars_per_case):
    """P8: recommendation ∈ {"MARKET", "PASS"}."""
    from src.domain.armory_pass import compare_armory_pass
    assume(stars_per_case <= stars_in_pass)
    with patch("src.domain.armory_pass.settings", _MOCK_S):
        result = compare_armory_pass(
            container_name="Test",
            market_price=market_price,
            pass_cost=pass_cost,
            stars_in_pass=stars_in_pass,
            stars_per_case=stars_per_case,
        )
    assert result.recommendation in ("MARKET", "PASS")


@given(
    market_price=_pos_price,
    pass_cost=st.floats(min_value=1.0, max_value=50_000.0, allow_nan=False, allow_infinity=False),
    stars_in_pass=_stars,
    stars_per_case=st.integers(min_value=1, max_value=100),
)
@h_settings(max_examples=300, deadline=500)
def test_P9_breakeven_always_positive(market_price, pass_cost, stars_in_pass, stars_per_case):
    """P9: breakeven listing price > 0 for all valid inputs."""
    from src.domain.armory_pass import compare_armory_pass
    assume(stars_per_case <= stars_in_pass)
    with patch("src.domain.armory_pass.settings", _MOCK_S):
        result = compare_armory_pass(
            container_name="Test",
            market_price=market_price,
            pass_cost=pass_cost,
            stars_in_pass=stars_in_pass,
            stars_per_case=stars_per_case,
        )
    assert result.breakeven_listing_price > 0


@given(
    market_price=_pos_price,
    pass_cost=_pass_cost,
    stars_in_pass=_stars,
    stars_per_case=st.integers(min_value=1, max_value=100),
)
@h_settings(max_examples=400, deadline=500)
def test_P10_P11_recommendation_matches_net_vs_cost(market_price, pass_cost, stars_in_pass, stars_per_case):
    """P10+P11: recommendation is determined solely by net > effective_cost."""
    from src.domain.armory_pass import compare_armory_pass
    assume(stars_per_case <= stars_in_pass)
    with patch("src.domain.armory_pass.settings", _MOCK_S):
        result = compare_armory_pass(
            container_name="Test",
            market_price=market_price,
            pass_cost=pass_cost,
            stars_in_pass=stars_in_pass,
            stars_per_case=stars_per_case,
        )

    net = float(result.net_market_proceeds.amount)
    eff = float(result.effective_pass_cost.amount)

    if net > eff:
        assert result.recommendation == "MARKET", (
            f"net={net:.2f} > eff={eff:.2f} but got {result.recommendation}"
        )
    else:
        assert result.recommendation == "PASS", (
            f"net={net:.2f} ≤ eff={eff:.2f} but got {result.recommendation}"
        )


# ── P13–P16: ArmoryAdvisor invariants ─────────────────────────────────────────

@given(price=_pos_price)
@h_settings(max_examples=200, deadline=500)
def test_P13_net_proceeds_equals_price_times_multiplier(price):
    """P13: net_proceeds = price * STEAM_NET_MULTIPLIER (within float precision)."""
    from src.domain.analytics.armory_advisor import STEAM_NET_MULTIPLIER, ArmoryAdvisor

    repo = MagicMock()
    snap = MagicMock()
    snap.price = price
    repo.get_latest_price.return_value = snap
    repo.get_price_history.return_value = []

    advisor = ArmoryAdvisor(repo, reward_catalog={"Item": 1})
    result = advisor.get_pass_efficiency(pass_cost=2500.0)

    expected_net = price * STEAM_NET_MULTIPLIER
    actual_net = result.rewards[0].net_proceeds
    assert actual_net == pytest.approx(expected_net, rel=1e-6)


@given(price=_pos_price, n_history=st.integers(min_value=0, max_value=50))
@h_settings(max_examples=200, deadline=500)
def test_P14_confidence_always_valid(price, n_history):
    """P14: confidence ∈ {"HIGH","MEDIUM","LOW","UNKNOWN"}."""
    from src.domain.analytics.armory_advisor import ArmoryAdvisor

    history_prices = [price * (0.9 + 0.2 * (i / max(n_history, 1))) for i in range(n_history)]

    repo = MagicMock()
    snap = MagicMock()
    snap.price = price
    repo.get_latest_price.return_value = snap
    repo.get_price_history.return_value = [{"price": p} for p in history_prices]

    advisor = ArmoryAdvisor(repo, reward_catalog={"Item": 1})
    result = advisor.get_pass_efficiency(pass_cost=2500.0)

    assert result.rewards[0].confidence in ("HIGH", "MEDIUM", "LOW", "UNKNOWN")


@given(
    prices=st.lists(
        st.floats(min_value=0.01, max_value=10_000.0, allow_nan=False, allow_infinity=False),
        min_size=1, max_size=10,
    )
)
@h_settings(max_examples=200, deadline=1000)
def test_P15_overall_confidence_is_minimum(prices):
    """P15: overall_confidence = minimum confidence level across all rewards."""
    from src.domain.analytics.armory_advisor import _CONFIDENCE_RANK, ArmoryAdvisor

    catalog = {f"Item{i}": 1 for i in range(len(prices))}

    repo = MagicMock()

    def _latest(name):
        idx = int(name[4:])
        snap = MagicMock()
        snap.price = prices[idx]
        return snap

    repo.get_latest_price.side_effect = _latest
    repo.get_price_history.return_value = []  # minimal history → LOW

    advisor = ArmoryAdvisor(repo, reward_catalog=catalog)
    result = advisor.get_pass_efficiency(pass_cost=2500.0)

    min_rank = min(_CONFIDENCE_RANK[r.confidence] for r in result.rewards)
    expected_confidence = next(k for k, v in _CONFIDENCE_RANK.items() if v == min_rank)
    assert result.overall_confidence == expected_confidence


@given(
    known_prices=st.lists(_pos_price, min_size=0, max_size=3),
    unknown_count=st.integers(min_value=1, max_value=3),
)
@h_settings(max_examples=150, deadline=1000)
def test_P16_total_roi_none_when_any_price_missing(known_prices, unknown_count):
    """P16: total_roi is None when any reward has no price."""
    from src.domain.analytics.armory_advisor import ArmoryAdvisor

    known_names = [f"Known{i}" for i in range(len(known_prices))]
    unknown_names = [f"Unknown{i}" for i in range(unknown_count)]
    catalog = dict.fromkeys(known_names + unknown_names, 1)

    repo = MagicMock()

    def _latest(name):
        if name.startswith("Known"):
            idx = int(name[5:])
            snap = MagicMock()
            snap.price = known_prices[idx]
            return snap
        return None  # unknown item → no price

    repo.get_latest_price.side_effect = _latest
    repo.get_price_history.return_value = []

    advisor = ArmoryAdvisor(repo, reward_catalog=catalog)
    result = advisor.get_pass_efficiency(pass_cost=2500.0)

    assert result.total_roi is None, (
        "total_roi must be None when at least one reward price is unknown"
    )
