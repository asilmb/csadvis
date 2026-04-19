import dataclasses

import pytest

from src.domain.services import SuperDealDomainService
from src.domain.value_objects import ROI, Amount

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@pytest.fixture()
def svc() -> SuperDealDomainService:
    return SuperDealDomainService()


def _passing_kwargs() -> dict:
    """All filters pass. current=500, baseline=1000, pre_crash_mean=1000."""
    return dict(
        current_price=Amount(500),
        baseline_price=Amount(1000),   # filter 1: 500 < 700 ✓
        z_score=-3.5,               # filter 2: <= -3.0 ✓
        vol_spike=1.5,              # filter 3: <= 2.0 ✓
        history_days=180,           # filter 4: >= 90 ✓
        net_cagr=ROI(0.05),         # filter 5: >= 0.01 ✓
        consecutive_days_at_low=3,  # filter 6: < 7 ✓
        pre_crash_mean=Amount(1000),   # filter 7+8: net=865₸ > 500₸, margin ≈ 0.729 >= 0.20 ✓
        steam_fee_multiplier=1.15,
        steam_fee_fixed=Amount(5),
    )


# ---------------------------------------------------------------------------
# SUCCESS
# ---------------------------------------------------------------------------

class TestSuperDealSuccess:
    def test_is_super_deal_true(self, svc):
        d = svc.evaluate(**_passing_kwargs())
        assert d.is_super_deal is True

    def test_reason_is_none(self, svc):
        d = svc.evaluate(**_passing_kwargs())
        assert d.reason is None

    def test_target_exit_price(self, svc):
        # pre_crash_mean=1000, target = 1000/1.15 = 869.565 -> Amount rounds to 870
        d = svc.evaluate(**_passing_kwargs())
        assert isinstance(d.target_exit_price, Amount)
        assert d.target_exit_price.amount == 870.0

    def test_stop_loss_price(self, svc):
        # current=500, stop = 500 * 0.85 = 425
        d = svc.evaluate(**_passing_kwargs())
        assert isinstance(d.stop_loss_price, Amount)
        assert d.stop_loss_price.amount == 425.0

    def test_expected_margin_formula(self, svc):
        # (1000/1.15 - 5) / 500 - 1 ≈ 0.7291
        d = svc.evaluate(**_passing_kwargs())
        assert isinstance(d.expected_margin, ROI)
        assert pytest.approx(d.expected_margin.value, abs=1e-4) == 0.7291

    def test_decision_is_frozen(self, svc):
        d = svc.evaluate(**_passing_kwargs())
        with pytest.raises(dataclasses.FrozenInstanceError):
            d.is_super_deal = False  # type: ignore[misc]


# ---------------------------------------------------------------------------
# FAILURE — each filter individually
# ---------------------------------------------------------------------------

class TestSuperDealFilterFailures:
    def test_filter1_price_not_deep_enough(self, svc):
        # current=750 is NOT < baseline*0.70=700
        d = svc.evaluate(**{**_passing_kwargs(), "current_price": Amount(750)})
        assert d.is_super_deal is False
        assert "price not deep" in d.reason

    def test_filter1_returns_no_prices(self, svc):
        d = svc.evaluate(**{**_passing_kwargs(), "current_price": Amount(750)})
        assert d.target_exit_price is None
        assert d.stop_loss_price is None
        assert d.expected_margin is None

    def test_filter2_z_score_too_high(self, svc):
        d = svc.evaluate(**{**_passing_kwargs(), "z_score": -2.9})
        assert d.is_super_deal is False
        assert "z_score" in d.reason

    def test_filter3_vol_spike_too_high(self, svc):
        d = svc.evaluate(**{**_passing_kwargs(), "vol_spike": 2.1})
        assert d.is_super_deal is False
        assert "vol_spike" in d.reason

    def test_filter4_insufficient_history(self, svc):
        d = svc.evaluate(**{**_passing_kwargs(), "history_days": 89})
        assert d.is_super_deal is False
        assert "history" in d.reason

    def test_filter5_net_cagr_too_low(self, svc):
        d = svc.evaluate(**{**_passing_kwargs(), "net_cagr": ROI(0.005)})
        assert d.is_super_deal is False
        assert "net_cagr" in d.reason

    def test_filter6_consecutive_days_too_high(self, svc):
        d = svc.evaluate(**{**_passing_kwargs(), "consecutive_days_at_low": 7})
        assert d.is_super_deal is False
        assert "consecutive" in d.reason

    def test_filter8_margin_too_low(self, svc):
        # pre_crash_mean=695 → margin ≈ 0.1987 < 0.20
        d = svc.evaluate(**{**_passing_kwargs(), "pre_crash_mean": Amount(695)})
        assert d.is_super_deal is False
        assert "margin" in d.reason

    def test_filter8_exposes_margin_on_failure(self, svc):
        # expected_margin is populated even on filter-8 failure
        d = svc.evaluate(**{**_passing_kwargs(), "pre_crash_mean": Amount(695)})
        assert isinstance(d.expected_margin, ROI)
        assert d.expected_margin.value < 0.20


# ---------------------------------------------------------------------------
# NET PROCEEDS GUARD (Filter 7)
# ---------------------------------------------------------------------------

class TestNetProceedsGuard:
    def test_guard_fires_when_net_exit_below_current_price(self, svc):
        # pre_crash=20, current=20: net = 20/1.15 - 5 = 12.39 <= 20 → guard fires
        d = svc.evaluate(**{**_passing_kwargs(), "current_price": Amount(20), "pre_crash_mean": Amount(20)})
        assert d.is_super_deal is False
        assert "Negative or zero net margin after fees" in d.reason

    def test_guard_fires_returns_no_prices(self, svc):
        d = svc.evaluate(**{**_passing_kwargs(), "current_price": Amount(20), "pre_crash_mean": Amount(20)})
        assert d.target_exit_price is None
        assert d.stop_loss_price is None
        assert d.expected_margin is None

    def test_guard_fires_on_cheap_item_low_pre_crash(self, svc):
        # pre_crash=30, current=20: net = 30/1.15 - 5 = 21.09 > 20 → guard passes
        # but margin = 21.09/20 - 1 = 5.4% < 20% → filter 8 rejects
        d = svc.evaluate(**{**_passing_kwargs(), "current_price": Amount(20), "pre_crash_mean": Amount(30)})
        assert d.is_super_deal is False
        assert "margin" in d.reason

    def test_guard_respects_custom_fee_multiplier(self, svc):
        # Higher fee → lower net exit → guard fires sooner
        # pre_crash=100, fee=10x: net = 100/10 - 5 = 5 <= 50 → fires
        d = svc.evaluate(**{
            **_passing_kwargs(),
            "current_price": Amount(50),
            "pre_crash_mean": Amount(100),
            "steam_fee_multiplier": 10.0,
            "steam_fee_fixed": Amount(5),
        })
        assert d.is_super_deal is False
        assert "Negative or zero net margin after fees" in d.reason

    def test_guard_respects_custom_fee_fixed(self, svc):
        # pre_crash=1000, current=500, fee_fixed=900:
        # net = 1000/1.15 - 900 = 869.6 - 900 = -30.4 <= 500 → guard fires
        d = svc.evaluate(**{
            **_passing_kwargs(),
            "steam_fee_fixed": Amount(900),
        })
        assert d.is_super_deal is False
        assert "Negative or zero net margin after fees" in d.reason


# ---------------------------------------------------------------------------
# EDGE CASES
# ---------------------------------------------------------------------------

class TestSuperDealEdgeCases:
    def test_z_score_exactly_minus_3_passes(self, svc):
        d = svc.evaluate(**{**_passing_kwargs(), "z_score": -3.0})
        assert d.is_super_deal is True

    def test_history_days_exactly_90_passes(self, svc):
        d = svc.evaluate(**{**_passing_kwargs(), "history_days": 90})
        assert d.is_super_deal is True

    def test_consecutive_days_exactly_7_fails(self, svc):
        d = svc.evaluate(**{**_passing_kwargs(), "consecutive_days_at_low": 7})
        assert d.is_super_deal is False

    def test_consecutive_days_6_passes(self, svc):
        d = svc.evaluate(**{**_passing_kwargs(), "consecutive_days_at_low": 6})
        assert d.is_super_deal is True

    def test_margin_just_above_20pct_passes(self, svc):
        # pre_crash_mean=696 → margin ≈ 0.2004 >= 0.20
        d = svc.evaluate(**{**_passing_kwargs(), "pre_crash_mean": Amount(696)})
        assert d.is_super_deal is True
        assert d.expected_margin.value >= 0.20

    def test_margin_just_below_20pct_fails(self, svc):
        # pre_crash_mean=695 → margin ≈ 0.1987 < 0.20
        d = svc.evaluate(**{**_passing_kwargs(), "pre_crash_mean": Amount(695)})
        assert d.is_super_deal is False

    def test_net_cagr_exactly_1pct_passes(self, svc):
        d = svc.evaluate(**{**_passing_kwargs(), "net_cagr": ROI(0.01)})
        assert d.is_super_deal is True

    def test_vol_spike_exactly_2_passes(self, svc):
        d = svc.evaluate(**{**_passing_kwargs(), "vol_spike": 2.0})
        assert d.is_super_deal is True

    def test_current_price_one_below_threshold_passes(self, svc):
        # baseline=1000, threshold=700; current=699 passes
        d = svc.evaluate(**{**_passing_kwargs(), "current_price": Amount(699)})
        assert d.is_super_deal is True

    def test_current_price_at_threshold_fails(self, svc):
        # current=700 is NOT < 700
        d = svc.evaluate(**{**_passing_kwargs(), "current_price": Amount(700)})
        assert d.is_super_deal is False
