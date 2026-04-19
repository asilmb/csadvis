import dataclasses

import pytest

from src.domain.value_objects import ROI, Amount, Percentage

# ---------------------------------------------------------------------------
# KZT — rounding
# ---------------------------------------------------------------------------

class TestKZTRounding:
    def test_half_rounds_up(self):
        assert Amount(100.5).amount == 101.0

    def test_below_half_rounds_down(self):
        assert Amount(100.4).amount == 100.0

    def test_above_half_rounds_up(self):
        assert Amount(100.51).amount == 101.0

    def test_exact_integer_unchanged(self):
        assert Amount(100.0).amount == 100.0

    def test_negative_half_rounds_away_from_zero(self):
        assert Amount(-100.5).amount == -101.0

    def test_zero(self):
        assert Amount(0.0).amount == 0.0


# ---------------------------------------------------------------------------
# KZT — arithmetic
# ---------------------------------------------------------------------------

class TestKZTArithmetic:
    def test_add_returns_kzt(self):
        result = Amount(1000) + Amount(500)
        assert isinstance(result, Amount)
        assert result.amount == 1500.0

    def test_sub_returns_kzt(self):
        result = Amount(1000) - Amount(300)
        assert isinstance(result, Amount)
        assert result.amount == 700.0

    def test_mul_float_returns_kzt(self):
        result = Amount(1000) * 1.5
        assert isinstance(result, Amount)
        assert result.amount == 1500.0

    def test_mul_applies_rounding(self):
        # 201 * 0.5 = 100.5 exactly in IEEE 754 → rounds up to 101
        result = Amount(201) * 0.5
        assert result.amount == 101.0

    def test_rmul(self):
        assert (3 * Amount(200)).amount == 600.0

    def test_truediv(self):
        assert (Amount(1000) / 4).amount == 250.0

    def test_add_float_raises_typeerror(self):
        with pytest.raises(TypeError):
            _ = Amount(100) + 50.0  # type: ignore[operator]

    def test_sub_float_raises_typeerror(self):
        with pytest.raises(TypeError):
            _ = Amount(100) - 50.0  # type: ignore[operator]


# ---------------------------------------------------------------------------
# KZT — comparison
# ---------------------------------------------------------------------------

class TestKZTComparison:
    def test_equal(self):
        assert Amount(100) == Amount(100)

    def test_not_equal(self):
        assert Amount(100) != Amount(200)

    def test_greater_than(self):
        assert Amount(200) > Amount(100)

    def test_less_than(self):
        assert Amount(100) < Amount(200)

    def test_gte(self):
        assert Amount(200) >= Amount(200)
        assert Amount(201) >= Amount(200)

    def test_lte(self):
        assert Amount(200) <= Amount(200)
        assert Amount(199) <= Amount(200)

    def test_hashable_in_set(self):
        s = {Amount(100), Amount(100), Amount(200)}
        assert len(s) == 2


# ---------------------------------------------------------------------------
# KZT — string representation
# ---------------------------------------------------------------------------

class TestKZTStr:
    def test_format_1500(self):
        assert str(Amount(1500.0)) == "1,500"

    def test_format_1000000(self):
        assert str(Amount(1_000_000)) == "1,000,000"

    def test_format_zero(self):
        assert str(Amount(0)) == "0"

    def test_fstring_uses_str(self):
        assert f"{Amount(1500.0)}" == "1,500"


# ---------------------------------------------------------------------------
# ROI
# ---------------------------------------------------------------------------

class TestROI:
    def test_to_percent_str_5_21(self):
        assert ROI(0.0521).to_percent_str() == "5.21%"

    def test_to_percent_str_10(self):
        assert ROI(0.1).to_percent_str() == "10.00%"

    def test_to_percent_str_zero(self):
        assert ROI(0.0).to_percent_str() == "0.00%"

    def test_mul_number(self):
        result = ROI(0.05) * 2
        assert isinstance(result, ROI)
        assert pytest.approx(result.value) == 0.10

    def test_rmul(self):
        result = 3 * ROI(0.05)
        assert isinstance(result, ROI)
        assert pytest.approx(result.value) == 0.15

    def test_comparison_gt(self):
        assert ROI(0.1) > ROI(0.05)

    def test_comparison_lt(self):
        assert ROI(0.05) < ROI(0.1)

    def test_comparison_eq(self):
        assert ROI(0.05) == ROI(0.05)

    def test_comparison_gte(self):
        assert ROI(0.1) >= ROI(0.1)
        assert ROI(0.11) >= ROI(0.1)

    def test_hashable(self):
        s = {ROI(0.05), ROI(0.05), ROI(0.1)}
        assert len(s) == 2

    def test_frozen(self):
        r = ROI(0.05)
        with pytest.raises(dataclasses.FrozenInstanceError):
            r.value = 0.1  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Percentage
# ---------------------------------------------------------------------------

class TestPercentage:
    def test_boundary_zero(self):
        assert Percentage(0.0).value == 0.0

    def test_boundary_one(self):
        assert Percentage(1.0).value == 1.0

    def test_valid_midpoint(self):
        assert Percentage(0.4).value == 0.4

    def test_above_one_raises(self):
        with pytest.raises(ValueError):
            Percentage(1.1)

    def test_below_zero_raises(self):
        with pytest.raises(ValueError):
            Percentage(-0.1)

    def test_comparison_gt(self):
        assert Percentage(0.4) > Percentage(0.2)

    def test_comparison_lt(self):
        assert Percentage(0.2) < Percentage(0.4)

    def test_comparison_eq(self):
        assert Percentage(0.4) == Percentage(0.4)

    def test_comparison_gte(self):
        assert Percentage(0.5) >= Percentage(0.5)

    def test_hashable(self):
        s = {Percentage(0.4), Percentage(0.4), Percentage(0.6)}
        assert len(s) == 2

    def test_frozen(self):
        p = Percentage(0.5)
        with pytest.raises(dataclasses.FrozenInstanceError):
            p.value = 0.9  # type: ignore[misc]
