"""Tests for engine/correlation.py — Pearson r and portfolio warnings."""

from __future__ import annotations

import pytest

from domain.correlation import (
    _pearson,
    _to_log_returns,
    check_portfolio_correlation,
    compute_correlation_matrix,
)

# ─── _pearson ────────────────────────────────────────────────────────────────


class TestPearson:
    def test_perfect_positive_correlation(self) -> None:
        # Use 30 samples to meet _MIN_SAMPLE_PEARSON threshold
        xs = [float(i + 1) for i in range(30)]
        ys = [x * 2 for x in xs]
        r = _pearson(xs, ys)
        assert r == pytest.approx(1.0, abs=1e-9)

    def test_perfect_negative_correlation(self) -> None:
        xs = [float(i + 1) for i in range(30)]
        ys = [31.0 - x for x in xs]
        r = _pearson(xs, ys)
        assert r == pytest.approx(-1.0, abs=1e-9)

    def test_constant_series_returns_none(self) -> None:
        xs = [5.0] * 30
        ys = [float(i + 1) for i in range(30)]
        # std(xs) = 0 → denominator = 0 → None
        assert _pearson(xs, ys) is None

    def test_fewer_than_30_returns_none(self) -> None:
        # _MIN_SAMPLE_PEARSON = 30; 29 samples must return None
        xs = [float(i) for i in range(29)]
        ys = [float(i) for i in range(29)]
        assert _pearson(xs, ys) is None

    def test_exactly_30_is_computed(self) -> None:
        xs = [float(i) for i in range(30)]
        ys = [float(i) for i in range(30)]
        r = _pearson(xs, ys)
        assert r is not None
        assert r == pytest.approx(1.0, abs=1e-9)

    def test_result_in_minus_one_to_one(self) -> None:
        import math

        xs = [math.sin(i * 0.3) for i in range(50)]
        ys = [math.cos(i * 0.3) for i in range(50)]
        r = _pearson(xs, ys)
        assert r is not None
        assert -1.0 <= r <= 1.0


# ─── compute_correlation_matrix ──────────────────────────────────────────────


def _make_history(prices: list[float]) -> list[dict]:
    """Build fake price history rows with sequential daily timestamps starting 2024-01-01."""
    from datetime import date, timedelta

    start = date(2024, 1, 1)
    rows = []
    for i, p in enumerate(prices):
        d = start + timedelta(days=i)
        rows.append(
            {
                "timestamp": f"{d.isoformat()} 12:00",
                "price": p,
            }
        )
    return rows


class TestComputeCorrelationMatrix:
    def test_returns_empty_on_no_history(self) -> None:
        result = compute_correlation_matrix({}, {})
        assert result == {"names": [], "matrix": [], "pairs": []}

    def test_single_container_no_pairs(self) -> None:
        # A single container has no dates shared with any other series
        # (common_dates requires cnt >= 2), so matrix is empty.
        prices = [float(i + 1) for i in range(35)]
        result = compute_correlation_matrix(
            {"c1": _make_history(prices)},
            {"c1": "Container A"},
        )
        assert result["pairs"] == []
        assert result["matrix"] == []

    def test_two_perfectly_correlated(self) -> None:
        # Need 30+ shared dates to meet _MIN_SAMPLE_PEARSON for both series and _pearson()
        prices_a = [float(i + 1) for i in range(35)]
        prices_b = [p * 2 for p in prices_a]
        result = compute_correlation_matrix(
            {"c1": _make_history(prices_a), "c2": _make_history(prices_b)},
            {"c1": "A", "c2": "B"},
        )
        assert len(result["pairs"]) == 1
        _, _, r = result["pairs"][0]
        assert r == pytest.approx(1.0, abs=1e-6)

    def test_diagonal_always_one(self) -> None:
        prices_a = [float(i + 1) for i in range(35)]
        prices_b = [35.0 - float(i) for i in range(35)]
        result = compute_correlation_matrix(
            {"c1": _make_history(prices_a), "c2": _make_history(prices_b)},
            {"c1": "A", "c2": "B"},
        )
        matrix = result["matrix"]
        for i in range(len(matrix)):
            assert matrix[i][i] == 1.0

    def test_matrix_is_symmetric(self) -> None:
        prices_a = [float(i + 1) for i in range(35)]
        prices_b = [float(35 - i) for i in range(35)]
        result = compute_correlation_matrix(
            {"c1": _make_history(prices_a), "c2": _make_history(prices_b)},
            {"c1": "A", "c2": "B"},
        )
        m = result["matrix"]
        assert m[0][1] == m[1][0]

    def test_pairs_sorted_by_abs_r(self) -> None:
        # Three containers: A perfectly correlated with B, C slightly with A
        # Use 35 prices so that log-returns (N-1=34) exceed _MIN_SAMPLE_PEARSON=30
        prices_a = [float(i + 1) for i in range(35)]
        prices_b = [p * 2 for p in prices_a]  # r(A,B) ≈ 1.0
        prices_c = [p + (i % 5) * 0.5 for i, p in enumerate(prices_a)]  # r(A,C) < 1.0

        result = compute_correlation_matrix(
            {
                "c1": _make_history(prices_a),
                "c2": _make_history(prices_b),
                "c3": _make_history(prices_c),
            },
            {"c1": "A", "c2": "B", "c3": "C"},
        )
        abs_rs = [abs(r) for _, _, r in result["pairs"]]
        assert abs_rs == sorted(abs_rs, reverse=True)


# ─── check_portfolio_correlation ─────────────────────────────────────────────


class TestCheckPortfolioCorrelation:
    _pairs = [("Alpha", "Beta", 0.85), ("Alpha", "Gamma", 0.30)]

    def test_warns_on_high_correlation(self) -> None:
        warning = check_portfolio_correlation("Alpha", "Beta", self._pairs)
        assert warning is not None
        assert "0.85" in warning or "коррелиров" in warning.lower() or "r =" in warning

    def test_no_warning_below_threshold(self) -> None:
        warning = check_portfolio_correlation("Alpha", "Gamma", self._pairs)
        assert warning is None

    def test_no_warning_when_same_name(self) -> None:
        warning = check_portfolio_correlation("Alpha", "Alpha", self._pairs)
        assert warning is None

    def test_no_warning_on_none_names(self) -> None:
        assert check_portfolio_correlation(None, "Beta", self._pairs) is None
        assert check_portfolio_correlation("Alpha", None, self._pairs) is None

    def test_warns_on_high_negative_correlation(self) -> None:
        pairs = [("X", "Y", -0.90)]
        warning = check_portfolio_correlation("X", "Y", pairs)
        assert warning is not None


# ─── _resample_pair ──────────────────────────────────────────────────────────


class TestResamplePair:
    def test_no_gaps_passthrough(self) -> None:
        """Contiguous daily data → resampled length equals original."""
        from domain.correlation import _resample_pair

        si = {"2024-01-01": 100.0, "2024-01-02": 101.0, "2024-01-03": 102.0}
        sj = {"2024-01-01": 200.0, "2024-01-02": 202.0, "2024-01-03": 204.0}
        pi, pj = _resample_pair(si, sj)
        assert pi == [100.0, 101.0, 102.0]
        assert pj == [200.0, 202.0, 204.0]

    def test_gap_in_one_series_fills_forward(self) -> None:
        """Series i missing Jan 2 → Jan 2 price is filled from Jan 1."""
        from domain.correlation import _resample_pair

        si = {"2024-01-01": 100.0, "2024-01-03": 102.0}  # gap on Jan 2
        sj = {"2024-01-01": 200.0, "2024-01-02": 201.0, "2024-01-03": 204.0}
        pi, pj = _resample_pair(si, sj)
        # Jan 1: real i, real j
        # Jan 2: ffill i (100.0), real j (201.0) — at least one real → included
        # Jan 3: real i (102.0), real j (204.0)
        assert pi == [100.0, 100.0, 102.0]
        assert pj == [200.0, 201.0, 204.0]

    def test_day_excluded_when_both_ffill(self) -> None:
        """Days where neither series has real data are excluded."""
        from domain.correlation import _resample_pair

        # Both have data Jan 1 and Jan 3 only — Jan 2 is ffill for both
        si = {"2024-01-01": 100.0, "2024-01-03": 102.0}
        sj = {"2024-01-01": 200.0, "2024-01-03": 204.0}
        pi, pj = _resample_pair(si, sj)
        # Jan 2: both ffill → excluded
        assert len(pi) == 2
        assert pi == [100.0, 102.0]
        assert pj == [200.0, 204.0]

    def test_no_overlap_returns_empty(self) -> None:
        from domain.correlation import _resample_pair

        si = {"2024-01-01": 100.0, "2024-01-02": 101.0}
        sj = {"2024-02-01": 200.0, "2024-02-02": 202.0}
        pi, pj = _resample_pair(si, sj)
        assert pi == []
        assert pj == []

    def test_empty_series_returns_empty(self) -> None:
        from domain.correlation import _resample_pair

        assert _resample_pair({}, {"2024-01-01": 1.0}) == ([], [])
        assert _resample_pair({"2024-01-01": 1.0}, {}) == ([], [])

    def test_equal_length_output(self) -> None:
        """Output vectors are always equal length."""
        from domain.correlation import _resample_pair

        si = {"2024-01-01": 10.0, "2024-01-04": 13.0, "2024-01-07": 16.0}
        sj = {"2024-01-01": 20.0, "2024-01-03": 22.0, "2024-01-07": 26.0}
        pi, pj = _resample_pair(si, sj)
        assert len(pi) == len(pj)

    def test_resampled_correlation_equals_gapless(self) -> None:
        """
        Perfectly correlated series with a shared gap should still produce r ≈ 1.0
        after resampling, because ffill preserves the relative movement.
        """

        # 35 real points, same dates across Feb–Mar 2024 (no calendar overflow)
        from datetime import date, timedelta

        from domain.correlation import _pearson, _resample_pair, _to_log_returns

        start = date(2024, 2, 1)
        base_dates = [(start + timedelta(days=i)).isoformat() for i in range(35)]
        si = {d: float(i + 1) for i, d in enumerate(base_dates)}
        sj = {d: float(i + 1) * 2.0 for i, d in enumerate(base_dates)}

        pi, pj = _resample_pair(si, sj)
        xs = _to_log_returns(pi)
        ys = _to_log_returns(pj)
        r = _pearson(xs, ys)
        assert r is not None
        assert r == pytest.approx(1.0, abs=1e-6)


# ─── S13-MINOR-3: _to_log_returns direct unit tests ──────────────────────────


class TestToLogReturns:
    def test_basic_returns(self) -> None:
        import math

        prices = [1.0, 2.0, 4.0]  # ln(2/1)=0.693, ln(4/2)=0.693
        result = _to_log_returns(prices)
        assert len(result) == 2
        assert result[0] == pytest.approx(math.log(2.0), abs=1e-9)
        assert result[1] == pytest.approx(math.log(2.0), abs=1e-9)

    def test_zero_price_yields_zero_return(self) -> None:
        # zero price → cannot compute log → yields 0.0
        prices = [1.0, 0.0, 1.0]
        result = _to_log_returns(prices)
        assert result[0] == 0.0  # 0/1 → not > 0, so 0.0
        assert result[1] == 0.0  # 1/0 → not > 0 in prev, so 0.0

    def test_single_element_returns_empty(self) -> None:
        result = _to_log_returns([5.0])
        assert result == []

    def test_empty_returns_empty(self) -> None:
        result = _to_log_returns([])
        assert result == []

    def test_constant_series_all_zeros(self) -> None:
        prices = [3.0, 3.0, 3.0, 3.0]
        result = _to_log_returns(prices)
        # ln(3/3) = 0 for all
        assert all(r == pytest.approx(0.0) for r in result)
        assert len(result) == 3
