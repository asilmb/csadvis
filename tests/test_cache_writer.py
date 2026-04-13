"""
Unit tests for services/cache_writer.py — write_portfolio_advice,
write_investment_signals.

All tests use MagicMock for the DB session — no real DB or network.
"""

from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from infra.cache_writer import write_investment_signals, write_portfolio_advice

# ─── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture()
def mock_db():
    """Return a fresh MagicMock that mimics a SQLAlchemy Session."""
    return MagicMock()


def _minimal_plan(**overrides) -> dict:
    base = {
        "total_balance": 50000.0,
        "total_capital": 120000.0,
        "inventory_value": 70000.0,
        "flip_budget": 48000.0,
        "invest_budget": 48000.0,
        "reserve_amount": 24000.0,
        "flip": {"name": "Prisma 2 Case", "buy_price": 1200, "sell_price": 1500},
        "invest": {"name": "CS20 Case", "cagr_pct": 9.5},
        "top_flips": [{"name": "Prisma 2 Case"}],
        "top_invests": [{"name": "CS20 Case"}],
        "sell": [{"name": "Chroma 2 Case", "qty": 3}],
        "correlation_warning": None,
    }
    base.update(overrides)
    return base


def _minimal_signals() -> dict[str, dict]:
    return {
        "cid-001": {
            "verdict": "BUY",
            "score": 2,
            "ratio_signal": "CHEAP",
            "momentum_signal": None,
            "trend_signal": None,
            "event_signal": None,
            "sell_at_loss": False,
            "unrealized_pnl": None,
        },
        "cid-002": {
            "verdict": "HOLD",
            "score": 0,
            "ratio_signal": None,
            "momentum_signal": None,
            "trend_signal": None,
            "event_signal": None,
            "sell_at_loss": False,
            "unrealized_pnl": 1500.0,
        },
    }


# ─── write_portfolio_advice ────────────────────────────────────────────────────


def test_write_portfolio_advice_deletes_then_adds(mock_db):
    """Existing rows are cleared and exactly one new row is added."""
    write_portfolio_advice(mock_db, _minimal_plan())
    mock_db.query.assert_called()
    mock_db.add.assert_called_once()


def test_write_portfolio_advice_row_fields(mock_db):
    """The inserted FactPortfolioAdvice row has correct scalar fields."""
    from domain.models import FactPortfolioAdvice

    captured = []
    mock_db.add.side_effect = lambda row: captured.append(row)

    write_portfolio_advice(mock_db, _minimal_plan())

    assert len(captured) == 1
    row = captured[0]
    assert isinstance(row, FactPortfolioAdvice)
    assert row.wallet == 50000.0
    assert row.total_capital == 120000.0
    assert row.inventory_value == 70000.0
    assert row.flip_budget == 48000.0
    assert row.invest_budget == 48000.0
    assert row.reserve_amount == 24000.0
    assert row.correlation_warning is None


def test_write_portfolio_advice_json_serialisation(mock_db):
    """Nested dicts and lists are stored as JSON strings, not Python objects."""

    captured = []
    mock_db.add.side_effect = lambda row: captured.append(row)

    write_portfolio_advice(mock_db, _minimal_plan())

    row = captured[0]
    assert isinstance(row.flip_json, str), "flip_json must be a JSON string"
    assert isinstance(row.invest_json, str), "invest_json must be a JSON string"
    assert isinstance(row.top_flips_json, str), "top_flips_json must be a JSON string"
    assert isinstance(row.sell_json, str), "sell_json must be a JSON string"

    # Deserialisation round-trip
    flip = json.loads(row.flip_json)
    assert flip["name"] == "Prisma 2 Case"
    assert flip["buy_price"] == 1200


def test_write_portfolio_advice_null_flip_invest(mock_db):
    """flip_json and invest_json are None when plan has no flip/invest candidates."""

    captured = []
    mock_db.add.side_effect = lambda row: captured.append(row)

    write_portfolio_advice(mock_db, _minimal_plan(flip=None, invest=None))

    row = captured[0]
    assert row.flip_json is None
    assert row.invest_json is None


def test_write_portfolio_advice_no_commit_called(mock_db):
    """write_portfolio_advice must NOT call db.commit() — caller owns transaction."""
    write_portfolio_advice(mock_db, _minimal_plan())
    mock_db.commit.assert_not_called()


# ─── write_investment_signals ──────────────────────────────────────────────────


def test_write_investment_signals_bulk_insert(mock_db):
    """One row per container_id is passed to bulk_save_objects."""
    from domain.models import FactInvestmentSignal

    now = datetime(2026, 3, 28, 12, 0, 0)
    write_investment_signals(mock_db, _minimal_signals(), now)

    mock_db.query.assert_called()
    mock_db.bulk_save_objects.assert_called_once()
    rows = mock_db.bulk_save_objects.call_args[0][0]
    assert len(rows) == 2
    assert all(isinstance(r, FactInvestmentSignal) for r in rows)


def test_write_investment_signals_verdict_and_score(mock_db):
    """FactInvestmentSignal rows carry correct verdict and score values."""
    now = datetime(2026, 3, 28, 12, 0, 0)
    captured_rows = []
    mock_db.bulk_save_objects.side_effect = lambda rows: captured_rows.extend(rows)

    write_investment_signals(mock_db, _minimal_signals(), now)

    by_cid = {r.container_id: r for r in captured_rows}
    assert by_cid["cid-001"].verdict == "BUY"
    assert by_cid["cid-001"].score == 2
    assert by_cid["cid-002"].verdict == "HOLD"
    assert by_cid["cid-002"].unrealized_pnl == 1500.0


def test_write_investment_signals_sell_at_loss_bool_to_int(mock_db):
    """sell_at_loss Python bool is stored as integer 0/1."""
    signals = {
        "cid-003": {
            "verdict": "SELL",
            "score": -2,
            "ratio_signal": None,
            "momentum_signal": None,
            "trend_signal": None,
            "event_signal": None,
            "sell_at_loss": True,
            "unrealized_pnl": -500.0,
        }
    }
    now = datetime(2026, 3, 28, 12, 0, 0)
    captured_rows = []
    mock_db.bulk_save_objects.side_effect = lambda rows: captured_rows.extend(rows)

    write_investment_signals(mock_db, signals, now)

    assert captured_rows[0].sell_at_loss == 1


def test_write_investment_signals_no_commit_called(mock_db):
    """write_investment_signals must NOT call db.commit() — caller owns transaction."""
    now = datetime(2026, 3, 28, 12, 0, 0)
    write_investment_signals(mock_db, _minimal_signals(), now)
    mock_db.commit.assert_not_called()


def test_write_investment_signals_empty_signals(mock_db):
    """Empty signals dict — bulk_save_objects is not called (no rows to insert)."""
    now = datetime(2026, 3, 28, 12, 0, 0)
    write_investment_signals(mock_db, {}, now)
    mock_db.bulk_save_objects.assert_not_called()


# ─── Signal label helpers ─────────────────────────────────────────────────────


def test_get_ratio_label_cheap():
    from infra.cache_writer import _get_ratio_label

    assert _get_ratio_label(-10.0) == "CHEAP"
    assert _get_ratio_label(-50.0) == "CHEAP"


def test_get_ratio_label_expensive():
    from infra.cache_writer import _get_ratio_label

    assert _get_ratio_label(10.0) == "EXPENSIVE"
    assert _get_ratio_label(99.0) == "EXPENSIVE"


def test_get_ratio_label_neutral():
    from infra.cache_writer import _get_ratio_label

    assert _get_ratio_label(0.0) == "NEUTRAL"
    assert _get_ratio_label(5.0) == "NEUTRAL"
    assert _get_ratio_label(-9.9) == "NEUTRAL"


def test_get_ratio_label_none():
    from infra.cache_writer import _get_ratio_label

    assert _get_ratio_label(None) == "NEUTRAL"


def test_get_momentum_label_rising():
    from infra.cache_writer import _get_momentum_label

    assert _get_momentum_label(5.0) == "RISING"
    assert _get_momentum_label(20.0) == "RISING"


def test_get_momentum_label_falling():
    from infra.cache_writer import _get_momentum_label

    assert _get_momentum_label(-5.0) == "FALLING"
    assert _get_momentum_label(-20.0) == "FALLING"


def test_get_momentum_label_stable():
    from infra.cache_writer import _get_momentum_label

    assert _get_momentum_label(0.0) == "STABLE"
    assert _get_momentum_label(4.9) == "STABLE"
    assert _get_momentum_label(-4.9) == "STABLE"


def test_get_momentum_label_none():
    from infra.cache_writer import _get_momentum_label

    assert _get_momentum_label(None) == "STABLE"


def test_write_investment_signals_ratio_momentum_mapped_from_engine_keys(mock_db):
    """ratio_signal and momentum_signal are derived from price_ratio_pct / momentum_pct."""
    signals = {
        "cid-010": {
            "verdict": "BUY",
            "score": 2,
            "price_ratio_pct": -15.0,  # CHEAP
            "momentum_pct": -8.0,  # FALLING
            "trend_signal": None,
            "event_signal": None,
            "sell_at_loss": False,
            "unrealized_pnl": None,
        },
        "cid-011": {
            "verdict": "SELL",
            "score": -2,
            "price_ratio_pct": 25.0,  # EXPENSIVE
            "momentum_pct": 10.0,  # RISING
            "trend_signal": None,
            "event_signal": None,
            "sell_at_loss": True,
            "unrealized_pnl": None,
        },
    }
    now = datetime(2026, 4, 3, 10, 0, 0)
    captured_rows = []
    mock_db.bulk_save_objects.side_effect = lambda rows: captured_rows.extend(rows)

    write_investment_signals(mock_db, signals, now)

    by_cid = {r.container_id: r for r in captured_rows}
    assert by_cid["cid-010"].ratio_signal == "CHEAP"
    assert by_cid["cid-010"].momentum_signal == "FALLING"
    assert by_cid["cid-011"].ratio_signal == "EXPENSIVE"
    assert by_cid["cid-011"].momentum_signal == "RISING"


# ─── reader functions (get_cached_portfolio_advice, get_cached_signals) ───────


def test_get_cached_portfolio_advice_returns_none_on_empty():
    """Returns None when fact_portfolio_advice table is empty."""
    from domain.portfolio import get_cached_portfolio_advice

    mock_db_instance = MagicMock()
    mock_query = MagicMock()
    mock_db_instance.query.return_value = mock_query
    mock_query.order_by.return_value = mock_query
    mock_query.first.return_value = None

    with patch("domain.portfolio.SessionLocal", return_value=mock_db_instance):
        result = get_cached_portfolio_advice()

    assert result is None


def test_get_cached_portfolio_advice_deserialises_json():
    """Returns a dict with flip/invest deserialised from JSON strings."""
    from domain.models import FactPortfolioAdvice
    from domain.portfolio import get_cached_portfolio_advice

    fake_row = MagicMock(spec=FactPortfolioAdvice)
    fake_row.computed_at = datetime(2026, 3, 28, 10, 0, 0)
    fake_row.wallet = 50000.0
    fake_row.total_capital = 120000.0
    fake_row.inventory_value = 70000.0
    fake_row.flip_budget = 48000.0
    fake_row.invest_budget = 48000.0
    fake_row.reserve_amount = 24000.0
    fake_row.flip_json = json.dumps({"name": "Prisma 2 Case", "buy_price": 1200})
    fake_row.invest_json = json.dumps({"name": "CS20 Case", "cagr_pct": 9.5})
    fake_row.top_flips_json = json.dumps([{"name": "Prisma 2 Case"}])
    fake_row.top_invests_json = json.dumps([{"name": "CS20 Case"}])
    fake_row.sell_json = json.dumps([])
    fake_row.correlation_warning = None

    mock_db_instance = MagicMock()
    mock_query = MagicMock()
    mock_db_instance.query.return_value = mock_query
    mock_query.order_by.return_value = mock_query
    mock_query.first.return_value = fake_row

    with patch("domain.portfolio.SessionLocal", return_value=mock_db_instance):
        result = get_cached_portfolio_advice()

    assert result is not None
    assert result["flip"]["name"] == "Prisma 2 Case"
    assert result["invest"]["cagr_pct"] == 9.5
    assert result["total_balance"] == 50000.0


def test_get_cached_signals_returns_empty_on_no_data():
    """Returns empty dict when fact_investment_signals table is empty."""
    from domain.portfolio import get_cached_signals

    mock_db_instance = MagicMock()
    mock_query = MagicMock()
    mock_db_instance.query.return_value = mock_query
    mock_query.scalar.return_value = None

    with patch("domain.portfolio.SessionLocal", return_value=mock_db_instance):
        result = get_cached_signals()

    assert result == {}


def test_get_cached_signals_returns_latest_batch():
    """Returns all rows from the most-recent computed_at batch as a dict."""
    from domain.models import FactInvestmentSignal
    from domain.portfolio import get_cached_signals

    ts = datetime(2026, 3, 28, 12, 0, 0)

    def _make_row(cid: str, verdict: str, score: int) -> MagicMock:
        r = MagicMock(spec=FactInvestmentSignal)
        r.container_id = cid
        r.verdict = verdict
        r.score = score
        r.ratio_signal = None
        r.momentum_signal = None
        r.trend_signal = None
        r.event_signal = None
        r.sell_at_loss = 0
        r.unrealized_pnl = None
        r.computed_at = ts
        return r

    rows = [_make_row("cid-001", "BUY", 2), _make_row("cid-002", "HOLD", 0)]

    mock_db_instance = MagicMock()
    mock_query_ts = MagicMock()
    mock_query_rows = MagicMock()

    call_count = [0]

    def _query_side_effect(*args):
        call_count[0] += 1
        if call_count[0] == 1:
            return mock_query_ts  # first call: max(computed_at)
        return mock_query_rows  # second call: filter rows

    mock_db_instance.query.side_effect = _query_side_effect
    mock_query_ts.scalar.return_value = ts
    mock_query_rows.filter.return_value = mock_query_rows
    mock_query_rows.all.return_value = rows

    with patch("domain.portfolio.SessionLocal", return_value=mock_db_instance):
        result = get_cached_signals()

    assert len(result) == 2
    assert result["cid-001"]["verdict"] == "BUY"
    assert result["cid-001"]["score"] == 2
    assert result["cid-002"]["verdict"] == "HOLD"
