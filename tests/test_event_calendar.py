"""Tests for engine/event_calendar.py — event-driven trade signals."""

from __future__ import annotations

from datetime import date, timedelta

from engine.event_calendar import (
    _POST_EVENT_DAYS,
    _PRE_EVENT_DAYS,
    EVENTS,
    _matches,
    get_event_impact,
    get_event_signals,
    get_upcoming_events,
    is_calendar_stale,
)

# ─── _matches ────────────────────────────────────────────────────────────────


class TestMatches:
    def test_case_insensitive_match(self) -> None:
        assert _matches("PGL Bucharest 2025 Capsule", ["bucharest"]) is True

    def test_no_match(self) -> None:
        assert _matches("IEM Cologne 2025", ["bucharest"]) is False

    def test_multiple_keywords_any_match(self) -> None:
        assert _matches("IEM Cologne 2025", ["bucharest", "cologne"]) is True

    def test_empty_keywords(self) -> None:
        assert _matches("Any Name", []) is False


# ─── get_event_signals ───────────────────────────────────────────────────────


def _first_event() -> dict:
    """Return a known stable event from EVENTS for deterministic testing.

    Uses a named lookup instead of EVENTS[0] so that adding events at the front
    of the calendar does not silently break these tests (S13-MINOR-8 coupling fix).
    """
    name = "IEM Katowice 2025"
    for ev in EVENTS:
        if ev["name"] == name:
            return ev
    raise RuntimeError(f"Test fixture event '{name}' not found in EVENTS — update _first_event()")


class TestGetEventSignals:
    def test_buy_signal_before_event(self) -> None:
        ev = _first_event()
        # 10 days before start → BUY
        today = ev["start"] - timedelta(days=10)
        name = f"Capsule {ev['keywords'][0].title()}"
        signals = get_event_signals([name], today=today)
        assert name in signals
        assert signals[name]["signal"] == "BUY"

    def test_hold_signal_during_event(self) -> None:
        ev = _first_event()
        today = ev["start"] + timedelta(days=1)
        name = f"Capsule {ev['keywords'][0].title()}"
        signals = get_event_signals([name], today=today)
        assert signals[name]["signal"] == "HOLD"

    def test_sell_signal_after_event(self) -> None:
        ev = _first_event()
        today = ev["end"] + timedelta(days=3)
        name = f"Capsule {ev['keywords'][0].title()}"
        signals = get_event_signals([name], today=today)
        assert signals[name]["signal"] == "SELL"

    def test_no_signal_outside_all_windows(self) -> None:
        ev = _first_event()
        # Way before pre-event window
        today = ev["start"] - timedelta(days=_PRE_EVENT_DAYS + 30)
        name = f"Capsule {ev['keywords'][0].title()}"
        signals = get_event_signals([name], today=today)
        assert name not in signals

    def test_no_signal_after_post_event_window(self) -> None:
        ev = _first_event()
        today = ev["end"] + timedelta(days=_POST_EVENT_DAYS + 1)
        name = f"Capsule {ev['keywords'][0].title()}"
        signals = get_event_signals([name], today=today)
        assert name not in signals

    def test_unmatched_container_gets_no_signal(self) -> None:
        signals = get_event_signals(["Completely Unrelated Item"], today=date(2025, 3, 20))
        assert signals == {}

    def test_signal_metadata_keys(self) -> None:
        ev = _first_event()
        today = ev["start"] - timedelta(days=5)
        name = f"Capsule {ev['keywords'][0].title()}"
        signals = get_event_signals([name], today=today)
        info = signals[name]
        assert "event" in info
        assert "signal" in info
        assert "start" in info
        assert "end" in info
        assert "message" in info

    def test_buy_signal_exactly_on_pre_event_boundary(self) -> None:
        ev = _first_event()
        today = ev["start"] - timedelta(days=_PRE_EVENT_DAYS)
        name = f"Capsule {ev['keywords'][0].title()}"
        signals = get_event_signals([name], today=today)
        assert signals[name]["signal"] == "BUY"

    def test_sell_signal_exactly_on_post_event_boundary(self) -> None:
        ev = _first_event()
        today = ev["end"] + timedelta(days=_POST_EVENT_DAYS)
        name = f"Capsule {ev['keywords'][0].title()}"
        signals = get_event_signals([name], today=today)
        assert signals[name]["signal"] == "SELL"

    def test_empty_container_list(self) -> None:
        assert get_event_signals([]) == {}

    def test_multiple_containers_independent_signals(self) -> None:
        ev = _first_event()
        today = ev["start"] - timedelta(days=5)
        name_match = f"Capsule {ev['keywords'][0].title()}"
        name_no_match = "Random Case"
        signals = get_event_signals([name_match, name_no_match], today=today)
        assert name_match in signals
        assert name_no_match not in signals


# ─── get_upcoming_events ─────────────────────────────────────────────────────


class TestGetUpcomingEvents:
    def test_returns_only_within_lookahead(self) -> None:
        ev = _first_event()
        today = ev["start"] - timedelta(days=10)
        upcoming = get_upcoming_events(today=today, lookahead_days=30)
        names = [e["name"] for e in upcoming]
        assert ev["name"] in names

    def test_excludes_events_beyond_lookahead(self) -> None:
        ev = _first_event()
        today = ev["start"] - timedelta(days=100)
        upcoming = get_upcoming_events(today=today, lookahead_days=30)
        names = [e["name"] for e in upcoming]
        assert ev["name"] not in names

    def test_live_event_included_with_zero_days(self) -> None:
        ev = _first_event()
        today = ev["start"] + timedelta(days=1)
        upcoming = get_upcoming_events(today=today, lookahead_days=60)
        live_events = [e for e in upcoming if e.get("live")]
        # At least this event should be live (if today is within it)
        if ev["start"] <= today <= ev["end"]:
            assert any(e["name"] == ev["name"] for e in live_events)

    def test_sorted_by_start_date(self) -> None:
        # Use a date guaranteed to have multiple events upcoming
        upcoming = get_upcoming_events(today=date(2025, 1, 1), lookahead_days=365)
        starts = [e["start"] for e in upcoming]
        assert starts == sorted(starts)

    def test_empty_when_no_events_in_window(self) -> None:
        # Far future — no hardcoded events exist
        upcoming = get_upcoming_events(today=date(2040, 1, 1), lookahead_days=30)
        assert upcoming == []


# ─── is_calendar_stale ───────────────────────────────────────────────────────


class TestIsCalendarStale:
    def test_returns_false_when_recent_events_exist(self) -> None:
        # Most recent event ends today → not stale
        last_ev = max(EVENTS, key=lambda e: e["end"])
        today = last_ev["end"]
        assert is_calendar_stale(stale_days=180, today=today) is False

    def test_returns_true_when_all_events_expired_beyond_threshold(self) -> None:
        # All events ended before 2020 — trivially stale
        assert is_calendar_stale(stale_days=180, today=date(2030, 1, 1)) is True

    def test_boundary_exactly_on_stale_threshold(self) -> None:
        # today = most_recent_end + stale_days → days == stale_days → NOT stale (boundary inclusive)
        last_ev = max(EVENTS, key=lambda e: e["end"])
        today = last_ev["end"] + timedelta(days=180)
        # (today - end).days == 180 which is NOT > stale_days → False
        assert is_calendar_stale(stale_days=180, today=today) is False

    def test_one_day_past_threshold_is_stale(self) -> None:
        last_ev = max(EVENTS, key=lambda e: e["end"])
        today = last_ev["end"] + timedelta(days=181)
        assert is_calendar_stale(stale_days=180, today=today) is True

    def test_empty_events_list_always_stale(self) -> None:
        # Monkey-patch: temporarily empty the EVENTS list
        import engine.event_calendar as ec

        original = ec.EVENTS[:]
        try:
            ec.EVENTS.clear()
            assert is_calendar_stale(today=date(2026, 1, 1)) is True
        finally:
            ec.EVENTS.extend(original)

    def test_custom_stale_days_parameter(self) -> None:
        # With stale_days=365, a calendar ending 200 days ago is not stale
        last_ev = max(EVENTS, key=lambda e: e["end"])
        today = last_ev["end"] + timedelta(days=200)
        assert is_calendar_stale(stale_days=365, today=today) is False


# ─── get_event_impact ────────────────────────────────────────────────────────


def _make_history(
    start_date: date,
    days: int,
    base_price: float = 1000.0,
) -> list:
    """Generate a synthetic price history list of (datetime, price) tuples."""
    from datetime import datetime

    result = []
    for i in range(days):
        ts = datetime(
            start_date.year,
            start_date.month,
            start_date.day,
        ) + timedelta(days=i)
        result.append((ts, base_price))
    return result


def _past_event() -> dict:
    """Return the earliest past event from EVENTS for deterministic testing."""
    past = [ev for ev in EVENTS if ev["end"] < date.today()]
    if not past:
        raise RuntimeError("No past events in EVENTS — cannot run get_event_impact tests")
    return min(past, key=lambda e: e["start"])


class TestGetEventImpact:
    def test_empty_price_history_returns_empty(self) -> None:
        results = get_event_impact("Katowice 2025 Capsule", [])
        assert results == []

    def test_empty_container_name_returns_empty(self) -> None:
        ev = _past_event()
        history = _make_history(ev["start"] - timedelta(days=60), 120)
        results = get_event_impact("", history)
        assert results == []

    def test_no_matching_event_returns_empty(self) -> None:
        ev = _past_event()
        history = _make_history(ev["start"] - timedelta(days=60), 120)
        results = get_event_impact("Completely Unrelated Container", history)
        assert results == []

    def test_matching_event_returns_one_record(self) -> None:
        ev = _past_event()
        container_name = f"Capsule {ev['keywords'][0].title()}"
        # Provide price history covering ±30d window
        history_start = ev["start"] - timedelta(days=35)
        history = _make_history(history_start, 100, base_price=1500.0)
        results = get_event_impact(container_name, history)
        assert len(results) >= 1
        record = results[0]
        assert record["event_name"] == ev["name"]
        assert record["start_date"] == ev["start"].isoformat()
        assert record["end_date"] == ev["end"].isoformat()

    def test_record_has_required_keys(self) -> None:
        ev = _past_event()
        container_name = f"Capsule {ev['keywords'][0].title()}"
        history_start = ev["start"] - timedelta(days=35)
        history = _make_history(history_start, 100, base_price=2000.0)
        results = get_event_impact(container_name, history)
        assert results, "Expected at least one impact record"
        record = results[0]
        for key in (
            "event_name",
            "event_type",
            "start_date",
            "end_date",
            "price_at_minus30",
            "price_at_start",
            "price_at_end",
            "price_at_plus30",
            "pct_change_pre",
            "pct_change_post",
            "pct_change_window",
        ):
            assert key in record, f"Missing key: {key}"

    def test_pct_change_zero_when_flat_price(self) -> None:
        ev = _past_event()
        container_name = f"Capsule {ev['keywords'][0].title()}"
        history_start = ev["start"] - timedelta(days=35)
        history = _make_history(history_start, 120, base_price=1000.0)
        results = get_event_impact(container_name, history)
        assert results, "Expected at least one impact record"
        record = results[0]
        # All prices are 1000, so pct changes should be 0.0 or None
        if record["pct_change_window"] is not None:
            assert record["pct_change_window"] == 0.0

    def test_none_when_price_history_too_short(self) -> None:
        ev = _past_event()
        container_name = f"Capsule {ev['keywords'][0].title()}"
        # History starts after the event — minus30 will have no data
        history_start = ev["end"] + timedelta(days=5)
        history = _make_history(history_start, 30, base_price=1000.0)
        results = get_event_impact(container_name, history)
        if results:
            record = results[0]
            # price_at_minus30 should be None (no data before event start)
            assert record["price_at_minus30"] is None
            assert record["pct_change_pre"] is None
            assert record["pct_change_window"] is None

    def test_future_events_excluded(self) -> None:
        ev = _past_event()
        container_name = f"Capsule {ev['keywords'][0].title()}"
        history = _make_history(date(2020, 1, 1), 365, base_price=1000.0)
        results = get_event_impact(container_name, history)
        # All results must be past events
        today = date.today()
        for record in results:
            from datetime import date as _date

            end = _date.fromisoformat(record["end_date"])
            assert end < today, f"Future event included: {record['event_name']}"
