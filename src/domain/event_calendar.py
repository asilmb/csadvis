"""
CS2 Event Calendar — tournament-driven trade signals.

Logic:
  BUY_SIGNAL  : N days before a major → buy capsules of that tournament
  SELL_SIGNAL : N days after a major ends → sell if capsule price is elevated

Capsule names are matched by substring against DimContainer.container_name.

Event source (F-07): events are loaded from data/events.yaml at import time.
If the YAML file is missing or invalid, the module falls back to the hardcoded
_HARDCODED_EVENTS list below and logs a warning. No crash.

Signal window:
  Pre-event  : 30 days before start → BUY capsules
  Post-event :  7 days after end    → SELL if overpriced
  Active     : event is live now    → HOLD (peak demand, don't buy)
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta

logger = logging.getLogger(__name__)

# ─── Hardcoded fallback tournament calendar ───────────────────────────────────
# Used when data/events.yaml is missing or invalid.
# To edit events: update data/events.yaml instead of this list.

_HARDCODED_EVENTS: list[dict] = [
    # ── 2025 ──────────────────────────────────────────────────────────────────
    {
        "name": "IEM Katowice 2025",
        "start": date(2025, 1, 28),
        "end": date(2025, 2, 9),
        "keywords": ["katowice 2025", "iem katowice 2025"],
        "type": "iem",
    },
    {
        "name": "BLAST Premier Spring Groups 2025",
        "start": date(2025, 2, 20),
        "end": date(2025, 2, 23),
        "keywords": ["blast spring groups 2025"],
        "type": "premier",
    },
    # ESL Pro League removed: no Steam capsules are sold for ESL Pro League events.
    # Entries here generated false BUY signals with no tradeable capsule to buy.
    {
        "name": "PGL Bucharest Major 2025",
        "start": date(2025, 3, 17),
        "end": date(2025, 3, 30),
        "keywords": ["bucharest", "pgl 2025"],
        "type": "major",
    },
    {
        "name": "PGL Austin Major 2025",
        "start": date(2025, 6, 9),
        "end": date(2025, 6, 22),
        "keywords": ["austin 2025", "pgl austin 2025"],
        "type": "major",
    },
    {
        "name": "BLAST Premier Spring Final 2025",
        "start": date(2025, 6, 1),
        "end": date(2025, 6, 8),
        "keywords": ["blast premier spring final 2025"],
        "type": "premier",
    },
    {
        "name": "IEM Cologne 2025",
        "start": date(2025, 7, 1),
        "end": date(2025, 7, 13),
        "keywords": ["cologne 2025", "iem cologne 2025"],
        "type": "iem",
    },
    # ── 2026 ──────────────────────────────────────────────────────────────────
    # 2026 dates: IEM/BLAST are estimates. PGL Major 2026 confirmed 11–24 May.
    {
        "name": "IEM Katowice 2026",
        "start": date(2026, 1, 27),
        "end": date(2026, 2, 8),
        "keywords": ["katowice 2026", "iem katowice 2026"],
        "type": "iem",
    },
    # ESL Pro League Season 23 removed: no Steam capsules for ESL Pro League.
    {
        "name": "BLAST Premier Spring Groups 2026",
        "start": date(2026, 2, 19),
        "end": date(2026, 2, 22),
        "keywords": ["blast spring groups 2026"],
        "type": "premier",
    },
    {
        "name": "PGL Major 2026",
        "start": date(2026, 5, 11),
        "end": date(2026, 5, 24),
        "keywords": ["pgl major 2026", "pgl 2026"],
        "type": "major",
    },
    {
        "name": "BLAST Premier Spring Final 2026",
        "start": date(2026, 6, 7),
        "end": date(2026, 6, 14),
        "keywords": ["blast premier spring final 2026"],
        "type": "premier",
    },
    {
        "name": "IEM Cologne 2026",
        "start": date(2026, 7, 7),
        "end": date(2026, 7, 19),
        "keywords": ["cologne 2026", "iem cologne 2026"],
        "type": "iem",
    },
]

# ─── YAML lazy-load with hardcoded fallback ───────────────────────────────────


def _load_events_or_fallback() -> list[dict]:
    """
    Load events from the YAML file configured in settings.events_yaml_path.

    Falls back to _HARDCODED_EVENTS if:
    - The file does not exist (FileNotFoundError)
    - The file is invalid YAML or fails schema validation (ValueError)
    - pyyaml is not installed (ImportError)

    No crash in any case — always returns a usable list.
    """
    try:
        from config import settings
        from domain.event_loader import load_events

        events = load_events(settings.events_yaml_path)
        logger.debug(
            "EventCalendar: loaded %d events from %s", len(events), settings.events_yaml_path
        )
        return events
    except FileNotFoundError:
        logger.debug(
            "EventCalendar: %s not found — using hardcoded event list.",
            "data/events.yaml",
        )
        return _HARDCODED_EVENTS
    except (ValueError, ImportError, Exception) as exc:
        logger.warning(
            "EventCalendar: failed to load YAML events (%s) — falling back to hardcoded list.",
            exc,
        )
        return _HARDCODED_EVENTS


# Public EVENTS list — loaded from YAML on import; falls back to hardcoded list on error.
EVENTS: list[dict] = _load_events_or_fallback()

# Signal windows
_PRE_EVENT_DAYS = 30  # buy capsules this many days before event starts
_POST_EVENT_DAYS = 7  # sell window opens this many days after event ends


# ─── Signal computation ───────────────────────────────────────────────────────


def _matches(container_name: str, keywords: list[str]) -> bool:
    name_lower = container_name.lower()
    return any(kw.lower() in name_lower for kw in keywords)


def get_event_signals(
    container_names: list[str],
    today: date | None = None,
) -> dict[str, dict]:
    """
    For each container name, return the active event signal (if any).

    Returns {container_name: {event, signal, days_to_event, message}}
    Only containers that match a keyword get an entry.

    signal values:
        "BUY"  — pre-event window, accumulate capsules
        "HOLD" — event is live (peak demand, don't buy now)
        "SELL" — post-event window, sell if elevated
        None   — no active signal
    """
    today = today or date.today()
    result: dict[str, dict] = {}

    for name in container_names:
        for ev in EVENTS:
            if not _matches(name, ev["keywords"]):
                continue

            start = ev["start"]
            end = ev["end"]
            days_to_start = (start - today).days
            days_since_end = (today - end).days

            if 0 <= days_to_start <= _PRE_EVENT_DAYS:
                signal = "BUY"
                msg = (
                    f"Мажор «{ev['name']}» через {days_to_start} дн. "
                    f"({start.strftime('%d %b')} – {end.strftime('%d %b')}). "
                    "Покупай капсулы этого турнира до начала — спрос растёт."
                )
            elif start <= today <= end:
                signal = "HOLD"
                msg = (
                    f"Мажор «{ev['name']}» идёт прямо сейчас "
                    f"(до {end.strftime('%d %b')}). "
                    "Не покупай на пике — подожди завершения."
                )
            elif 0 <= days_since_end <= _POST_EVENT_DAYS:
                signal = "SELL"
                msg = (
                    f"Мажор «{ev['name']}» завершился {days_since_end} дн. назад. "
                    "Если капсулы переоценены — хорошее время выставить на продажу."
                )
            else:
                continue  # outside any signal window

            result[name] = {
                "event": ev["name"],
                "event_type": ev["type"],
                "signal": signal,
                "start": start.isoformat(),
                "end": end.isoformat(),
                "days_to_start": days_to_start if days_to_start >= 0 else None,
                "days_since_end": days_since_end if days_since_end >= 0 else None,
                "message": msg,
            }
            break  # one signal per container (first matching event wins)

    return result


def is_calendar_stale(stale_days: int = 180, today: date | None = None) -> bool:
    """Return True if all events ended more than stale_days ago (calendar needs updating)."""
    today = today or date.today()
    if not EVENTS:
        return True
    most_recent_end = max(ev["end"] for ev in EVENTS)
    return (today - most_recent_end).days > stale_days


def get_upcoming_events(today: date | None = None, lookahead_days: int = 60) -> list[dict]:
    """Return events starting within the next N days, sorted by start date."""
    today = today or date.today()
    upcoming = []
    for ev in EVENTS:
        days_to = (ev["start"] - today).days
        if 0 <= days_to <= lookahead_days:
            upcoming.append({**ev, "days_to_start": days_to})
        elif ev["start"] <= today <= ev["end"]:
            upcoming.append({**ev, "days_to_start": 0, "live": True})
    upcoming.sort(key=lambda e: e["start"])
    if not upcoming:
        logger.warning(
            "EventCalendar: no upcoming events in the next %d days (today=%s). "
            "Event-driven signals are inactive. Update EVENTS list in event_calendar.py.",
            lookahead_days,
            today,
        )
    return upcoming


# ─── Event impact analysis ────────────────────────────────────────────────────

_IMPACT_WINDOW_DAYS = 30  # days before/after event to measure price change


def _price_at_date(
    price_history: list[tuple[datetime, float]],
    target: date,
) -> float | None:
    """Return the closest price on or before target date, or None if no data available."""
    best: tuple[datetime, float] | None = None
    for ts, price in price_history:
        ts_date = ts.date() if isinstance(ts, datetime) else ts
        best_date = (
            best[0].date()
            if best is not None and isinstance(best[0], datetime)
            else (best[0] if best is not None else None)
        )
        if ts_date <= target and (best is None or ts_date > best_date):
            best = (ts, price)
    return best[1] if best is not None else None


def get_event_impact(
    container_name: str,
    price_history: list[tuple[datetime, float]],
) -> list[dict]:
    """
    For a given container and its price history, return impact records for all
    past events that match the container name.

    Each record contains:
        event_name      : str
        event_type      : str
        start_date      : str (ISO)
        end_date        : str (ISO)
        price_at_minus30: float | None  — price 30d before event start
        price_at_start  : float | None  — price on event start date
        price_at_end    : float | None  — price on event end date
        price_at_plus30 : float | None  — price 30d after event end
        pct_change_pre  : float | None  — % change from -30d to event start
        pct_change_post : float | None  — % change from event end to +30d
        pct_change_window: float | None — % change from -30d to +30d (full window)

    Pure function — no DB calls. price_history is a list of (datetime, price) tuples.
    Only events whose keywords match the container_name are included.
    Only past events (end date < today) are included to avoid incomplete windows.
    """
    if not price_history or not container_name:
        return []

    today = date.today()
    results: list[dict] = []

    for ev in EVENTS:
        if not _matches(container_name, ev["keywords"]):
            continue
        # Only include events that have already ended (complete impact window)
        if ev["end"] >= today:
            continue

        start = ev["start"]
        end = ev["end"]
        minus30 = start - timedelta(days=_IMPACT_WINDOW_DAYS)
        plus30 = end + timedelta(days=_IMPACT_WINDOW_DAYS)

        p_minus30 = _price_at_date(price_history, minus30)
        p_start = _price_at_date(price_history, start)
        p_end = _price_at_date(price_history, end)
        p_plus30 = _price_at_date(price_history, plus30)

        def _pct(a: float | None, b: float | None) -> float | None:
            if a is None or b is None or a == 0:
                return None
            return round((b - a) / a * 100, 2)

        results.append(
            {
                "event_name": ev["name"],
                "event_type": ev["type"],
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "price_at_minus30": p_minus30,
                "price_at_start": p_start,
                "price_at_end": p_end,
                "price_at_plus30": p_plus30,
                "pct_change_pre": _pct(p_minus30, p_start),
                "pct_change_post": _pct(p_end, p_plus30),
                "pct_change_window": _pct(p_minus30, p_plus30),
            }
        )

    return results
