"""
CS2 Event Loader — reads and validates the YAML event calendar.

Pure function: load_events(path) reads a YAML file and returns a list of validated
event dicts compatible with the engine/event_calendar.py EVENTS format.

Raises:
    FileNotFoundError  — if the YAML file does not exist
    ValueError         — if any event entry is missing required fields or has an invalid date

Schema per event:
    name:     str   — full tournament name
    start:    date  — YYYY-MM-DD string or date object
    end:      date  — YYYY-MM-DD string or date object
    keywords: list[str]
    type:     str
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_REQUIRED_FIELDS = ("name", "start", "end", "keywords", "type")


def load_events(path: str | Path) -> list[dict]:
    """
    Load and validate events from a YAML file.

    Args:
        path: Path to the YAML events file.

    Returns:
        List of validated event dicts with 'start' and 'end' as date objects.

    Raises:
        FileNotFoundError: If the YAML file does not exist.
        ValueError: If any event entry is missing required fields or has unparseable dates.
    """
    import yaml  # lazy import — pyyaml may not be installed in all envs

    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Events YAML not found: {path}")

    raw = path.read_text(encoding="utf-8")
    try:
        data = yaml.safe_load(raw)
    except yaml.YAMLError as exc:
        raise ValueError(f"YAML parse error in {path}: {exc}") from exc

    if data is None:
        return []

    # Support both top-level list and {events: [...]} structure
    if isinstance(data, dict):
        raw_events: Any = data.get("events", [])
    elif isinstance(data, list):
        raw_events = data
    else:
        raise ValueError(f"YAML root must be a list or a dict with 'events' key, got {type(data)}")

    if not isinstance(raw_events, list):
        raise ValueError(f"'events' must be a list, got {type(raw_events)}")

    events: list[dict] = []
    for i, entry in enumerate(raw_events):
        if not isinstance(entry, dict):
            raise ValueError(f"Event #{i} is not a dict: {entry!r}")

        for field in _REQUIRED_FIELDS:
            if field not in entry:
                raise ValueError(
                    f"Event #{i} ('{entry.get('name', '?')}') missing required field: '{field}'"
                )

        # Validate and normalise keywords
        keywords = entry["keywords"]
        if not isinstance(keywords, list) or not all(isinstance(k, str) for k in keywords):
            raise ValueError(
                f"Event #{i} ('{entry['name']}'): 'keywords' must be a list of strings"
            )

        # Parse dates
        start = _parse_date(entry["start"], i, entry.get("name", "?"), "start")
        end = _parse_date(entry["end"], i, entry.get("name", "?"), "end")

        if end < start:
            raise ValueError(
                f"Event #{i} ('{entry['name']}'): 'end' date {end} is before 'start' date {start}"
            )

        events.append(
            {
                "name": str(entry["name"]),
                "start": start,
                "end": end,
                "keywords": [str(k) for k in keywords],
                "type": str(entry["type"]),
            }
        )

    return events


def _parse_date(value: Any, idx: int, name: str, field: str) -> date:
    """Parse a date value (string or date object) and return a date.

    Raises ValueError on failure.
    """
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(
                f"Event #{idx} ('{name}'): invalid '{field}' date '{value}' — expected YYYY-MM-DD"
            ) from exc
    raise ValueError(
        f"Event #{idx} ('{name}'): '{field}' must be a string (YYYY-MM-DD) or date, "
        f"got {type(value)}"
    )
