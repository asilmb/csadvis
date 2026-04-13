"""Tests for engine/event_loader.py — YAML event calendar loader."""

from __future__ import annotations

import textwrap
from datetime import date
from pathlib import Path

import pytest

from domain.event_loader import load_events

# ─── Helpers ─────────────────────────────────────────────────────────────────


def _write_yaml(tmp_path: Path, content: str) -> Path:
    """Write YAML content to a temp file and return its path."""
    f = tmp_path / "events.yaml"
    f.write_text(textwrap.dedent(content), encoding="utf-8")
    return f


# ─── Valid YAML ───────────────────────────────────────────────────────────────


class TestLoadEventsValid:
    def test_valid_list_format(self, tmp_path: Path) -> None:
        path = _write_yaml(
            tmp_path,
            """
            - name: "Test Event"
              start: "2025-01-01"
              end: "2025-01-10"
              keywords:
                - "test event"
              type: major
            """,
        )
        events = load_events(path)
        assert len(events) == 1
        ev = events[0]
        assert ev["name"] == "Test Event"
        assert ev["start"] == date(2025, 1, 1)
        assert ev["end"] == date(2025, 1, 10)
        assert ev["keywords"] == ["test event"]
        assert ev["type"] == "major"

    def test_valid_dict_format_with_events_key(self, tmp_path: Path) -> None:
        path = _write_yaml(
            tmp_path,
            """
            events:
              - name: "Dict Format Event"
                start: "2025-03-01"
                end: "2025-03-15"
                keywords:
                  - "dict format"
                type: iem
            """,
        )
        events = load_events(path)
        assert len(events) == 1
        assert events[0]["name"] == "Dict Format Event"

    def test_empty_list_is_valid(self, tmp_path: Path) -> None:
        path = _write_yaml(tmp_path, "events: []\n")
        events = load_events(path)
        assert events == []

    def test_multiple_keywords(self, tmp_path: Path) -> None:
        path = _write_yaml(
            tmp_path,
            """
            - name: "Multi Key Event"
              start: "2025-05-01"
              end: "2025-05-10"
              keywords:
                - "key one"
                - "key two"
                - "key three"
              type: premier
            """,
        )
        events = load_events(path)
        assert events[0]["keywords"] == ["key one", "key two", "key three"]

    def test_dates_returned_as_date_objects(self, tmp_path: Path) -> None:
        path = _write_yaml(
            tmp_path,
            """
            - name: "Date Type Test"
              start: "2026-06-01"
              end: "2026-06-08"
              keywords: ["date test"]
              type: major
            """,
        )
        events = load_events(path)
        assert isinstance(events[0]["start"], date)
        assert isinstance(events[0]["end"], date)

    def test_roundtrip_matches_hardcoded_events(self, tmp_path: Path) -> None:
        """All current events.yaml entries should load without error."""
        yaml_path = Path(__file__).resolve().parent.parent / "data" / "events.yaml"
        if not yaml_path.exists():
            pytest.skip("data/events.yaml not found")
        events = load_events(yaml_path)
        assert len(events) >= 10  # we have 11 events currently
        for ev in events:
            assert isinstance(ev["start"], date)
            assert isinstance(ev["end"], date)
            assert ev["end"] >= ev["start"]


# ─── File not found ───────────────────────────────────────────────────────────


class TestLoadEventsFileNotFound:
    def test_raises_file_not_found(self, tmp_path: Path) -> None:
        missing = tmp_path / "nonexistent.yaml"
        with pytest.raises(FileNotFoundError):
            load_events(missing)


# ─── Missing required fields ─────────────────────────────────────────────────


class TestLoadEventsMissingFields:
    def test_missing_name_raises(self, tmp_path: Path) -> None:
        path = _write_yaml(
            tmp_path,
            """
            - start: "2025-01-01"
              end: "2025-01-10"
              keywords: ["test"]
              type: major
            """,
        )
        with pytest.raises(ValueError, match=r"missing required field.*name"):
            load_events(path)

    def test_missing_start_raises(self, tmp_path: Path) -> None:
        path = _write_yaml(
            tmp_path,
            """
            - name: "No Start"
              end: "2025-01-10"
              keywords: ["test"]
              type: major
            """,
        )
        with pytest.raises(ValueError, match=r"missing required field.*start"):
            load_events(path)

    def test_missing_end_raises(self, tmp_path: Path) -> None:
        path = _write_yaml(
            tmp_path,
            """
            - name: "No End"
              start: "2025-01-01"
              keywords: ["test"]
              type: major
            """,
        )
        with pytest.raises(ValueError, match=r"missing required field.*end"):
            load_events(path)

    def test_missing_keywords_raises(self, tmp_path: Path) -> None:
        path = _write_yaml(
            tmp_path,
            """
            - name: "No Keywords"
              start: "2025-01-01"
              end: "2025-01-10"
              type: major
            """,
        )
        with pytest.raises(ValueError, match=r"missing required field.*keywords"):
            load_events(path)

    def test_missing_type_raises(self, tmp_path: Path) -> None:
        path = _write_yaml(
            tmp_path,
            """
            - name: "No Type"
              start: "2025-01-01"
              end: "2025-01-10"
              keywords: ["test"]
            """,
        )
        with pytest.raises(ValueError, match=r"missing required field.*type"):
            load_events(path)


# ─── Invalid dates ────────────────────────────────────────────────────────────


class TestLoadEventsInvalidDates:
    def test_invalid_start_date_string_raises(self, tmp_path: Path) -> None:
        path = _write_yaml(
            tmp_path,
            """
            - name: "Bad Date"
              start: "not-a-date"
              end: "2025-01-10"
              keywords: ["test"]
              type: major
            """,
        )
        with pytest.raises(ValueError, match=r"invalid.*start.*date"):
            load_events(path)

    def test_invalid_end_date_string_raises(self, tmp_path: Path) -> None:
        path = _write_yaml(
            tmp_path,
            """
            - name: "Bad End Date"
              start: "2025-01-01"
              end: "2025-13-99"
              keywords: ["test"]
              type: major
            """,
        )
        with pytest.raises(ValueError, match=r"invalid.*end.*date"):
            load_events(path)

    def test_end_before_start_raises(self, tmp_path: Path) -> None:
        path = _write_yaml(
            tmp_path,
            """
            - name: "End Before Start"
              start: "2025-02-01"
              end: "2025-01-01"
              keywords: ["test"]
              type: major
            """,
        )
        with pytest.raises(ValueError, match=r"'end' date.*before.*'start'"):
            load_events(path)
