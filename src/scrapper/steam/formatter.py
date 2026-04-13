"""
Name formatting utilities for Steam Market API (PV-48).

Handles the gap between DB-stored names and API-expected names.
"""

from __future__ import annotations

import re


class InvalidHashNameError(Exception):
    """Raised when Steam returns 404 or 500 for a market_hash_name (invalid/unlisted item)."""

    def __init__(self, name: str, status: int) -> None:
        super().__init__(f"Invalid Hash Name [HTTP {status}]: {name!r}")
        self.name = name
        self.status = status


def normalize_market_hash_name(raw_name: str) -> str:
    """
    Build the market_hash_name to send to Steam API (PV-50).

    Used ONLY for HTTP requests — the database stores the original name.
    Idempotent — safe to call on already-normalized strings.

    Rules (applied in order):
      1. Strip leading/trailing whitespace.
      2. Terminals: prepend "Sealed " if name ends with " Terminal" and
         does not already start with "Sealed ".
         (Terminals ARE stored with the "Sealed" prefix in the DB.)
      3. Holo/Foil: replace "(Holo/Foil)" → "(Holo-Foil)" via regex.
         Steam Market URLs use a dash; the slash would be URL-encoded to
         %2F and rejected. The DB retains the original "(Holo/Foil)" form.
    """
    name = raw_name.strip()
    # Rule 1: Armory Terminal prefix
    if name.endswith(" Terminal") and not name.startswith("Sealed "):
        name = "Sealed " + name
    # Rule 2: Holo/Foil → Holo-Foil (API only; DB keeps original slash form)
    name = re.sub(r"\(Holo/Foil\)", "(Holo-Foil)", name)
    return name


def to_api_name(display_name: str) -> str:
    """Return the market_hash_name to send to Steam API. Delegates to normalize_market_hash_name."""
    return normalize_market_hash_name(display_name)
