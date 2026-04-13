"""
Design tokens for the CS2 Market Analytics dashboard.

Central source of truth for colours and common style dicts.
Import COLORS and STYLES in frontend modules instead of repeating inline dicts.

Usage:
    from ui.theme import COLORS, STYLES, verdict_color
"""

from __future__ import annotations

# ─── Colour palette ────────────────────────────────────────────────────────────

COLORS: dict[str, str] = {
    # Backgrounds
    "bg": "#0f1923",
    "bg2": "#1a2433",
    "border": "#2a3a4a",
    # Text
    "text": "#c7d5e0",
    "muted": "#8f98a0",
    # Accent
    "gold": "#ffd700",
    "blue": "#66c0f4",
    # Signal colours
    "green": "#00c853",  # BUY
    "yellow": "#ffd600",  # HOLD / LEAN BUY boundary
    "orange": "#ff9800",  # LEAN SELL
    "red": "#eb4b4b",  # SELL / loss
    # Extra
    "lean_buy_green": "#4caf50",  # slightly muted green for LEAN BUY
}

# ─── Verdict → display colour mapping ─────────────────────────────────────────
# Maps investment signal verdicts to their display colour.
# Used across container list, detail card, inventory table, and badges.

_VERDICT_COLOR: dict[str, str] = {
    "BUY": COLORS["green"],
    "LEAN BUY": COLORS["lean_buy_green"],
    "HOLD": COLORS["yellow"],
    "LEAN SELL": COLORS["orange"],
    "SELL": COLORS["red"],
    "NO DATA": COLORS["border"],
}


def verdict_color(verdict: str) -> str:
    """Return the hex colour string for a given verdict label."""
    return _VERDICT_COLOR.get(verdict, COLORS["muted"])


# ─── Common style dicts ────────────────────────────────────────────────────────

STYLES: dict[str, dict] = {
    "card": {
        "backgroundColor": COLORS["bg2"],
        "border": f"1px solid {COLORS['border']}",
    },
    "section_label": {
        "color": COLORS["muted"],
        "fontSize": "10px",
        "letterSpacing": "1.5px",
        "marginBottom": "8px",
    },
    "table_header_cell": {
        "color": COLORS["muted"],
        "fontSize": "11px",
        "backgroundColor": COLORS["bg2"],
        "border": "none",
        "paddingBottom": "6px",
        "whiteSpace": "nowrap",
    },
}
