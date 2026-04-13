"""
Steam URL generator for Action Links (PV-20).

Pure functions — no I/O, no Dash imports.  Import `item_link` for ready-made
html.A components when building Dash layouts.

Public API:
    get_market_url(market_hash_name, appid=730) → str
    get_inventory_url(steam_id, asset_id)        → str | None
    get_inspect_url(raw_link)                    → str | None
    item_link(market_hash_name, label, **style)  → dash html.A
"""

from __future__ import annotations

from urllib.parse import quote

# ─── URL generators ───────────────────────────────────────────────────────────

_MARKET_BASE = "https://steamcommunity.com/market/listings"
_INVENTORY_BASE = "https://steamcommunity.com/profiles"
_INSPECT_PREFIXES = ("steam://rungame/", "csgo_econ_action_preview")


def get_market_url(market_hash_name: str, appid: int = 730) -> str:
    """
    Return a Steam Market listing URL for the given item name and app ID.

    Special characters (spaces, &, #, %, quotes, etc.) are percent-encoded.
    Empty market_hash_name returns the generic CS2 market URL.

    Examples:
        "AK-47 | Redline (Field-Tested)"
        → "https://steamcommunity.com/market/listings/730/AK-47%20%7C%20Redline%20%28Field-Tested%29"
    """
    if not market_hash_name or not market_hash_name.strip():
        return f"{_MARKET_BASE}/{appid}"
    encoded = quote(market_hash_name.strip(), safe="")
    return f"{_MARKET_BASE}/{appid}/{encoded}"


def get_inventory_url(steam_id: str | int, asset_id: str | int | None = None) -> str | None:
    """
    Return a Steam inventory URL for a given Steam64 ID.

    When asset_id is provided, the URL includes a fragment anchor (#assetID)
    that Steam's UI uses to highlight the specific item.

    Returns None when steam_id is falsy (empty / zero).
    """
    sid = str(steam_id).strip() if steam_id else ""
    if not sid or sid == "0":
        return None
    base = f"{_INVENTORY_BASE}/{sid}/inventory/#730_2"
    if asset_id is not None:
        aid = str(asset_id).strip()
        if aid and aid != "0":
            return f"{base}_{aid}"
    return base


def get_inspect_url(raw_link: str | None) -> str | None:
    """
    Validate and return a Steam inspect link as-is.

    Accepts:
      - steam://rungame/730/... format (desktop client)
      - csgo_econ_action_preview ... format (raw preview)

    Returns None for empty, None, or unrecognised formats.
    The link is returned as-is (not re-encoded) because inspect links
    contain pre-encoded parameters.
    """
    if not raw_link:
        return None
    link = raw_link.strip()
    if any(link.startswith(p) for p in _INSPECT_PREFIXES):
        return link
    return None


# ─── Dash component helper ────────────────────────────────────────────────────

def item_link(
    market_hash_name: str,
    label: str | None = None,
    color: str = "#e8e8e8",
    font_size: str = "12px",
    appid: int = 730,
):
    """
    Return a Dash html.A hyperlink to the Steam Market listing.

    Parameters
    ----------
    market_hash_name: Item name — used both as URL key and default label.
    label:            Display text; defaults to market_hash_name.
    color:            CSS color for the link text.
    font_size:        CSS font-size.
    appid:            Steam App ID (default 730 = CS2).
    """
    from dash import html  # lazy import — keeps module importable without Dash

    url = get_market_url(market_hash_name, appid=appid)
    return html.A(
        label if label is not None else market_hash_name,
        href=url,
        target="_blank",
        rel="noopener noreferrer",
        style={
            "color": color,
            "fontSize": font_size,
            "textDecoration": "none",
        },
    )
