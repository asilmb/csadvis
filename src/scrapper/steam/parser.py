"""
Pure parsing functions for Steam Market API responses (PV-48).

No I/O, no side-effects. Each function takes raw data and returns typed dicts.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any


def _parse_steam_price(s: str) -> float:
    """Parse Steam price string to float rounded to 2 decimal places.

    Handles:  "$1.25"  "1 234,56 ₸"  "1.25"  "1,234.56"
    Returns 0.0 on failure.

    Currency symbols, spaces, and non-numeric chars are stripped automatically.
    Result is rounded to 2dp to avoid IEEE-754 noise (e.g. 0.10000000000000001).
    Full Decimal precision will be introduced in the domain layer (Task #18/#23).
    """
    if not s:
        return 0.0
    # Strip all non-numeric except comma and dot
    cleaned = re.sub(r"[^\d.,]", "", s.strip())
    if not cleaned:
        return 0.0
    # Determine separator convention:
    # If both comma and dot present, the last one is the decimal separator.
    if "," in cleaned and "." in cleaned:
        if cleaned.rfind(".") > cleaned.rfind(","):
            cleaned = cleaned.replace(",", "")  # 1,234.56 -> 1234.56
        else:
            cleaned = cleaned.replace(".", "").replace(",", ".")  # 1.234,56 -> 1234.56
    elif "," in cleaned:
        # Might be decimal comma (European) or thousands separator.
        # If only one comma and ≤2 digits after it, treat as decimal.
        parts = cleaned.split(",")
        if len(parts) == 2 and len(parts[1]) <= 2:
            cleaned = cleaned.replace(",", ".")
        else:
            cleaned = cleaned.replace(",", "")
    try:
        return round(float(cleaned), 2)
    except ValueError:
        return 0.0


def parse_history_response(data: dict) -> list[dict]:
    """
    Parse /market/pricehistory/ JSON into a list of price rows.

    Returns list of:
        {"date": datetime, "price": float, "volume": int}
    sorted oldest-first.
    """
    rows: list[dict[str, Any]] = []
    for entry in data.get("prices", []):
        # entry format: ["Mar 26 2024 01: +0", 500.0, "10"]
        try:
            parts = entry[0].split()
            date_str = f"{parts[0]} {parts[1]} {parts[2]}"  # "Mar 26 2024"
            dt = datetime.strptime(date_str, "%b %d %Y")
            price = round(float(entry[1]), 2)
            volume = int(entry[2]) if entry[2] else 0
            rows.append({"date": dt, "price": price, "volume": volume})
        except Exception:
            continue
    rows.sort(key=lambda x: x["date"])  # type: ignore[arg-type, return-value]
    return rows


def parse_overview_response(data: dict, market_hash_name: str) -> dict:
    """
    Parse /market/priceoverview/ JSON into a price overview dict.

    Returns:
        {"market_hash_name": str, "median_price": float,
         "lowest_price": float, "volume": int}
    """
    median = _parse_steam_price(data.get("median_price", ""))
    lowest = _parse_steam_price(data.get("lowest_price", ""))
    vol_str = str(data.get("volume", "0")).replace(",", "").replace(" ", "")
    try:
        volume = int(vol_str)
    except ValueError:
        volume = 0
    return {
        "market_hash_name": market_hash_name,
        "median_price": median,
        "lowest_price": lowest,
        "volume": volume,
    }


def parse_nameid_html(html_text: str) -> int | None:
    """
    Extract item_nameid from a Steam Market listing page HTML.

    item_nameid is embedded as: Market_LoadOrderSpread( <id> )
    Returns int or None if not found.
    """
    match = re.search(r"Market_LoadOrderSpread\(\s*(\d+)\s*\)", html_text)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def parse_order_book_response(data: dict) -> dict:
    """
    Parse /market/itemordershistogram/ JSON into sell/buy order graphs.

    Returns:
        {
            "sell_order_graph": [[price, qty, desc], ...],
            "buy_order_graph":  [[price, qty, desc], ...],
        }
    """

    def _parse_graph(raw: list) -> list:
        parsed = []
        for entry in raw:
            try:
                price = _parse_steam_price(str(entry[0]))
                qty = int(entry[1])
                desc = str(entry[2]) if len(entry) > 2 else ""
                parsed.append([price, qty, desc])
            except (IndexError, TypeError, ValueError):
                continue
        return parsed

    return {
        "sell_order_graph": _parse_graph(data.get("sell_order_graph") or []),
        "buy_order_graph": _parse_graph(data.get("buy_order_graph") or []),
    }
