"""
Steam inventory fetcher.

Endpoint:
  GET https://steamcommunity.com/inventory/{steamid}/730/2
       ?l=english&count=5000

Response structure:
  assets        — list of item instances (assetid, classid, instanceid)
  descriptions  — item metadata (market_hash_name, type, tags, icon_url)

Each asset is matched to a description via (classid, instanceid).
Multiple assets can share the same description = duplicate items in inventory.
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import UTC, datetime

import httpx

logger = logging.getLogger(__name__)

# ─── Trade ban parser ─────────────────────────────────────────────────────────

# Matches: "Tradable After: Jul 31, 2024 (7:00:00) GMT"
_TRADABLE_AFTER_RE = re.compile(
    r"Tradable After:\s+([A-Za-z]+ \d+,\s*\d{4}\s*\(\d+:\d+:\d+\)\s*GMT)",
    re.IGNORECASE,
)
_DATE_FORMAT = "%b %d, %Y %H:%M:%S"


def _extract_trade_unlock_at(desc: dict) -> datetime | None:
    """
    Extract trade-ban expiry timestamp from a Steam CEconItem description dict.

    Steam encodes the expiry in two ways (checked in priority order):
      1. ``app_data.cache_expiration`` — Unix timestamp string inside any entry of
         ``owner_descriptions`` or ``descriptions``.
      2. Text pattern ``"Tradable After: Jul 31, 2024 (7:00:00) GMT"`` in the
         ``value`` field of any description entry (fallback).

    Returns a naive UTC ``datetime`` when the item will become tradable, or
    ``None`` when the item is already freely tradable / no lock data found.
    Past timestamps (item already unlocked) are treated as None.
    """
    now = datetime.now(UTC).replace(tzinfo=None)

    for key in ("owner_descriptions", "descriptions"):
        for entry in desc.get(key, []) or []:
            if not isinstance(entry, dict):
                continue

            # ── Priority 1: numeric Unix timestamp ─────────────────────────
            app_data = entry.get("app_data") or {}
            cache_exp = app_data.get("cache_expiration")
            if cache_exp:
                try:
                    ts = int(cache_exp)
                    dt = datetime.fromtimestamp(ts, tz=UTC).replace(tzinfo=None)
                    if dt > now:
                        return dt
                except (ValueError, OSError, OverflowError):
                    pass

            # ── Priority 2: parse "Tradable After: ..." text ────────────────
            value = entry.get("value") or ""
            m = _TRADABLE_AFTER_RE.search(value)
            if m:
                raw = (
                    m.group(1)
                    .replace("(", "")
                    .replace(")", "")
                    .replace("GMT", "")
                    .strip()
                )
                # Normalise multiple spaces introduced by stripping parentheses
                raw = " ".join(raw.split())
                try:
                    dt = datetime.strptime(raw, _DATE_FORMAT)
                    if dt > now:
                        return dt
                except ValueError:
                    pass

    return None

_INVENTORY_URL = "https://steamcommunity.com/inventory/{steamid}/730/2"
_CDN_BASE = "https://community.akamai.steamstatic.com/economy/image/"


class SteamInventoryClient:
    """
    Fetches and parses a public CS2 Steam inventory.

    No authentication required for public profiles.
    Returns a flat list of InventoryItem dicts ready for recommendation engine.
    """

    def __init__(self) -> None:
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> SteamInventoryClient:
        # Use Steam credentials if available so that non-tradable/AP items are visible.
        cookies: dict[str, str] = {}
        try:
            from infra.steam_credentials import get_login_secure, get_session_id
            ls = get_login_secure()
            si = get_session_id()
            if ls and si:
                cookies = {"steamLoginSecure": ls, "sessionid": si}
                logger.debug("[INV] Using authenticated Steam session for inventory fetch")
        except Exception:
            pass  # credentials unavailable — fall back to public (unauthenticated) API

        self._client = httpx.AsyncClient(
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36",
                "Accept": "application/json",
                "Accept-Language": "en-US,en;q=0.9",
            },
            cookies=cookies,
            timeout=httpx.Timeout(45.0),
            limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
            follow_redirects=True,
        )
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def _fetch_pages(self, steam_id: str) -> list[dict]:
        """Fetch all inventory pages and return raw per-asset item list (no aggregation)."""
        all_items: list[dict] = []
        start_assetid: str | None = None
        page = 0

        while True:
            page += 1
            params: dict = {"l": "english", "count": 2000}
            if start_assetid:
                params["start_assetid"] = start_assetid

            url = _INVENTORY_URL.format(steamid=steam_id)
            logger.info("Fetching Steam inventory page %d (steamid=%s)", page, steam_id)

            assert self._client is not None, "SteamInventoryClient used outside async context"
            try:
                resp = await self._client.get(url, params=params)
            except Exception as exc:
                logger.error("Steam inventory request failed: %s", exc)
                raise

            if resp.status_code == 403:
                raise PermissionError(
                    f"Steam inventory for {steam_id} is private or does not exist."
                )
            if resp.status_code == 429:
                logger.warning("Steam 429 — waiting 10s")
                await asyncio.sleep(10)
                continue
            if resp.status_code == 200 and resp.text.strip() == "null":
                raise PermissionError(f"Steam inventory for {steam_id} is private.")

            resp.raise_for_status()
            data = resp.json()

            if not data.get("success"):
                raise RuntimeError(f"Steam inventory API error: {data}")

            items = _parse_page(data)
            all_items.extend(items)

            # Pagination
            more = data.get("more_items", 0)
            if not more:
                break
            last_assetid = data.get("last_assetid")
            if not last_assetid:
                break
            start_assetid = last_assetid
            await asyncio.sleep(1.5)  # be polite between pages

        return all_items

    async def fetch(self, steam_id: str) -> list[dict]:
        """
        Fetch all CS2 items from a public Steam inventory.

        Returns aggregated list (one entry per market_hash_name) with keys:
            asset_id, classid, market_hash_name, name, item_type, rarity,
            tradable, marketable, icon_url, count, asset_ids, trade_unlock_at
        """
        return _aggregate(await self._fetch_pages(steam_id))

    async def fetch_assets(self, steam_id: str) -> list[dict]:
        """
        Fetch all CS2 items from a public Steam inventory.

        Returns one dict per asset (no aggregation) with keys:
            asset_id, classid, market_hash_name, name, item_type, rarity,
            tradable, marketable, icon_url, count, trade_unlock_at

        Use this for reconciliation where each asset must be matched separately.
        """
        return await self._fetch_pages(steam_id)


# ─── Parsing helpers ─────────────────────────────────────────────────────────


def _parse_page(data: dict) -> list[dict]:
    """Parse one page of Steam inventory API response."""
    descriptions = data.get("descriptions", [])
    assets = data.get("assets", [])

    # Build lookup: (classid, instanceid) -> description dict
    desc_map: dict[tuple[str, str], dict] = {}
    for d in descriptions:
        key = (str(d.get("classid", "")), str(d.get("instanceid", "")))
        desc_map[key] = d

    items = []
    for asset in assets:
        key = (str(asset.get("classid", "")), str(asset.get("instanceid", "")))
        desc = desc_map.get(key, {})

        mhn = desc.get("market_hash_name", "") or desc.get("name", "")
        if not mhn:
            logger.debug(
                "[INV] Skipping asset %s — no market_hash_name or name (classid=%s)",
                asset.get("assetid"), asset.get("classid"),
            )
            continue

        marketable = int(desc.get("marketable", 0))
        tradable = int(desc.get("tradable", 0))

        # Extract rarity from tags
        rarity = ""
        item_type = desc.get("type", "")
        for tag in desc.get("tags", []):
            if tag.get("category") == "Rarity":
                rarity = tag.get("localized_tag_name", "")
                break

        icon_url = desc.get("icon_url", "")
        full_icon = f"{_CDN_BASE}{icon_url}/96fx96f" if icon_url else ""

        items.append(
            {
                "asset_id": str(asset.get("assetid", "")),
                "classid": str(asset.get("classid", "") or ""),
                "market_hash_name": mhn,
                "name": desc.get("name", mhn),
                "item_type": item_type,
                "rarity": rarity,
                "tradable": tradable,
                "marketable": marketable,
                "icon_url": full_icon,
                "count": 1,
                "trade_unlock_at": _extract_trade_unlock_at(desc),
            }
        )

    return items


def _aggregate(items: list[dict]) -> list[dict]:
    """
    Group items by market_hash_name, summing counts.
    Each unique item keeps a list of asset_ids for individual tracking.
    """
    grouped: dict[str, dict] = {}
    for item in items:
        mhn = item["market_hash_name"]
        if mhn in grouped:
            grouped[mhn]["count"] += 1
            grouped[mhn]["asset_ids"].append(item["asset_id"])
            # Keep the latest non-None trade_unlock_at across duplicates.
            if item.get("trade_unlock_at") is not None:
                existing = grouped[mhn].get("trade_unlock_at")
                if existing is None or item["trade_unlock_at"] > existing:
                    grouped[mhn]["trade_unlock_at"] = item["trade_unlock_at"]
        else:
            entry = dict(item)
            entry["asset_ids"] = [item["asset_id"]]
            grouped[mhn] = entry
    return sorted(grouped.values(), key=lambda x: x["market_hash_name"])
