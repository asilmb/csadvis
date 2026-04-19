"""
Steam Market transaction history scraper.

Fetches CS2 buy/sell history from /market/myhistory (up to 500 per page,
paginated). Prices are in the account's native currency (currencyid=2037).

Steam API response format (new JSON structure, PV-52):
  events    — timeline entries: {listingid, purchaseid, event_type, time_event}
  listings  — seller-side data: {listingid → {original_price, price, currencyid, asset}}
  purchases — buyer-side data:  {lid_pid    → {paid_amount, currencyid, …}}
  assets    — item metadata:    {appid → {contextid → {asset_id → {market_hash_name, …}}}}

event_type:  3 = SELL (our listing was bought)  |  4 = BUY (we bought a listing)
Price units: Steam stores all amounts as integer cents (1/100 of currency unit).
             Divide by 100 to get a float amount.

Main entry points:
  fetch_market_history(max_pages)  → (list[dict], message)
  compute_annual_pnl(transactions) → {year: pnl}
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import httpx

from infra.steam_credentials import get_login_secure, get_session_id

logger = logging.getLogger(__name__)

_HISTORY_URL = "https://steamcommunity.com/market/myhistory"

# Steam event type constants
_EVENT_SELL = 3   # our listing was purchased by someone → we received funds
_EVENT_BUY  = 4   # we purchased someone else's listing → we spent funds

# Steam currency ID for the account's native currency
_STEAM_CURRENCY_ID = 2037


def fetch_market_history(max_pages: int = 10) -> tuple[list[dict], str]:
    """
    Fetch Steam Market CS2 transaction history.

    Returns (transactions, message).
    Each transaction dict:
        listing_id  — Steam listing ID (str)
        date        — datetime
        action      — "BUY" | "SELL"
        item_name   — str  (market_hash_name)
        price       — float  (amount: received for SELL, paid for BUY)
        total       — float  (same as price; Steam Market is always qty=1)
    """
    cookie = get_login_secure()
    if not cookie:
        return [], "NO_COOKIE"

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "ru-RU,ru;q=0.9",
        "Referer": "https://steamcommunity.com/market/",
    }
    cookies: dict[str, str] = {"steamLoginSecure": cookie}
    if get_session_id():
        cookies["sessionid"] = get_session_id()

    all_rows: list[dict] = []
    page_size = 500

    for page in range(max_pages):
        start = page * page_size
        try:
            resp = httpx.get(
                _HISTORY_URL,
                params={"norender": "1", "start": str(start), "count": str(page_size)},
                cookies=cookies,
                headers=headers,
                timeout=30,
                follow_redirects=True,
            )
        except Exception as exc:
            logger.error("Steam history fetch error page %d: %s", page, exc)
            break

        if resp.status_code == 403:
            return all_rows, "Cookie устарел (HTTP 403). Запусти: cs2 cookie"
        if resp.status_code != 200:
            return all_rows, f"Steam вернул HTTP {resp.status_code}"

        try:
            data: dict[str, Any] = resp.json()
        except Exception:
            return all_rows, "Ответ Steam не является JSON"

        if not data.get("success"):
            return all_rows, "Steam history: success=false (возможно cookie устарел)"

        total_count = int(data.get("total_count", 0))

        # Parse structured JSON response (current Steam API format).
        # Falls back to HTML parser if `events` key is absent (older API).
        if "events" in data:
            rows = _parse_history_json(data)
        else:
            rows = _parse_history_html(data)

        all_rows.extend(rows)

        logger.info(
            "Steam history page %d: parsed %d rows (total_count=%d)",
            page,
            len(rows),
            total_count,
        )

        if total_count == 0 or start + page_size >= total_count or not rows:
            break

    msg = f"Загружено {len(all_rows)} транзакций CS2"
    return all_rows, msg


def _parse_history_json(data: dict) -> list[dict]:
    """
    Parse structured JSON response from /market/myhistory.

    The API returns:
      data['events']    — list of timeline entries
      data['listings']  — dict keyed by listingid (seller side)
      data['purchases'] — dict keyed by "{listingid}_{purchaseid}" (buyer side)
      data['assets']    — dict keyed by appid→contextid→asset_id (item metadata)

    Filters to CS2 (appid=730) only.  Skips non-BUY/SELL event types.
    Price: listing['original_price'] for SELL (cents, currencyid=2037).
           purchase['paid_amount'] for BUY  (cents, currencyid=2037).
           Falls back to listing['original_price'] when paid_amount is missing.
    Zero prices are logged but NOT skipped — they are persisted as 0.
    """
    events: list[dict] = data.get("events") or []
    listings: dict[str, dict] = data.get("listings") or {}
    purchases: dict[str, dict] = data.get("purchases") or {}
    assets_map: dict = data.get("assets") or {}

    rows: list[dict] = []

    for event in events:
        event_type = event.get("event_type")
        if event_type not in (_EVENT_SELL, _EVENT_BUY):
            continue

        listing_id = str(event.get("listingid", ""))
        purchase_id = str(event.get("purchaseid", ""))
        purchase_key = f"{listing_id}_{purchase_id}"
        time_event = event.get("time_event", 0)

        listing  = listings.get(listing_id) or {}
        purchase = purchases.get(purchase_key) or {}

        # ── Asset lookup (CS2 filter + item name) ─────────────────────────────
        # Always use the listing's asset reference — it uses contextid=2 (inventory)
        # which is present in the assets dict. Purchase asset uses contextid=16
        # (market escrow) which is NOT indexed in assets.
        asset_ref = listing.get("asset") or {}
        appid     = str(asset_ref.get("appid", ""))
        contextid = str(asset_ref.get("contextid", "2"))
        asset_id  = str(asset_ref.get("id", ""))

        if appid != "730":
            continue  # not CS2 — skip

        asset = (
            assets_map.get(appid, {})
                      .get(contextid, {})
                      .get(asset_id, {})
        )
        item_name = asset.get("market_hash_name") or asset.get("name") or ""
        if not item_name:
            logger.debug(
                "_parse_history_json: cannot resolve item name for listing_id=%s "
                "(appid=%s ctx=%s id=%s) — skipping",
                listing_id, appid, contextid, asset_id,
            )
            continue

        # ── Price (cents → float) ─────────────────────────────────────────────
        if event_type == _EVENT_SELL:
            # listing['price'] == 0 when transaction completed; use original_price
            raw_price = listing.get("original_price") or listing.get("price") or 0
            price = raw_price / 100.0

        else:  # _EVENT_BUY
            # paid_amount is always in our wallet currency (currencyid=2037)
            raw_price = purchase.get("paid_amount") or listing.get("original_price") or 0
            price = raw_price / 100.0

        if price == 0:
            logger.debug(
                "_parse_history_json: zero price for %r listing_id=%s — recording as 0",
                item_name, listing_id,
            )

        # ── Date ──────────────────────────────────────────────────────────────
        trade_date = datetime.fromtimestamp(time_event) if time_event else datetime.now()

        rows.append({
            "listing_id": listing_id,
            "date": trade_date,
            "action": "SELL" if event_type == _EVENT_SELL else "BUY",
            "item_name": item_name,
            "price": price,
            "total": price,  # Steam Market is always qty=1
        })

    return rows


def _parse_history_html(data: dict) -> list[dict]:
    """
    Legacy HTML parser — fallback for older Steam API responses that return
    results_html / html fields instead of structured JSON.

    Only called when 'events' key is absent from the response.
    """
    import re

    from scrapper.steam_wallet import _parse_amount

    results_html = data.get("html") or data.get("results_html", "")
    if not results_html:
        logger.warning("_parse_history_html: no html/results_html in response — 0 rows")
        return []

    rows: list[dict] = []

    chunks = re.split(r'(?=id="history_row_\d+_(?:sell|buy)")', results_html)

    for chunk in chunks:
        head = re.match(r'id="history_row_(\d+)_(sell|buy)"', chunk)
        if not head:
            continue

        listing_id = head.group(1)
        row_type = head.group(2)

        if not re.search(r"Counter-Strike", chunk, re.IGNORECASE):
            continue

        gain = re.search(r'market_listing_gainorloss["\'][^>]*>\s*([\+\-])\s*</div>', chunk)
        action = "SELL" if (gain and gain.group(1) == "+") or row_type == "sell" else "BUY"

        name_m = re.search(r'market_listing_item_name["\'][^>]*>([^<]+)</span>', chunk)
        if not name_m:
            continue
        item_name = name_m.group(1).strip()

        full_date = re.search(r'title="([A-Za-z]+ \d{1,2},\s*\d{4})"', chunk)
        if full_date:
            try:
                trade_date = datetime.strptime(full_date.group(1).strip(), "%b %d, %Y")
            except ValueError:
                continue
        else:
            short_date = re.search(
                r"market_listing_listed_date[^>]*>[^<]*<[^>]+>([A-Za-z]+ \d{1,2})</[^>]+>",
                chunk,
            )
            if not short_date:
                continue
            try:
                trade_date = datetime.strptime(short_date.group(1).strip(), "%b %d").replace(
                    year=datetime.now().year
                )
            except ValueError:
                continue

        price_raw: str | None = None
        price_block = re.search(r"can_combine[^>]*>[^<]*<span[^>]*>\s*([^<]+)\s*</span>", chunk)
        if price_block:
            price_raw = price_block.group(1).strip()
        else:
            price_m = re.search(
                r'market_listing_price["\'][^>]*>\s*(?:<span[^>]*>)?([^<]+?)(?:</span>)?<',
                chunk,
            )
            if price_m:
                price_raw = price_m.group(1).strip()

        if not price_raw:
            continue

        price = _parse_amount(price_raw)
        if not price or price <= 0:
            continue

        rows.append({
            "listing_id": listing_id,
            "date": trade_date,
            "action": action,
            "item_name": item_name,
            "price": price,
            "total": price,
        })

    return rows


def compute_annual_pnl(transactions: list[dict]) -> dict[int, float]:
    """
    Compute annual P&L from transactions.

    P&L = SELL proceeds − BUY costs, grouped by calendar year.
    SELL proceeds are net of Steam fee (what the seller receives).
    """
    by_year: dict[int, dict[str, float]] = {}
    for tx in transactions:
        year = tx["date"].year
        if year not in by_year:
            by_year[year] = {"sell": 0.0, "buy": 0.0}
        if tx["action"] == "SELL":
            by_year[year]["sell"] += tx["total"]
        else:
            by_year[year]["buy"] += tx["total"]
    return {yr: data["sell"] - data["buy"] for yr, data in sorted(by_year.items())}
