"""
Scraper for CS2 containers via Steam Community Market Search API.

Discovers all weapon cases, souvenir packages, and sticker/autograph/event
capsules by querying the Steam Market Search JSON endpoint directly.

No HTML parsing required — the API returns structured JSON with item tags.
Rate-limited to 1 request per 1.5 seconds to avoid Steam bans.
"""

from __future__ import annotations

import asyncio
import logging
import urllib.parse
from dataclasses import dataclass, field

import httpx

STEAM_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Referer": "https://steamcommunity.com/market/",
    "X-Requested-With": "XMLHttpRequest",
}

logger = logging.getLogger(__name__)

_API_BASE = "https://steamcommunity.com/market/search/render/"
_LISTING_BASE = "https://steamcommunity.com/market/listings/730/"
_APP_ID = 730
_PAGE_SIZE = 100
_TIMEOUT = 45.0  # PV-68: EU servers need more time
_RETRY_SLEEP = 60.0  # seconds to wait after a 429 (before re-acquiring a token)

# Steam Market Type tag → raw container type string
_TAG_TO_CTYPE: dict[str, str] = {
    "tag_CSGO_Type_WeaponCase": "Weapon Case",
    "tag_CSGO_Type_SouvenirPackage": "Souvenir Package",
    "tag_CSGO_Type_StickerCapsule": "Sticker Capsule",
}

# Category tags to query (one request batch per tag)
_QUERY_TAGS = list(_TAG_TO_CTYPE.keys())

# Keywords that identify group-level event capsules (Steam-listable)
_CAPSULE_GROUP_WORDS = frozenset({"challengers", "legends", "contenders", "champions"})


# ─── Data classes (public interface — same shape as old csgostash.py) ──────────


@dataclass
class ScrapedItem:
    base_name: str
    rarity: str


@dataclass
class ScrapedContainer:
    name: str
    container_type: str
    page_url: str
    items: list[ScrapedItem] = field(default_factory=list)


# ─── Container type resolution ─────────────────────────────────────────────────


def _resolve_container_type(name: str, raw_ctype: str) -> str:
    """
    Refine the raw container type string for capsule sub-types.

    Weapon Cases and Souvenir Packages keep their raw type unchanged.
    For StickerCapsule entries, apply name-based sub-classification:
      - "autograph" in name → "Autograph Capsule"
      - group keyword present → "Event Capsule"
      - otherwise → "Sticker Capsule"
    """
    if raw_ctype != "Sticker Capsule":
        return raw_ctype

    low = name.lower()
    if "autograph" in low:
        return "Autograph Capsule"
    if any(kw in low for kw in _CAPSULE_GROUP_WORDS):
        return "Event Capsule"
    return "Sticker Capsule"


def _extract_type_tag(tags: list[dict]) -> str | None:
    """Return the internal_name of the Type tag, or None if absent."""
    for tag in tags:
        if tag.get("category") == "Type":
            return str(tag.get("internal_name", ""))
    return None


# ─── HTTP helpers ──────────────────────────────────────────────────────────────


def _build_search_url(type_tag: str, start: int) -> str:
    """
    Build the Steam Market search URL with unencoded square brackets.

    httpx.AsyncClient encodes '[' and ']' as %5B/%5D when passed via params=dict.
    Steam's search API requires literal brackets in the category parameter key
    (category_730_Type[]=...) — encoding them causes 0 results for some tag types
    (SouvenirPackage, StickerCapsule).  We pre-build the query string manually
    using urllib.parse.quote which does NOT encode brackets by default.
    """
    base_params = urllib.parse.urlencode([
        ("appid", str(_APP_ID)),
        ("norender", "1"),
        ("count", str(_PAGE_SIZE)),
        ("start", str(start)),
    ])
    # Bracket key is appended literally; tag value is safely quoted.
    tag_param = f"category_{_APP_ID}_Type[]={urllib.parse.quote(type_tag, safe='')}"
    return f"{_API_BASE}?{base_params}&{tag_param}"


async def _fetch_page(
    client: httpx.AsyncClient,
    type_tag: str,
    start: int,
) -> dict | None:
    """
    Fetch one page of Steam Market search results for a given type tag.

    Returns parsed JSON dict on success, None on error.
    """
    url = _build_search_url(type_tag, start)

    try:
        await asyncio.sleep(4.0)  # simple rate limit: ~15 req/min
        resp = await client.get(url)

        if resp.status_code == 429:
            logger.warning(
                "[SCRAPER] 429 on tag=%s start=%d — waiting %.0fs before retry",
                type_tag, start, _RETRY_SLEEP,
            )
            await asyncio.sleep(_RETRY_SLEEP)
            resp = await client.get(url)

        if resp.status_code != 200:
            logger.warning(
                "[SCRAPER] HTTP %d on tag=%s start=%d", resp.status_code, type_tag, start,
            )
            return None

        return resp.json()

    except Exception as exc:
        logger.warning("[SCRAPER] fetch failed (tag=%s start=%d): %s", type_tag, start, exc)
        return None


# ─── Public API ────────────────────────────────────────────────────────────────


async def scrape_all_containers(task_id: str | None = None) -> list[ScrapedContainer]:
    """
    Discover all CS2 weapon cases, souvenir packages, and capsules from
    the Steam Community Market Search API.

    Returns a flat list of ScrapedContainer objects.  Items inside each
    container are not fetched (items=[] always) — db_writer only needs
    container-level metadata.
    """
    headers = {**STEAM_HEADERS, "Accept": "application/json"}

    containers: list[ScrapedContainer] = []
    seen_names: set[str] = set()

    async def _report(msg: str) -> None:
        """Push progress string to task_queue payload (best-effort, non-blocking)."""
        if not task_id:
            return
        try:
            from infra.task_manager import TaskQueueService
            await asyncio.to_thread(
                TaskQueueService().update_task_progress, task_id, {"_progress": msg}
            )
        except Exception:
            pass

    async with httpx.AsyncClient(
        headers=headers,
        timeout=httpx.Timeout(_TIMEOUT),
        limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
        follow_redirects=True,
    ) as client:
        n_tags = len(_QUERY_TAGS)
        for tag_idx, type_tag in enumerate(_QUERY_TAGS, start=1):
            raw_ctype = _TAG_TO_CTYPE[type_tag]
            logger.info("[SCRAPER] Querying Steam Market for tag=%s", type_tag)
            await _report(f"Scraping: {raw_ctype} (tag {tag_idx}/{n_tags})")

            start = 0
            total_count: int | None = None

            while True:
                data = await _fetch_page(client, type_tag, start)

                if not data or not data.get("success"):
                    logger.warning(
                        "[SCRAPER] unsuccessful response for tag=%s start=%d", type_tag, start
                    )
                    break

                if total_count is None:
                    total_count = int(data.get("total_count", 0))
                    logger.info("[SCRAPER] tag=%s total_count=%d", type_tag, total_count)

                # Progress: "Scraping: Weapon Cases (18/120)"
                await _report(f"Scraping: {raw_ctype} ({start}/{total_count or '?'})")

                results = data.get("results") or []
                if not results:
                    break

                for item in results:
                    name = str(item.get("name") or item.get("hash_name") or "").strip()
                    if not name:
                        continue
                    if name in seen_names:
                        continue
                    seen_names.add(name)

                    hash_name = str(item.get("hash_name") or name)
                    page_url = _LISTING_BASE + urllib.parse.quote(hash_name)

                    # Derive container type from API tags when available
                    tags = item.get("tags") or []
                    tag_internal = _extract_type_tag(tags)
                    effective_raw = _TAG_TO_CTYPE.get(tag_internal or "", raw_ctype)
                    ctype = _resolve_container_type(name, effective_raw)

                    containers.append(
                        ScrapedContainer(
                            name=name,
                            container_type=ctype,
                            page_url=page_url,
                            items=[],
                        )
                    )

                start += len(results)
                if total_count is not None and start >= total_count:
                    break

            logger.info(
                "[SCRAPER] finished tag=%s — %d containers collected so far",
                type_tag,
                len(containers),
            )

    logger.info("[SCRAPER] total containers discovered: %d", len(containers))
    return containers
