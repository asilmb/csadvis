"""
Scraper for CS2 containers via Steam Community Market Search API.

Discovers all weapon cases, souvenir packages, and sticker/autograph/event
capsules by querying the Steam Market Search JSON endpoint directly.

No HTML parsing required — the API returns structured JSON with item tags.
"""

from __future__ import annotations

import asyncio
import logging
import random
import urllib.parse
from dataclasses import dataclass, field

from curl_cffi.requests import AsyncSession

from scrapper.steam.client import _is_emergency_blocked, _trigger_emergency_stop

# Chromium profile — must match SteamMarketClient._IMPERSONATE so both clients
# produce identical TLS fingerprints from the same IP.
_IMPERSONATE = "chrome131"

STEAM_HEADERS: dict[str, str] = {
    # Sec-Ch-Ua / Sec-Fetch-* are injected by curl_cffi impersonation;
    # these overlay Steam-specific headers that curl_cffi does not set.
    "Referer": "https://steamcommunity.com/market/",
    "X-Requested-With": "XMLHttpRequest",
    "Accept": "application/json",
}

logger = logging.getLogger(__name__)

_API_BASE = "https://steamcommunity.com/market/search/render/"
_LISTING_BASE = "https://steamcommunity.com/market/listings/730/"
_APP_ID = 730
_PAGE_SIZE = 100
_TIMEOUT = 45.0  # PV-68: EU servers need more time

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
    page_url: str = ""
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
    client: AsyncSession,
    type_tag: str,
    start: int,
) -> dict | None:
    """
    Fetch one page of Steam Market search results for a given type tag.

    Returns parsed JSON dict on success, None on error.
    Respects the global emergency stop — skips request if a 429 block is active.
    """
    if _is_emergency_blocked():
        logger.debug("[SCRAPER] skipped tag=%s start=%d — emergency stop active", type_tag, start)
        return None

    url = _build_search_url(type_tag, start)

    try:
        await asyncio.sleep(random.uniform(3.2, 7.1))  # jittered delay — less detectable than fixed
        resp = await client.get(url, timeout=_TIMEOUT)

        if resp.status_code == 429:
            logger.warning(
                "[SCRAPER] 429 on tag=%s start=%d — activating emergency stop",
                type_tag, start,
            )
            _trigger_emergency_stop(f"scraper:{type_tag}", attempt=0)
            return None

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


async def scrape_all_containers() -> list[ScrapedContainer]:
    """
    Discover all CS2 weapon cases, souvenir packages, and capsules from
    the Steam Community Market Search API.

    Returns a flat list of ScrapedContainer objects.  Items inside each
    container are not fetched (items=[] always) — db_writer only needs
    container-level metadata.
    """
    headers = STEAM_HEADERS  # Accept already included in STEAM_HEADERS

    containers: list[ScrapedContainer] = []
    seen_names: set[str] = set()

    async with AsyncSession(impersonate=_IMPERSONATE, headers=headers) as client:
        n_tags = len(_QUERY_TAGS)
        for tag_idx, type_tag in enumerate(_QUERY_TAGS, start=1):
            raw_ctype = _TAG_TO_CTYPE[type_tag]
            logger.info("[SCRAPER] Querying Steam Market for tag=%s (%d/%d)", type_tag, tag_idx, n_tags)

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
                logger.info("[SCRAPER] Scraping: %s (%d/%s)", raw_ctype, start, total_count or "?")

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
