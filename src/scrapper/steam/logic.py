"""
High-level orchestration for Steam Market batch operations (PV-48).

Combines SteamMarketClient calls with rate-limiting and error isolation.
"""

from __future__ import annotations

import asyncio
import logging
import random
from datetime import datetime
from typing import Any

from scrapper.steam.formatter import InvalidHashNameError

logger = logging.getLogger(__name__)

_DELAY_SECONDS = 4.0


async def fetch_all_current_prices(
    client: Any,
    names: list[str],
    on_progress: Any = None,
) -> dict[str, dict]:
    """
    Fetch current prices for multiple items with rate-limit delay.
    Returns {name: overview_dict}.

    On InvalidHashNameError the entry is {"_invalid_hash": True} so the
    caller can increment error_count without crashing the whole batch (PV-50).
    """
    results = {}
    total = len(names)
    for idx, name in enumerate(names, 1):
        try:
            result = await client.fetch_price_overview(name)
        except InvalidHashNameError as exc:
            logger.warning("fetch_all_current_prices: invalid hash %r — %s", name, exc)
            result = {"_invalid_hash": True}
        results[name] = result
        if on_progress:
            on_progress(name, idx, total)
        if idx < total:
            await asyncio.sleep(_DELAY_SECONDS)
    return results


async def fetch_all(
    client: Any,
    names: list[str],
    on_progress: Any = None,
    delay: float = _DELAY_SECONDS,
) -> dict[str, list[dict]]:
    """
    Fetch history for multiple items with rate-limit delay.
    on_progress(name, idx, total) called after each fetch.
    delay: seconds between requests (default 4s; pass 0 to disable).
    """
    results = {}
    total = len(names)
    for idx, name in enumerate(names, 1):
        logger.info("Steam Market history: %d/%d  %s", idx, total, name)
        rows = await client.fetch_history(name)
        results[name] = rows
        if on_progress:
            on_progress(name, idx, total)
        if idx < total and delay > 0:
            await asyncio.sleep(delay)
    return results


# ─── Volatility & Tiering ─────────────────────────────────────────────────────

_TIER1_VOL_MIN = 0.15   # >15% volatility → Tier 1 (Active)

def calculate_volatility(repo: Any, item_id: str, days: int = 7) -> float:
    """(Max - Min) / Avg over last `days` days of price history. Returns 0.0 if insufficient data."""
    price_rows = repo.get_price_history(item_id, days)
    prices = [r.price for r in price_rows if r.price and r.price > 0]
    if len(prices) < 2:
        return 0.0
    avg = sum(prices) / len(prices)
    return 0.0 if avg == 0 else (max(prices) - min(prices)) / avg


def classify_tier(volatility: float, price: float = 0.0) -> int:
    """
    Returns 1 (Active) or 3 (Cold).
    Tier 1: volatility > 15% AND price > cold threshold (~50).
    Tier 3: volatility <= 15% OR price <= cold threshold.
    """
    _COLD_PRICE = 50.0
    if volatility > _TIER1_VOL_MIN and price > _COLD_PRICE:
        return 1
    return 3


# ─── Ghost Scheduler ──────────────────────────────────────────────────────────

_NIGHT_START_H = 1    # 01:00
_NIGHT_END_H = 8      # 08:30
_NIGHT_END_M = 30
_SHARD_MIN = 0.10
_SHARD_MAX = 0.20
_JITTER_MIN = 27.0
_JITTER_MAX = 62.0
_DEEP_SLEEP_MIN = 3600.0
_DEEP_SLEEP_MAX = 7200.0


def is_night_mode() -> bool:
    """True between 01:00 and 08:30 local time — full stop period."""
    now = datetime.now()
    start = now.replace(hour=_NIGHT_START_H, minute=0, second=0, microsecond=0)
    end = now.replace(hour=_NIGHT_END_H, minute=_NIGHT_END_M, second=0, microsecond=0)
    return start <= now < end


def build_ghost_session(tier1_names: list[str]) -> list[str]:
    """Select random 10–20% of Tier 1 items for this ghost session."""
    if not tier1_names:
        return []
    count = max(1, int(len(tier1_names) * random.uniform(_SHARD_MIN, _SHARD_MAX)))
    selected = random.sample(tier1_names, min(count, len(tier1_names)))
    logger.info(
        "[Stealth] Ghost session: %d/%d items selected (%.0f%% shard)",
        len(selected),
        len(tier1_names),
        100.0 * len(selected) / len(tier1_names),
    )
    return selected


def ghost_jitter_delay() -> float:
    """Random within-batch delay: 27–62 seconds."""
    return random.uniform(_JITTER_MIN, _JITTER_MAX)


def deep_sleep_seconds() -> float:
    """Random inter-session deep sleep: 1–2 hours (3600–7200 seconds)."""
    return random.uniform(_DEEP_SLEEP_MIN, _DEEP_SLEEP_MAX)
