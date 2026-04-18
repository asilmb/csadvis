"""
High-level orchestration for Steam Market batch operations (PV-48).

Combines SteamMarketClient calls with rate-limiting and error isolation.
"""

from __future__ import annotations

import asyncio
import logging
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



