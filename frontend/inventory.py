"""
Inventory fetcher.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from ingestion.steam_inventory import SteamInventoryClient

logger = logging.getLogger(__name__)


def _run_async(coro: Any) -> Any:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)  # Required in Python 3.12+: makes the loop current in this thread
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()
        asyncio.set_event_loop(None)


def fetch_inventory(steam_id: str) -> list[dict]:
    async def _inner() -> list[dict]:
        async with SteamInventoryClient() as client:
            return await client.fetch(steam_id)

    result: list[dict] = _run_async(_inner())
    return result
