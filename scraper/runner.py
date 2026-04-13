"""
Scraper Celery tasks.

run_market_sync()           — Discover new containers from Steam Community Market.
                              Scrapes one full pass, writes to DB, returns a result dict.

run_inventory_sync(steam_id) — Sync Steam inventory for steam_id (falls back to
                               settings.steam_id).  Reconciles asset IDs, returns a
                               result dict.

Both tasks are one-shot: they start, process their work, and return.
No infinite loops, no background sleep.
"""

from __future__ import annotations

import asyncio
import logging

from scheduler.celery_app import app

logger = logging.getLogger(__name__)

_SCRAPE_TIMEOUT = 300  # seconds — Steam Market search can be slow


# ── run_market_sync ────────────────────────────────────────────────────────────

@app.task(bind=True, max_retries=2, default_retry_delay=120)
def run_market_sync(self) -> dict:  # type: ignore[misc]
    """
    Discover CS2 containers from Steam Community Market.

    Fetches all weapon cases, souvenir packages, and capsules via the Market
    Search JSON API, writes any new items to DimContainer, and enqueues a
    backfill_history task when new containers are found.

    Returns a summary dict: {"status": "ok", "scraped": N, "inserted": M}.
    """
    from database.connection import SessionLocal
    from scraper.db_writer import write_new_containers
    from scraper.state import mark_done
    from scraper.steam_market_scraper import scrape_all_containers

    task_id: str = self.request.id or ""
    logger.info("run_market_sync: starting (task=%s)", task_id)

    # ── Scrape ────────────────────────────────────────────────────────────────
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            containers = loop.run_until_complete(
                asyncio.wait_for(scrape_all_containers(task_id), timeout=_SCRAPE_TIMEOUT)
            )
        finally:
            loop.close()
            asyncio.set_event_loop(None)
    except TimeoutError:
        logger.error("run_market_sync: timed out after %ds", _SCRAPE_TIMEOUT)
        return {"status": "error", "reason": "timeout", "scraped": 0, "inserted": 0}
    except Exception as exc:
        logger.error("run_market_sync: scrape failed — %s", exc)
        raise self.retry(exc=exc)

    if not containers:
        logger.warning("run_market_sync: no containers returned from Steam Market API")
        return {"status": "ok", "scraped": 0, "inserted": 0}

    # ── Persist ───────────────────────────────────────────────────────────────
    with SessionLocal() as db:
        inserted = write_new_containers(db, containers)

    mark_done()

    if inserted:
        try:
            from services.task_manager import TaskQueueService
            TaskQueueService().enqueue("backfill_history", priority=1)
            logger.info(
                "run_market_sync: %d new container(s) — enqueued backfill_history", inserted
            )
        except Exception as exc:
            logger.warning("run_market_sync: could not enqueue backfill_history: %s", exc)

    logger.info(
        "run_market_sync: done — scraped=%d inserted=%d", len(containers), inserted
    )
    return {"status": "ok", "scraped": len(containers), "inserted": inserted}


# ── run_inventory_sync ─────────────────────────────────────────────────────────

@app.task(bind=True, max_retries=2, default_retry_delay=120)
def run_inventory_sync(self, steam_id: str | None = None) -> dict:  # type: ignore[misc]
    """
    Sync Steam inventory for steam_id.

    Fetches all CS2 assets, updates trade-unlock dates, and reconciles asset
    IDs against DimUserPosition rows.

    Returns a summary dict: {"status": "ok", "items": N, ...reconcile counts}.
    """
    from config import settings

    sid = (steam_id or settings.steam_id or "").strip()
    if not sid:
        logger.warning("run_inventory_sync: no steam_id configured — skipping")
        return {"status": "skipped", "reason": "no_steam_id"}

    logger.info("run_inventory_sync: starting for steam_id=%s", sid)

    # ── Fetch assets ──────────────────────────────────────────────────────────
    try:
        from ingestion.steam_inventory import SteamInventoryClient

        async def _fetch() -> list[dict]:
            async with SteamInventoryClient() as client:
                return await client.fetch_assets(sid)

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            items: list[dict] = loop.run_until_complete(_fetch())
        finally:
            loop.close()
            asyncio.set_event_loop(None)
    except Exception as exc:
        logger.error("run_inventory_sync: fetch failed — %s", exc)
        raise self.retry(exc=exc)

    if not items:
        logger.info("run_inventory_sync: inventory is empty for steam_id=%s", sid)
        return {"status": "ok", "items": 0}

    # ── Persist trade-ban dates ───────────────────────────────────────────────
    from database.connection import SessionLocal
    from database.repositories import (
        SqlAlchemyInventoryRepository,
        SqlAlchemyPositionRepository,
    )
    from services.reconciler import PositionReconciler

    with SessionLocal() as db:
        inv_repo = SqlAlchemyInventoryRepository(db)
        for item in items:
            inv_repo.update_trade_unlock_at(
                item_name=item["market_hash_name"],
                unlock_at=item.get("trade_unlock_at"),
            )
        db.commit()

    # ── Reconcile positions ───────────────────────────────────────────────────
    with SessionLocal() as db:
        pos_repo = SqlAlchemyPositionRepository(db)
        rec = PositionReconciler().sync(items, pos_repo)
        db.commit()

    logger.info(
        "run_inventory_sync: done — items=%d matched_direct=%d matched_fifo=%d",
        len(items), rec.matched_direct, rec.matched_fifo,
    )
    return {
        "status": "ok",
        "items": len(items),
        "matched_direct": rec.matched_direct,
        "matched_listing": rec.matched_listing,
        "matched_fifo": rec.matched_fifo,
    }
