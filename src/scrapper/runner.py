"""
Async scraper functions — run_market_sync, run_inventory_sync, run_backfill_history.

Previously these were Celery tasks; they are now plain async coroutines called
directly by the in-process work queue worker (infra/work_queue.py).
"""

from __future__ import annotations

import asyncio
import logging
import uuid

logger = logging.getLogger(__name__)

_SCRAPE_TIMEOUT = 300  # seconds


async def run_market_sync() -> dict:
    """
    Discover CS2 containers from Steam Community Market.

    Fetches all weapon cases, souvenir packages, and capsules via the Market
    Search JSON API, writes any new items to DimContainer, and enqueues a
    backfill_history job when new containers are found.

    Returns a summary dict: {"status": "ok", "scraped": N, "inserted": M}.
    """
    from src.domain.connection import SessionLocal
    from scrapper.db_writer import write_new_containers
    from scrapper.state import mark_done
    from scrapper.steam_market_scraper import scrape_all_containers

    logger.info("run_market_sync: starting")

    try:
        containers = await asyncio.wait_for(scrape_all_containers(), timeout=_SCRAPE_TIMEOUT)
    except TimeoutError:
        logger.error("run_market_sync: timed out after %ds", _SCRAPE_TIMEOUT)
        return {"status": "error", "reason": "timeout", "scraped": 0, "inserted": 0}
    except Exception as exc:
        logger.error("run_market_sync: scrape failed — %s", exc)
        raise

    if not containers:
        logger.warning("run_market_sync: no containers returned from Steam Market API")
        return {"status": "ok", "scraped": 0, "inserted": 0}

    inserted = await asyncio.to_thread(_write_containers, containers)
    mark_done()

    if inserted:
        try:
            from infra.work_queue import get_queue
            get_queue().put_nowait({"type": "backfill_history"})
            logger.info("run_market_sync: %d new container(s) — enqueued backfill_history", inserted)
        except asyncio.QueueFull:
            logger.warning("run_market_sync: queue full — backfill_history not enqueued")
        except Exception as exc:
            logger.warning("run_market_sync: could not enqueue backfill_history: %s", exc)

    logger.info("run_market_sync: done — scraped=%d inserted=%d", len(containers), inserted)
    return {"status": "ok", "scraped": len(containers), "inserted": inserted}


def _write_containers(containers) -> int:
    from src.domain.connection import SessionLocal
    from scrapper.db_writer import write_new_containers
    with SessionLocal() as db:
        count = write_new_containers(db, containers)
        db.commit()
        return count


async def run_inventory_sync(steam_id: str | None = None) -> dict:
    """
    Sync Steam inventory for steam_id.

    Fetches all CS2 assets, updates trade-unlock dates, and reconciles asset
    IDs against Position rows.

    Returns a summary dict: {"status": "ok", "items": N, ...reconcile counts}.
    """
    from config import settings

    sid = (steam_id or settings.steam_id or "").strip()
    if not sid:
        logger.warning("run_inventory_sync: no steam_id configured — skipping")
        return {"status": "skipped", "reason": "no_steam_id"}

    logger.info("run_inventory_sync: starting for steam_id=%s", sid)

    from scrapper.steam_inventory import SteamInventoryClient

    async with SteamInventoryClient() as client:
        items: list[dict] = await client.fetch_assets(sid)

    if not items:
        logger.info("run_inventory_sync: inventory is empty for steam_id=%s", sid)
        return {"status": "ok", "items": 0}

    rec = await asyncio.to_thread(_persist_inventory, items)

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


def _persist_inventory(items: list[dict]):
    from src.domain.connection import SessionLocal
    from src.domain.sql_repositories import SqlAlchemyInventoryRepository, SqlAlchemyPositionRepository
    from src.domain.reconciler import PositionReconciler

    with SessionLocal() as db:
        inv_repo = SqlAlchemyInventoryRepository(db)
        for item in items:
            inv_repo.update_trade_unlock_at(
                item_name=item["market_hash_name"],
                unlock_at=item.get("trade_unlock_at"),
            )
        db.commit()

    with SessionLocal() as db:
        pos_repo = SqlAlchemyPositionRepository(db)
        rec = PositionReconciler().sync(items, pos_repo)
        db.commit()

    return rec


async def run_backfill_history(names: list[str] | None = None) -> dict:
    """
    Fetch and persist daily price history for containers.

    names: Optional list of container_name values to process.
           When None or empty, all non-blacklisted containers are processed.
    """
    from sqlalchemy import func
    from src.domain.connection import SessionLocal
    from src.domain.models import DimContainer, FactContainerPrice
    from scrapper.steam.client import SteamMarketClient
    from scrapper.steam.formatter import InvalidHashNameError, to_api_name

    names_filter: list[str] = list(names or [])

    with SessionLocal() as db:
        q = db.query(DimContainer).filter(DimContainer.is_blacklisted == False)  # noqa: E712
        if names_filter:
            q = q.filter(DimContainer.container_name.in_(names_filter))
        containers = q.all()

        id_map: dict[str, str] = {str(c.container_name): str(c.container_id) for c in containers}

        max_ts_rows = (
            db.query(FactContainerPrice.container_id, func.max(FactContainerPrice.timestamp))
            .filter(FactContainerPrice.container_id.in_(list(id_map.values())))
            .group_by(FactContainerPrice.container_id)
            .all()
        )
        cid_to_name = {v: k for k, v in id_map.items()}
        from datetime import date
        max_dates: dict[str, date] = {
            cid_to_name[str(cid)]: ts.date()
            for cid, ts in max_ts_rows
            if str(cid) in cid_to_name and ts is not None
        }

    if not id_map:
        logger.info("backfill_history: no containers")
        return {"status": "ok", "saved": 0, "errors": 0}

    ordered_names: list[str] = (
        [n for n in names_filter if n in id_map] if names_filter else list(id_map.keys())
    )

    logger.info("backfill_history: processing %d containers", len(ordered_names))

    try:
        steam_client = SteamMarketClient()
    except RuntimeError as exc:
        logger.warning("backfill_history: no cookie — %s", exc)
        return {"status": "skipped", "reason": "no_cookie", "saved": 0, "errors": 0}

    from datetime import date
    saved_total = 0
    errors = 0

    for name in ordered_names:
        cid = id_map[name]
        try:
            api_name = to_api_name(name)
        except InvalidHashNameError as exc:
            logger.warning("backfill_history: invalid name %s — %s", name, exc)
            errors += 1
            continue

        try:
            rows: list[dict] = await steam_client.fetch_history(api_name)
        except Exception as exc:
            logger.error("backfill_history: fetch error for %s — %s", name, exc)
            errors += 1
            continue

        if not rows:
            continue

        existing_max: date = max_dates.get(name, date.min)
        new_rows = [r for r in rows if r["date"].date() > existing_max]

        if not new_rows:
            continue

        try:
            saved = await asyncio.to_thread(_save_history_rows, cid, new_rows)
            saved_total += saved
            max_dates[name] = max(r["date"].date() for r in new_rows)
        except Exception as exc:
            logger.error("backfill_history: DB write error for %s — %s", name, exc)
            errors += 1

    logger.info("backfill_history: done — saved=%d errors=%d", saved_total, errors)
    return {"status": "ok", "saved": saved_total, "errors": errors}


def _save_history_rows(cid: str, new_rows: list[dict]) -> int:
    from src.domain.connection import SessionLocal
    from src.domain.models import FactContainerPrice

    price_objs = [
        FactContainerPrice(
            id=str(uuid.uuid4()),
            container_id=cid,
            timestamp=r["date"],
            price=r["price"],
            volume_7d=r["volume"],
            source="steam_market",
        )
        for r in new_rows
    ]
    with SessionLocal() as db:
        db.bulk_save_objects(price_objs)
        db.commit()
    return len(new_rows)
