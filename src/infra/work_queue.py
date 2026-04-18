"""
In-process asyncio work queue — replaces Celery + Redis broker.

Public API
----------
get_queue()         → asyncio.Queue[dict]  (put jobs here)
get_worker_state()  → dict                 (read-only status for UI)
supervised_worker() → coroutine            (pass to asyncio.create_task)

Job dict shape
--------------
{"type": "market_catalog"}
{"type": "price_poll"}
{"type": "sync_inventory", "steam_id": "..."}
{"type": "backfill_history", "names": [...] | None}

Silent-failure guard
--------------------
The inner _worker_loop is wrapped by supervised_worker().  Any exception
that escapes _worker_loop (programming error, import failure, etc.) is
caught by the supervisor, logged at CRITICAL level, and the loop restarts
after a short backoff.  This prevents the worker from silently dying while
the web server keeps accepting requests.
"""

from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass, field
from datetime import UTC, datetime

logger = logging.getLogger(__name__)

_work_queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=10)


@dataclass
class _WorkerState:
    busy: bool = False
    current_type: str = ""
    last_job_at: datetime | None = None
    last_error: str = ""
    restarts: int = 0


_state = _WorkerState()


def get_queue() -> asyncio.Queue[dict]:
    return _work_queue


def get_worker_state() -> dict:
    return {
        "busy": _state.busy,
        "current_type": _state.current_type,
        "last_job_at": _state.last_job_at.isoformat() if _state.last_job_at else None,
        "last_error": _state.last_error,
        "restarts": _state.restarts,
        "queue_size": _work_queue.qsize(),
    }


# ── Job handlers ──────────────────────────────────────────────────────────────


async def _handle_market_catalog(_job: dict) -> None:
    from scrapper.runner import run_market_sync
    await run_market_sync()


async def _handle_price_poll(job: dict) -> None:
    """Fetch current Steam Market prices for all (or one) non-blacklisted containers."""
    import asyncio as _asyncio
    from src.domain.connection import SessionLocal
    from src.domain.models import DimContainer
    from scrapper.steam.client import SteamMarketClient
    from scrapper.steam.formatter import InvalidHashNameError, to_api_name
    from src.domain.item_service import ItemService

    single_cid = job.get("container_id")

    with SessionLocal() as db:
        q = db.query(DimContainer).filter(DimContainer.is_blacklisted == 0)
        if single_cid:
            q = q.filter(DimContainer.container_id == single_cid)
        containers = q.all()
        names = [(str(c.container_id), str(c.container_name)) for c in containers]

    if not names:
        logger.info("price_poll: no containers to poll")
        return

    logger.info("price_poll: polling %d containers", len(names))
    async with SteamMarketClient() as client:
        for cid, name in names:
            try:
                api_name = to_api_name(name)
            except InvalidHashNameError:
                continue
            try:
                overview = await client.fetch_price_overview(api_name)
                raw = overview.get("lowest_price") or overview.get("median_price")
                if raw is None:
                    continue
                price_str = str(raw).replace("₸", "").replace(",", ".").strip()
                price = float(price_str)
                if price <= 0:
                    continue
                svc = ItemService.open()
                try:
                    await _asyncio.to_thread(svc.process_new_price, cid, price)
                finally:
                    svc.close()
            except Exception as exc:
                logger.warning("price_poll: error for %s — %s", name, exc)
            # Non-deterministic delay between requests to avoid Steam rate-limiting
            await _asyncio.sleep(random.uniform(2.5, 5.5))

    logger.info("price_poll: done")


async def _handle_sync_inventory(job: dict) -> None:
    from scrapper.runner import run_inventory_sync
    await run_inventory_sync(steam_id=job.get("steam_id"))


async def _handle_backfill_history(job: dict) -> None:
    from scrapper.runner import run_backfill_history
    await run_backfill_history(names=job.get("names"))


_HANDLERS: dict[str, object] = {
    "market_catalog":  _handle_market_catalog,
    "price_poll":      _handle_price_poll,
    "sync_inventory":  _handle_sync_inventory,
    "backfill_history": _handle_backfill_history,
}


async def _process_job(job: dict) -> None:
    job_type = job.get("type", "unknown")
    handler = _HANDLERS.get(job_type)
    if handler is None:
        logger.warning("work_queue: unknown job type %r — skipping", job_type)
        return
    await handler(job)  # type: ignore[operator]


# ── Worker loop ───────────────────────────────────────────────────────────────


async def _worker_loop() -> None:
    """
    Inner worker loop.  Runs until cancelled.
    Exceptions from individual jobs are caught here — a single failing job
    does NOT stop the worker; the loop continues with the next item.
    """
    logger.info("work_queue: worker started")
    while True:
        job = await _work_queue.get()
        job_type = job.get("type", "unknown")
        _state.busy = True
        _state.current_type = job_type
        _state.last_job_at = datetime.now(UTC).replace(tzinfo=None)
        try:
            logger.info("work_queue: starting job type=%r", job_type)
            await _process_job(job)
            logger.info("work_queue: finished job type=%r", job_type)
            _state.last_error = ""
        except asyncio.CancelledError:
            _work_queue.task_done()
            raise
        except Exception as exc:
            _state.last_error = f"{type(exc).__name__}: {exc}"
            logger.error(
                "work_queue: job type=%r failed — %s",
                job_type,
                exc,
                exc_info=True,
            )
        finally:
            _state.busy = False
            _state.current_type = ""
            _work_queue.task_done()


async def supervised_worker() -> None:
    """
    Outer supervisor.  Restarts _worker_loop whenever it exits unexpectedly.

    CancelledError propagates normally so the task can be cancelled on shutdown.
    All other exceptions (programming errors, import failures, etc.) are caught,
    logged at CRITICAL level, and the loop restarts after a backoff delay.
    This prevents the silent-failure scenario where the worker dies but the
    web server keeps accepting /sync requests with no one to process them.
    """
    _backoff = 5.0  # seconds between restart attempts
    while True:
        try:
            await _worker_loop()
            # _worker_loop only returns normally if it was cancelled — but
            # CancelledError is raised, not returned, so this path means a
            # clean exit from a future code change.  Treat it as done.
            return
        except asyncio.CancelledError:
            logger.info("work_queue: worker cancelled — shutting down")
            raise
        except Exception as exc:
            _state.restarts += 1
            _state.last_error = f"SUPERVISOR: {type(exc).__name__}: {exc}"
            logger.critical(
                "work_queue: worker loop crashed (restart #%d) — %s. "
                "Restarting in %.0fs.",
                _state.restarts,
                exc,
                _backoff,
                exc_info=True,
            )
            await asyncio.sleep(_backoff)
            _backoff = min(_backoff * 2, 60.0)  # exponential backoff, cap at 60s
