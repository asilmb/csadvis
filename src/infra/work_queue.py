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
from dataclasses import dataclass
from datetime import UTC, datetime

from infra.steam_credentials import auth_credentials_exist
from scrapper.steam.client import SteamMarketClient
from scrapper.steam.formatter import InvalidHashNameError, to_api_name
from src.domain.connection import SessionLocal
from src.domain.item_service import ItemService

logger = logging.getLogger(__name__)

_work_queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=10)
_queue_shadow: list[str] = []  # mirrors queue job types for status display (no private API access)

# Max time (seconds) to stay in PAUSED_AUTH before giving up the current job.
_AUTH_WAIT_TIMEOUT_S = 1800  # 30 min

# Initial backoff delay between supervisor restarts (doubles on each crash, cap 60s).
_SUPERVISOR_RESTART_DELAY_S: float = 5.0

# Lazily-created asyncio.Event — set by POST /api/auth/steam to wake the worker
# immediately instead of waiting for the next 2-second poll tick.
_auth_event: asyncio.Event | None = None


def _get_auth_event() -> asyncio.Event:
    global _auth_event
    if _auth_event is None:
        _auth_event = asyncio.Event()
    return _auth_event


def signal_auth_ready() -> None:
    """Called by the auth endpoint after credentials are saved.
    Wakes the PAUSED_AUTH wait-loop in the worker without blocking."""
    ev = _get_auth_event()
    ev.set()


@dataclass
class _WorkerState:
    busy: bool = False
    auth_paused: bool = False
    current_type: str = ""
    last_job_at: datetime | None = None
    last_error: str = ""
    restarts: int = 0
    progress_current: int = 0
    progress_total: int = 0
    progress_started_at: datetime | None = None
    last_item_name: str = ""
    last_item_price: float = 0.0
    last_item_volume: int = 0


_state = _WorkerState()


def get_queue() -> asyncio.Queue[dict]:
    return _work_queue


def is_job_type_active(job_type: str) -> bool:
    """Return True if a job of this type is currently running or waiting in the queue."""
    if _state.busy and _state.current_type == job_type:
        return True
    return job_type in _queue_shadow


def enqueue(job: dict) -> None:
    """Put a job on the queue and register it in the shadow list for status display."""
    _work_queue.put_nowait(job)
    _queue_shadow.append(job.get("type", "?"))


def _calc_eta() -> int | None:
    cur = _state.progress_current
    tot = _state.progress_total
    started = _state.progress_started_at
    if not started or cur < 2 or tot <= cur:
        return None
    elapsed = (datetime.now(UTC).replace(tzinfo=None) - started).total_seconds()
    rate = cur / elapsed  # items per second
    remaining = tot - cur
    return int(remaining / rate)


def get_worker_state() -> dict:
    return {
        "busy": _state.busy,
        "auth_paused": _state.auth_paused,
        "current_type": _state.current_type,
        "last_job_at": _state.last_job_at.isoformat() if _state.last_job_at else None,
        "last_error": _state.last_error,
        "restarts": _state.restarts,
        "queue_size": _work_queue.qsize(),
        "queue_items": list(_queue_shadow),
        "progress_current": _state.progress_current,
        "progress_total": _state.progress_total,
        "eta_seconds": _calc_eta(),
        "last_item_name": _state.last_item_name,
        "last_item_price": _state.last_item_price,
        "last_item_volume": _state.last_item_volume,
    }


async def _wait_for_auth() -> bool:
    """
    Pause the worker until auth credentials appear or timeout expires.
    Non-blocking: yields control to the event loop every ≤2 s.
    Returns True when credentials are ready, False on timeout.
    """
    _state.auth_paused = True
    _state.current_type = "PAUSED_AUTH"
    logger.warning("work_queue: auth credentials missing — worker paused (max %ds)", _AUTH_WAIT_TIMEOUT_S)

    ev = _get_auth_event()
    ev.clear()

    loop = asyncio.get_running_loop()
    deadline = loop.time() + _AUTH_WAIT_TIMEOUT_S

    while not auth_credentials_exist():
        remaining = deadline - loop.time()
        if remaining <= 0:
            logger.error("work_queue: PAUSED_AUTH timed out after %ds — skipping job", _AUTH_WAIT_TIMEOUT_S)
            _state.auth_paused = False
            _state.last_error = "PAUSED_AUTH timed out"
            return False
        try:
            await asyncio.wait_for(ev.wait(), timeout=min(2.0, remaining))
        except asyncio.TimeoutError:
            pass  # just re-check credentials on next iteration

    ev.clear()
    _state.auth_paused = False
    logger.info("work_queue: auth credentials restored — resuming worker")
    return True


def _is_auth_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(kw in msg for kw in ("403", "forbidden", "unauthorized", "no_cookie", "invalid session", "no steam credentials"))


# ── Job handlers ──────────────────────────────────────────────────────────────


async def _ensure_auth() -> bool:
    """Return True if credentials are present, waiting for them if needed."""
    if auth_credentials_exist():
        return True
    ready = await _wait_for_auth()
    return ready


async def _handle_market_catalog(_job: dict) -> None:
    if not await _ensure_auth():
        logger.error("market_catalog: auth timeout — skipping")
        return
    from scrapper.runner import run_market_sync
    await run_market_sync()


async def _handle_price_poll(job: dict) -> None:
    """Fetch current Steam Market prices for containers."""
    if not await _ensure_auth():
        logger.error("price_poll: auth timeout — skipping")
        return

    import asyncio as _asyncio

    from infra.scrape_guard import (
        ScrapeBlocked,
        check_cooldown,
        create_session,
        finish_session,
        tick_session,
    )
    from scrapper.steam.client import _is_emergency_blocked
    from src.domain.models import DimContainer

    try:
        check_cooldown()
    except ScrapeBlocked as _blocked:
        logger.warning("price_poll: skipped — Steam cooldown active until %s", _blocked.cooldown_until)
        return

    single_cid = job.get("container_id")
    container_ids = job.get("container_ids")  # optional list of IDs to restrict poll
    include_blacklisted = job.get("include_blacklisted", False)
    session_id: int | None = job.get("session_id")  # resume existing snapshot

    with SessionLocal() as db:
        q = db.query(DimContainer).filter(
            DimContainer.is_blacklisted == (1 if include_blacklisted else 0)
        )
        if single_cid:
            q = q.filter(DimContainer.container_id == single_cid)
        elif container_ids:
            q = q.filter(DimContainer.container_id.in_(container_ids))
        containers = q.all()
        names = [(str(c.container_id), str(c.container_name)) for c in containers]
    random.shuffle(names)

    if not names:
        logger.info("price_poll: no containers to poll")
        return

    # Create snapshot only for bulk jobs (not single-container refresh)
    if not single_cid and session_id is None:
        session_id = await _asyncio.to_thread(create_session, "price_poll", [n[0] for n in names])

    logger.info("price_poll: polling %d containers", len(names))
    _state.progress_current = 0
    _state.progress_total = len(names)
    _state.progress_started_at = datetime.now(UTC).replace(tzinfo=None)
    _rate_limited = False
    async with SteamMarketClient() as client:
        for cid, name in names:
            try:
                api_name = to_api_name(name)
            except InvalidHashNameError:
                _state.progress_current += 1
                if session_id is not None:
                    await _asyncio.to_thread(tick_session, session_id)
                continue

            retry = True
            while retry:
                retry = False
                try:
                    overview = await client.fetch_price_overview(api_name)
                    raw = overview.get("lowest_price") or overview.get("median_price")
                    if raw is None:
                        continue
                    price_str = str(raw).replace("₸", "").replace(",", ".").strip()
                    price = float(price_str)
                    if price <= 0:
                        continue
                    raw_vol = overview.get("volume", "0")
                    volume = int(str(raw_vol).replace(",", "").strip() or "0")
                    if include_blacklisted:
                        # Save directly — bypass process_new_price blacklist check
                        from src.domain.sql_repositories import SqlAlchemyPriceRepository
                        with SessionLocal() as _db:
                            repo = SqlAlchemyPriceRepository(_db)
                            repo.save_jit_price(name, price, volume=volume, source="steam_live")
                            _db.commit()
                    else:
                        from config import settings as _settings
                        if price < _settings.ratio_floor:
                            logger.info(
                                "price_poll: auto-blacklisting %s (price=%.2f < ratio_floor=%.2f)",
                                name, price, _settings.ratio_floor,
                            )
                            with SessionLocal() as _db:
                                _c = _db.get(DimContainer, cid)
                                if _c:
                                    _c.is_blacklisted = 1
                                    _db.commit()
                            continue
                        svc = ItemService.open()
                        try:
                            await _asyncio.to_thread(svc.process_new_price, cid, price, volume)
                        finally:
                            svc.close()
                    _state.last_item_name = name
                    _state.last_item_price = price
                    _state.last_item_volume = volume
                except Exception as exc:
                    if _is_auth_error(exc):
                        ready = await _wait_for_auth()
                        if ready:
                            _state.current_type = "price_poll"
                            retry = True  # replay this container with fresh credentials
                        else:
                            logger.error("price_poll: giving up after auth timeout")
                            return
                    else:
                        logger.warning("price_poll: error for %s — %s", name, exc)

            _state.progress_current += 1
            if _is_emergency_blocked():
                logger.warning(
                    "price_poll: stopping — Steam 429 block active, session preserved for resume"
                )
                _rate_limited = True
                break
            if session_id is not None:
                await _asyncio.to_thread(tick_session, session_id)
            # Non-deterministic delay between requests to avoid Steam rate-limiting
            await _asyncio.sleep(random.uniform(8.0, 12.0))

    _remaining = _state.progress_total - _state.progress_current
    _state.progress_current = 0
    _state.progress_total = 0
    _state.progress_started_at = None
    if _rate_limited:
        logger.warning("price_poll: aborted due to Steam 429 — %d items unprocessed", _remaining)
        return
    if session_id is not None:
        await _asyncio.to_thread(finish_session, session_id)
    logger.info("price_poll: done")


async def _handle_sync_inventory(job: dict) -> None:
    if not await _ensure_auth():
        logger.error("sync_inventory: auth timeout — skipping")
        return
    from scrapper.runner import run_inventory_sync
    await run_inventory_sync(steam_id=job.get("steam_id"))


async def _handle_backfill_history(job: dict) -> None:
    if not await _ensure_auth():
        logger.error("backfill_history: auth timeout — skipping")
        return
    from infra.scrape_guard import ScrapeBlocked, check_cooldown
    try:
        check_cooldown()
    except ScrapeBlocked as _blocked:
        logger.warning("backfill_history: skipped — Steam cooldown active until %s", _blocked.cooldown_until)
        return
    from scrapper.runner import run_backfill_history
    await run_backfill_history(names=job.get("names"), session_id=job.get("session_id"))


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
        if _queue_shadow and _queue_shadow[0] == job_type:
            _queue_shadow.pop(0)
        _state.busy = True
        _state.current_type = job_type
        _state.last_job_at = datetime.now(UTC).replace(tzinfo=None)
        try:
            logger.info("work_queue: starting job type=%r", job_type)
            await _process_job(job)
            logger.info("work_queue: finished job type=%r", job_type)
            _state.last_error = ""
        except asyncio.CancelledError:
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
    _backoff = _SUPERVISOR_RESTART_DELAY_S
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
