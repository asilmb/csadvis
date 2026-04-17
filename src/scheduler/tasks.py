"""
Celery tasks (PV-04).

fetch_steam_price(item_id)    — fetch current Steam Market price for one container.
sync_inventory_task()         — periodic Beat task: sync Steam inventory + reconcile.
poll_container_prices_task()  — periodic Beat task: fetch prices for all containers.

Stealth contract
----------------
On HTTP 429 from Steam:
  1. Set Redis key STEALTH_BLOCK_EXPIRES (TTL = 6 h) so other tasks can check it.
  2. self.retry(countdown=21600) — retry after 6 hours.
"""

from __future__ import annotations

import os
import subprocess
import time

import redis as redis_lib
import structlog

from infra.metrics import inc_prices_fetched, inc_steam_429
from scheduler.celery_app import app

logger = structlog.get_logger()

_STEALTH_KEY = "STEALTH_BLOCK_EXPIRES"
_STEALTH_TTL = 6 * 3600          # 6 hours in seconds
_RETRY_COUNTDOWN = 21600          # 6 hours — used for non-429 stealth-block retries

# Exponential backoff for 429 responses: 120 s → 240 s → 480 s … capped at 6 h.
_BACKOFF_BASE = 120


def _exp_backoff(retries: int) -> int:
    """Return Celery countdown (seconds) for a 429 retry, growing exponentially."""
    return min(_RETRY_COUNTDOWN, _BACKOFF_BASE * (2 ** retries))

_redis_url: str = os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0")


def _redis() -> redis_lib.Redis:
    """Return a short-lived Redis client (one per call — Celery workers are multi-process)."""
    return redis_lib.from_url(_redis_url, decode_responses=True)


def _is_stealth_blocked() -> bool:
    """Return True if a prior 429 has placed a 6-hour block in Redis."""
    try:
        return bool(_redis().exists(_STEALTH_KEY))
    except Exception:
        return False


def _trigger_stealth_block(reason: str) -> None:
    """Write STEALTH_BLOCK_EXPIRES to Redis with a 6-hour TTL."""
    try:
        _redis().setex(_STEALTH_KEY, _STEALTH_TTL, reason)
        logger.warning("stealth_block_set", service="tasks", ttl_hours=6, reason=reason)
    except Exception as exc:
        logger.error("stealth_block_write_failed", service="tasks", error=str(exc))


# ─── fetch_steam_price ────────────────────────────────────────────────────────


@app.task(bind=True, max_retries=3, default_retry_delay=300)
def fetch_steam_price(self, item_id: str) -> float | None:
    """
    Fetch the current Steam Market price for a single container.

    Args:
        item_id: dim_containers.container_id (UUID string).

    Returns:
        Numeric price, or None if price is unavailable / <= 0.

    Stealth:
        On HTTP 429: sets STEALTH_BLOCK_EXPIRES in Redis and retries after 6 h.
    """
    if _is_stealth_blocked():
        logger.info("fetch_skipped_stealth_block", service="tasks", item_id=item_id)
        raise self.retry(countdown=_RETRY_COUNTDOWN)

    # ── Resolve container name ─────────────────────────────────────────────────
    from src.domain.connection import SessionLocal
    from src.domain.models import DimContainer

    with SessionLocal() as db:
        container = db.get(DimContainer, item_id)

    if container is None:
        logger.warning("fetch_container_not_found", service="tasks", item_id=item_id)
        return None

    if container.is_blacklisted:
        logger.debug("fetch_skipped_blacklisted", service="tasks", item_id=item_id, name=container.container_name)
        return None

    container_name: str = container.container_name

    # ── Fetch price via Steam client ───────────────────────────────────────────
    import asyncio

    from scrapper.steam.client import SteamMarketClient
    from scrapper.steam.formatter import InvalidHashNameError, to_api_name

    try:
        api_name = to_api_name(container_name)
    except InvalidHashNameError as exc:
        logger.warning("fetch_invalid_hash_name", service="tasks", item_id=item_id, name=container_name, error=str(exc))
        return None

    _attempt = self.request.retries

    async def _fetch() -> dict:
        async with SteamMarketClient(attempt=_attempt) as client:
            return await client.fetch_price_overview(api_name)

    def _run(headers: dict) -> dict:  # noqa: ARG001
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(_fetch())
        finally:
            loop.close()
            asyncio.set_event_loop(None)

    try:
        overview: dict = _run({})
    except Exception as exc:
        err_str = str(exc)
        if "429" in err_str:
            inc_steam_429()
            _trigger_stealth_block(f"fetch_steam_price:{container_name}")
            countdown = _exp_backoff(_attempt)
            logger.warning(
                "fetch_429_backoff",
                service="tasks",
                item_id=item_id,
                name=container_name,
                attempt=_attempt,
                countdown_s=countdown,
            )
            raise self.retry(countdown=countdown, exc=exc)
        logger.error("fetch_steam_error", service="tasks", item_id=item_id, name=container_name, error=str(exc))
        raise self.retry(exc=exc)

    # ── Extract & validate price ───────────────────────────────────────────────
    raw_price = overview.get("lowest_price") or overview.get("median_price")
    if raw_price is None:
        logger.debug("fetch_no_price_in_overview", service="tasks", item_id=item_id, name=container_name)
        return None

    # Strip currency symbols and parse float
    try:
        price_str = str(raw_price).replace("₸", "").replace(",", ".").strip()
        price = float(price_str)
    except (ValueError, TypeError):
        logger.warning("fetch_unparseable_price", service="tasks", item_id=item_id, name=container_name, raw=raw_price)
        return None

    if price <= 0:
        logger.debug("fetch_price_zero_or_negative", service="tasks", item_id=item_id, name=container_name)
        return None

    # ── Persist via ItemService (validation + save) ────────────────────────────
    from src.domain.item_service import ItemService

    _t0 = time.monotonic()
    svc = ItemService.open()
    try:
        saved = svc.process_new_price(item_id, price)
    except Exception as exc:
        logger.error(
            "fetch_save_failed",
            service="tasks",
            item_id=item_id,
            name=container_name,
            error=str(exc),
        )
        saved = False
    finally:
        svc.close()

    duration_ms = round((time.monotonic() - _t0) * 1000)

    if not saved:
        logger.warning(
            "fetch_price_not_saved",
            service="tasks",
            item_id=item_id,
            name=container_name,
            price=price,
            duration_ms=duration_ms,
        )
        return None

    inc_prices_fetched()
    logger.info(
        "fetch_price_saved",
        service="tasks",
        item_id=item_id,
        name=container_name,
        price=price,
        duration_ms=duration_ms,
    )
    return price


# ─── poll_container_prices_task ───────────────────────────────────────────────


@app.task(bind=True, max_retries=1)
def poll_container_prices_task(self) -> dict:
    """
    Beat task: enqueue fetch_steam_price for every non-blacklisted container.

    Also triggers backfill_history_task for any containers that have no price
    history yet (newly discovered containers).

    Returns a summary dict {queued: int, skipped_blacklisted: int, skipped_block: int,
    backfill_triggered: int}.
    """
    if _is_stealth_blocked():
        logger.info("poll_skipped_stealth_block", service="tasks")
        return {"queued": 0, "skipped_blacklisted": 0, "skipped_block": 1, "backfill_triggered": 0}

    from src.domain.connection import SessionLocal
    from src.domain.models import DimContainer, FactContainerPrice

    with SessionLocal() as db:
        containers = db.query(DimContainer).filter(
            DimContainer.is_blacklisted == False  # noqa: E712
        ).all()
        container_ids = [c.container_id for c in containers]

        # Detect containers with no price history at all (newly added)
        containers_with_history = {
            str(cid)
            for (cid,) in db.query(FactContainerPrice.container_id).distinct().all()
        }
        new_container_names = [
            c.container_name
            for c in containers
            if str(c.container_id) not in containers_with_history
        ]

    queued = 0
    for cid in container_ids:
        fetch_steam_price.delay(cid)
        queued += 1

    # Trigger backfill for containers that have never had price history fetched
    backfill_triggered = 0
    if new_container_names:
        backfill_history_task.delay(names=new_container_names)
        backfill_triggered = len(new_container_names)
        logger.info(
            "poll_backfill_triggered",
            service="tasks",
            new_containers=backfill_triggered,
        )

    logger.info("poll_enqueued", service="tasks", queued=queued)
    return {
        "queued": queued,
        "skipped_blacklisted": 0,
        "skipped_block": 0,
        "backfill_triggered": backfill_triggered,
    }


# ─── backfill_history_task ────────────────────────────────────────────────────

_BACKFILL_CHECKPOINT_H: int = 24   # skip container if it has a price row within this window


@app.task(bind=True, max_retries=2, default_retry_delay=_BACKFILL_CHECKPOINT_H * 3600)
def backfill_history_task(self, names: list[str] | None = None) -> dict:
    """
    Fetch and persist daily price history for containers that lack it.

    Args:
        names: Optional list of container_name values to process.
               When None or empty, all non-blacklisted containers are processed.

    Flow per container:
      1. Checkpoint: if the container already has a FactContainerPrice row within
         _BACKFILL_CHECKPOINT_H (24 h), skip it — history is up-to-date.
      2. Fetch daily price history from Steam (async SteamMarketClient).
      3. Filter rows to only those newer than the container's current max date.
      4. Persist each container's rows in its own SessionLocal + commit — independent
         of other items so a failure on item N cannot roll back items 1…N-1.
      5. Error isolation: any per-item failure is logged and the loop continues.

    Task always completes without raising; failed items will be retried on the next
    invocation because they will not have a recent checkpoint row.

    On HTTP 429 / stealth block: sets STEALTH_BLOCK_EXPIRES and retries after 24 h.
    """
    import asyncio
    import uuid

    from sqlalchemy import func

    from src.domain.connection import SessionLocal
    from src.domain.models import DimContainer, FactContainerPrice

    if _is_stealth_blocked():
        logger.info("backfill_skipped_stealth_block", service="tasks")
        raise self.retry(countdown=_RETRY_COUNTDOWN)

    names_filter: list[str] = list(names or [])

    # ── Load containers + max existing dates (one read-only query) ────────────
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
        max_dates: dict[str, "date"] = {
            cid_to_name[str(cid)]: ts.date()
            for cid, ts in max_ts_rows
            if str(cid) in cid_to_name and ts is not None
        }
        latest_ts: dict[str, "datetime"] = {
            cid_to_name[str(cid)]: ts
            for cid, ts in max_ts_rows
            if str(cid) in cid_to_name and ts is not None
        }

    if not id_map:
        logger.info("backfill_no_containers", service="tasks")
        return {"status": "ok", "saved": 0, "skipped_checkpoint": 0, "errors": 0}

    ordered_names: list[str] = (
        [n for n in names_filter if n in id_map] if names_filter else list(id_map.keys())
    )

    logger.info("backfill_started", service="tasks", total=len(ordered_names))

    from scrapper.steam.client import SteamMarketClient
    from scrapper.steam.formatter import InvalidHashNameError, to_api_name

    _attempt = self.request.retries

    try:
        steam_client = SteamMarketClient(attempt=_attempt)
    except RuntimeError as exc:
        logger.warning("backfill_no_cookie", service="tasks", error=str(exc))
        return {"status": "skipped", "reason": "no_cookie", "saved": 0, "skipped_checkpoint": 0, "errors": 0}

    from datetime import UTC, date, datetime, timedelta

    checkpoint_cutoff = datetime.now(UTC).replace(tzinfo=None) - timedelta(hours=_BACKFILL_CHECKPOINT_H)

    saved_total = 0
    skipped_checkpoint = 0
    errors = 0

    for name in ordered_names:
        cid = id_map[name]

        # ── Checkpoint: skip if recently updated ──────────────────────────────
        last_ts = latest_ts.get(name)
        if last_ts is not None and last_ts >= checkpoint_cutoff:
            skipped_checkpoint += 1
            logger.debug("backfill_checkpoint_hit", service="tasks", name=name)
            continue

        # ── Fetch from Steam (async SteamMarketClient) ───────────────────────
        try:
            api_name = to_api_name(name)
        except InvalidHashNameError as exc:
            logger.warning("backfill_invalid_name", service="tasks", name=name, error=str(exc))
            errors += 1
            continue

        async def _fetch(_name: str = api_name, _client: SteamMarketClient = steam_client) -> list[dict]:
            return await _client.fetch_history(_name)

        def _run(_headers: dict, _name: str = api_name) -> list[dict]:  # noqa: ARG001
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(_fetch(_name))
            finally:
                loop.close()
                asyncio.set_event_loop(None)

        try:
            rows: list[dict] = _run({})
        except Exception as exc:
            err_str = str(exc)
            if "429" in err_str:
                inc_steam_429()
                _trigger_stealth_block(f"backfill_history_task:{name}")
                countdown = _exp_backoff(_attempt)
                logger.warning(
                    "backfill_429_backoff",
                    service="tasks",
                    name=name,
                    attempt=_attempt,
                    countdown_s=countdown,
                )
                raise self.retry(countdown=countdown, exc=exc)
            logger.error("backfill_fetch_error", service="tasks", name=name, error=str(exc))
            errors += 1
            continue

        if not rows:
            logger.debug("backfill_empty_rows", service="tasks", name=name)
            continue

        # ── Filter to only new rows (date > max existing) ─────────────────────
        existing_max: date = max_dates.get(name, date.min)
        new_rows = [r for r in rows if r["date"].date() > existing_max]

        if not new_rows:
            logger.debug("backfill_all_rows_known", service="tasks", name=name, total=len(rows))
            continue

        # ── Persist in its own session (independent commit) ───────────────────
        try:
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

            saved_total += len(new_rows)
            max_dates[name] = max(r["date"].date() for r in new_rows)
            latest_ts[name] = datetime.now(UTC).replace(tzinfo=None)
            logger.debug("backfill_rows_saved", service="tasks", name=name, count=len(new_rows))

        except Exception as exc:
            logger.error("backfill_db_write_error", service="tasks", name=name, error=str(exc))
            errors += 1

    logger.info(
        "backfill_complete",
        service="tasks",
        saved=saved_total,
        skipped_checkpoint=skipped_checkpoint,
        errors=errors,
    )
    return {
        "status": "ok",
        "saved": saved_total,
        "skipped_checkpoint": skipped_checkpoint,
        "errors": errors,
    }


# ─── sync_inventory_task ──────────────────────────────────────────────────────


@app.task(bind=True, max_retries=3, default_retry_delay=300)
def sync_inventory_task(self) -> dict:
    """
    Beat task: fetch Steam CS2 inventory and reconcile positions.

    On HTTP 429: sets STEALTH_BLOCK_EXPIRES and retries after 6 h.

    Returns a summary dict on success.
    """
    if _is_stealth_blocked():
        logger.info("sync_skipped_stealth_block", service="tasks")
        raise self.retry(countdown=_RETRY_COUNTDOWN)

    from config import settings

    steam_id = settings.steam_id.strip()
    if not steam_id:
        logger.warning("sync_no_steam_id", service="tasks")
        return {"status": "skipped", "reason": "no steam_id"}

    import asyncio

    from scrapper.steam_inventory import SteamInventoryClient

    async def _fetch() -> list[dict]:
        async with SteamInventoryClient() as client:
            return await client.fetch_assets(steam_id)

    def _run(headers: dict) -> list[dict]:  # noqa: ARG001
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(_fetch())
        finally:
            loop.close()
            asyncio.set_event_loop(None)

    _attempt = self.request.retries

    try:
        items: list[dict] = _run({})
    except Exception as exc:
        err_str = str(exc)
        if "429" in err_str:
            inc_steam_429()
            _trigger_stealth_block("sync_inventory_task")
            countdown = _exp_backoff(_attempt)
            logger.warning(
                "sync_429_backoff",
                service="tasks",
                attempt=_attempt,
                countdown_s=countdown,
            )
            raise self.retry(countdown=countdown, exc=exc)
        logger.error("sync_steam_error", service="tasks", error=str(exc))
        raise self.retry(exc=exc)

    if not items:
        logger.info("sync_empty_inventory", service="tasks", steam_id=steam_id)
        return {"status": "ok", "items": 0, "reconciled": False}

    # ── Persist trade-ban dates ────────────────────────────────────────────────
    from src.domain.connection import SessionLocal
    from src.domain.sql_repositories import (
        SqlAlchemyInventoryRepository,
        SqlAlchemyPositionRepository,
    )
    from src.domain.reconciler import PositionReconciler

    with SessionLocal() as db:
        inv_repo = SqlAlchemyInventoryRepository(db)
        for item in items:
            inv_repo.update_trade_unlock_at(
                item_name=item["market_hash_name"],
                unlock_at=item.get("trade_unlock_at"),
            )
        db.commit()

    # ── Reconcile asset IDs ────────────────────────────────────────────────────
    with SessionLocal() as db:
        pos_repo = SqlAlchemyPositionRepository(db)
        rec = PositionReconciler().sync(items, pos_repo)
        db.commit()

    logger.info(
        "sync_complete",
        service="tasks",
        items=len(items),
        matched_direct=rec.matched_direct,
        matched_listing=rec.matched_listing,
        matched_fifo=rec.matched_fifo,
    )
    return {
        "status": "ok",
        "items": len(items),
        "matched_direct": rec.matched_direct,
        "matched_listing": rec.matched_listing,
        "matched_fifo": rec.matched_fifo,
    }


# ─── cleanup_old_history_task ─────────────────────────────────────────────────


@app.task(bind=True, max_retries=1, default_retry_delay=300)
def cleanup_old_history_task(self) -> dict:
    """
    Beat task: downsample price records older than 90 days into daily aggregates.

    Runs inside a single DB transaction.  On any error the transaction is rolled
    back and the task retries once after 5 minutes.

    Returns a summary dict: {status, rows_deleted, summaries_inserted}.
    """
    from src.domain.connection import SessionLocal
    from src.domain.postgres_repo import PostgresRepository

    _t0 = time.monotonic()
    logger.info("downsampling_started", service="tasks")

    try:
        with SessionLocal() as db:
            repo = PostgresRepository(db)
            rows_deleted, summaries = repo.downsample_old_prices(days_threshold=90)
            db.commit()
    except Exception as exc:
        logger.error("downsampling_failed", service="tasks", error=str(exc))
        raise self.retry(exc=exc)

    duration_ms = round((time.monotonic() - _t0) * 1000)
    logger.info(
        "downsampling_complete",
        service="tasks",
        message=f"[Downsampling] Optimized {rows_deleted} rows into {summaries} daily points.",
        rows_deleted=rows_deleted,
        summaries_inserted=summaries,
        duration_ms=duration_ms,
    )
    return {
        "status": "ok",
        "rows_deleted": rows_deleted,
        "summaries_inserted": summaries,
        "duration_ms": duration_ms,
    }


# ─── daily_backup_task ────────────────────────────────────────────────────────


@app.task(bind=True, max_retries=2, default_retry_delay=300)
def daily_backup_task(self) -> dict:
    """
    Beat task: run backup_db.sh once daily at 03:00 UTC.

    Executes /app/scripts/backup_db.sh in a subprocess and captures output.
    On script failure (non-zero exit), retries up to 2 times with a 5-min delay.

    Returns a summary dict: {status, returncode, stdout_tail}.
    """
    script_path = os.getenv("BACKUP_SCRIPT_PATH", "/app/scripts/backup_db.sh")
    _t0 = time.monotonic()

    logger.info("backup_started", service="tasks", script=script_path)

    try:
        result = subprocess.run(
            ["bash", script_path],
            capture_output=True,
            text=True,
            timeout=600,  # 10-minute hard limit
        )
    except FileNotFoundError:
        logger.error(
            "backup_script_not_found",
            service="tasks",
            script=script_path,
        )
        raise self.retry(countdown=300)
    except subprocess.TimeoutExpired:
        logger.error("backup_timeout", service="tasks", script=script_path)
        raise self.retry(countdown=300)

    duration_ms = round((time.monotonic() - _t0) * 1000)
    stdout_tail = result.stdout.strip().splitlines()[-5:] if result.stdout else []

    if result.returncode != 0:
        logger.error(
            "backup_failed",
            service="tasks",
            returncode=result.returncode,
            stderr=result.stderr.strip()[-500:] if result.stderr else "",
            duration_ms=duration_ms,
        )
        raise self.retry(countdown=300)

    logger.info(
        "backup_complete",
        service="tasks",
        returncode=result.returncode,
        stdout_tail=stdout_tail,
        duration_ms=duration_ms,
    )
    return {
        "status": "ok",
        "returncode": result.returncode,
        "stdout_tail": stdout_tail,
        "duration_ms": duration_ms,
    }
