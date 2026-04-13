"""
Sync endpoints — on-demand Steam data refresh.

POST /sync/wallet           — fetch wallet balance from Steam, persist to cache
POST /sync/inventory        — dispatch run_inventory_sync Celery task (Redis-locked)
POST /sync/transactions     — fetch Steam Market transaction history
POST /sync/market/catalog   — dispatch run_market_sync Celery task (Redis-locked)
POST /sync/market/prices    — dispatch poll_container_prices_task Celery task (Redis-locked)

Inventory / catalog / prices endpoints use a Redis SET NX lock to prevent
duplicate dispatches when the user clicks rapidly or calls the API concurrently.
Lock TTL matches the per-task TASK_TTL constant so it expires once the task
is expected to finish (with a generous ceiling).
"""

from __future__ import annotations

import logging

from fastapi import APIRouter

from api.schemas import (
    SyncDispatchResponse,
    SyncTransactionsResponse,
    SyncWalletResponse,
)
from config import settings
from scrapper.steam_sync import sync_transactions, sync_wallet

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/sync", tags=["sync"])

# Redis lock key → (lock TTL seconds, human label)
_LOCK_META: dict[str, tuple[int, str]] = {
    "sync:lock:inventory":     (120, "run_inventory_sync"),
    "sync:lock:market_catalog": (600, "run_market_sync"),
    "sync:lock:market_prices":  (600, "poll_container_prices_task"),
}


def _try_dispatch(lock_key: str, dispatch_fn) -> SyncDispatchResponse:
    """
    Acquire Redis NX lock → dispatch Celery task → return result.

    Returns ok=False (already_running=True) when another caller holds the lock.
    """
    ttl, label = _LOCK_META[lock_key]
    try:
        from infra.redis_client import get_redis
        r = get_redis()
        acquired = r.set(lock_key, "1", nx=True, ex=ttl)
        if not acquired:
            return SyncDispatchResponse(
                ok=False,
                already_running=True,
                message=f"{label} is already running.",
            )
    except Exception as exc:
        logger.warning("sync: Redis lock check failed for %s: %s", lock_key, exc)
        # Fall through — dispatch without lock rather than blocking the user.

    try:
        task = dispatch_fn()
        return SyncDispatchResponse(
            ok=True,
            already_running=False,
            task_id=task.id if task else None,
            message=f"{label} dispatched.",
        )
    except Exception as exc:
        logger.error("sync: dispatch failed for %s: %s", lock_key, exc)
        return SyncDispatchResponse(
            ok=False,
            already_running=False,
            message=f"Dispatch error: {exc}",
        )


# ── Wallet (unchanged — direct sync, no Celery) ───────────────────────────────

@router.post("/wallet", response_model=SyncWalletResponse)
def sync_wallet_endpoint() -> SyncWalletResponse:
    """
    Fetch Steam wallet balance from steamcommunity.com/market/ and persist to cache.

    Requires STEAM_LOGIN_SECURE cookie in .env.
    On failure returns cached balance (if any) with ok=False.
    """
    result = sync_wallet()
    return SyncWalletResponse(
        ok=result.ok,
        balance=result.balance,
        message=result.message,
        error_code=result.error_code,
    )


# ── Inventory sync (Celery, Redis-locked) ─────────────────────────────────────

@router.post("/inventory", response_model=SyncDispatchResponse)
def sync_inventory_endpoint() -> SyncDispatchResponse:
    """
    Dispatch run_inventory_sync Celery task for the configured STEAM_ID.

    Returns 409-equivalent (ok=False, already_running=True) when a task is
    already in flight (Redis lock held).
    """
    steam_id = (settings.steam_id or "").strip()
    if not steam_id:
        return SyncDispatchResponse(
            ok=False,
            already_running=False,
            message="No STEAM_ID configured.",
        )

    def _dispatch():
        from scrapper.runner import run_inventory_sync
        return run_inventory_sync.delay(steam_id)

    return _try_dispatch("sync:lock:inventory", _dispatch)


# ── Transactions (unchanged — direct sync, no Celery) ────────────────────────

@router.post("/transactions", response_model=SyncTransactionsResponse)
def sync_transactions_endpoint() -> SyncTransactionsResponse:
    """
    Fetch Steam Market CS2 transaction history (up to 10 pages).

    Does NOT persist to DB — persistence is handled by the dashboard callback
    so the service layer stays stateless.
    Requires STEAM_LOGIN_SECURE cookie in .env.
    """
    result = sync_transactions()
    return SyncTransactionsResponse(
        ok=result.ok,
        buy_count=result.buy_count,
        sell_count=result.sell_count,
        message=result.message,
        error_code=result.error_code,
    )


# ── Market catalog discovery (Celery, Redis-locked) ───────────────────────────

@router.post("/market/catalog", response_model=SyncDispatchResponse)
def sync_market_catalog_endpoint() -> SyncDispatchResponse:
    """
    Dispatch run_market_sync Celery task to discover new CS2 containers from
    Steam Community Market.

    Protected by a 10-minute Redis lock to prevent duplicate scrape runs.
    """
    def _dispatch():
        from scrapper.runner import run_market_sync
        return run_market_sync.delay()

    return _try_dispatch("sync:lock:market_catalog", _dispatch)


# ── Market price refresh (Celery, Redis-locked) ───────────────────────────────

@router.post("/market/prices", response_model=SyncDispatchResponse)
def sync_market_prices_endpoint() -> SyncDispatchResponse:
    """
    Dispatch poll_container_prices_task Celery task to refresh current prices
    for all tracked containers.

    Protected by a 10-minute Redis lock to prevent overlapping price polls.
    """
    def _dispatch():
        from scheduler.tasks import poll_container_prices_task
        return poll_container_prices_task.delay()

    return _try_dispatch("sync:lock:market_prices", _dispatch)
