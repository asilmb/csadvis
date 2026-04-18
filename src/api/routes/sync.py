"""
Sync endpoints — on-demand Steam data refresh.

POST /sync/wallet           — fetch wallet balance from Steam, persist to cache
POST /sync/inventory        — enqueue inventory sync job
POST /sync/transactions     — fetch Steam Market transaction history
POST /sync/market/catalog   — enqueue market catalog discovery job
POST /sync/market/prices    — enqueue price poll job
"""

from __future__ import annotations

import asyncio
import logging

from fastapi import APIRouter

from src.api.schemas import (
    SyncDispatchResponse,
    SyncTransactionsResponse,
    SyncWalletResponse,
)
from config import settings
from scrapper.steam_sync import sync_transactions, sync_wallet

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/sync", tags=["sync"])


def _enqueue(job: dict, label: str) -> SyncDispatchResponse:
    """Put a job on the in-process work queue. Returns ok=False when queue is full."""
    try:
        from infra.work_queue import get_queue
        get_queue().put_nowait(job)
        return SyncDispatchResponse(ok=True, already_running=False, message=f"{label} enqueued.")
    except asyncio.QueueFull:
        return SyncDispatchResponse(ok=False, already_running=True, message=f"{label} already queued (queue full).")
    except Exception as exc:
        logger.error("sync: enqueue failed for %s: %s", label, exc)
        return SyncDispatchResponse(ok=False, already_running=False, message=f"Enqueue error: {exc}")


# ── Wallet (direct sync, no queue) ───────────────────────────────────────────

@router.post("/wallet", response_model=SyncWalletResponse)
def sync_wallet_endpoint() -> SyncWalletResponse:
    result = sync_wallet()
    return SyncWalletResponse(
        ok=result.ok,
        balance=result.balance,
        message=result.message,
        error_code=result.error_code,
    )


# ── Inventory sync ────────────────────────────────────────────────────────────

@router.post("/inventory", response_model=SyncDispatchResponse)
def sync_inventory_endpoint() -> SyncDispatchResponse:
    steam_id = (settings.steam_id or "").strip()
    if not steam_id:
        return SyncDispatchResponse(ok=False, already_running=False, message="No STEAM_ID configured.")
    return _enqueue({"type": "sync_inventory", "steam_id": steam_id}, "sync_inventory")


# ── Transactions (direct sync, no queue) ─────────────────────────────────────

@router.post("/transactions", response_model=SyncTransactionsResponse)
def sync_transactions_endpoint() -> SyncTransactionsResponse:
    result = sync_transactions()
    return SyncTransactionsResponse(
        ok=result.ok,
        buy_count=result.buy_count,
        sell_count=result.sell_count,
        message=result.message,
        error_code=result.error_code,
    )


# ── Market catalog discovery ──────────────────────────────────────────────────

@router.post("/market/catalog", response_model=SyncDispatchResponse)
def sync_market_catalog_endpoint() -> SyncDispatchResponse:
    return _enqueue({"type": "market_catalog"}, "market_catalog")


# ── Market price refresh ──────────────────────────────────────────────────────

@router.post("/market/prices", response_model=SyncDispatchResponse)
def sync_market_prices_endpoint() -> SyncDispatchResponse:
    return _enqueue({"type": "price_poll"}, "price_poll")
