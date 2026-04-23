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
from pydantic import BaseModel

from config import settings
from scrapper.steam_sync import sync_transactions, sync_wallet
from api.schemas import (
    SyncDispatchResponse,
    SyncTransactionsResponse,
    SyncWalletResponse,
)


class TypeFilterRequest(BaseModel):
    container_type: str = ""


def _names_for_type(container_type: str) -> list[str] | None:
    """Return container names matching container_type, or None for all."""
    if not container_type:
        return None
    from src.domain.connection import SessionLocal
    from src.domain.models import DimContainer
    with SessionLocal() as db:
        rows = db.query(DimContainer.container_name).filter(
            DimContainer.is_blacklisted == 0,
            DimContainer.container_type == container_type,
        ).all()
    return [r.container_name for r in rows]

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/sync", tags=["sync"])


def _enqueue(job: dict, label: str) -> SyncDispatchResponse:
    """Put a job on the in-process work queue. Returns ok=False when already active or queue is full."""
    try:
        from infra.work_queue import enqueue, is_job_type_active
        if is_job_type_active(job["type"]):
            return SyncDispatchResponse(ok=False, already_running=True, message=f"{label} уже выполняется.")
        enqueue(job)
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

def _get_steam_id() -> str:
    """Return STEAM_ID from Redis (set via UI form) or fall back to env/settings."""
    try:
        from infra.redis_client import get_redis as _get_redis
        val = _get_redis().get("cs2:config:steam_id")
        if val:
            return val.strip()
    except Exception:
        pass
    return (settings.steam_id or "").strip()


@router.post("/inventory", response_model=SyncDispatchResponse)
def sync_inventory_endpoint() -> SyncDispatchResponse:
    steam_id = _get_steam_id()
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
def sync_market_prices_endpoint(req: TypeFilterRequest = TypeFilterRequest()) -> SyncDispatchResponse:
    names = _names_for_type(req.container_type)
    if names is not None and not names:
        return SyncDispatchResponse(ok=False, already_running=False, message=f"Нет контейнеров типа «{req.container_type}».")
    job: dict = {"type": "price_poll"}
    if names is not None:
        from src.domain.connection import SessionLocal
        from src.domain.models import DimContainer
        with SessionLocal() as db:
            ids = [str(c.container_id) for c in db.query(DimContainer).filter(DimContainer.container_name.in_(names)).all()]
        job["container_ids"] = ids
        label = f"price_poll/{req.container_type} ({len(ids)})"
    else:
        label = "price_poll"
    return _enqueue(job, label)


@router.post("/market/prices/missing-volume", response_model=SyncDispatchResponse)
def sync_prices_missing_volume_endpoint() -> SyncDispatchResponse:
    """Enqueue price_poll only for containers that have no steam_live record with volume > 0."""
    from sqlalchemy import exists

    from src.domain.connection import SessionLocal
    from src.domain.models import DimContainer, FactContainerPrice
    with SessionLocal() as db:
        has_volume = exists().where(
            (FactContainerPrice.container_id == DimContainer.container_id)
            & (FactContainerPrice.source == "steam_live")
            & (FactContainerPrice.volume_7d > 0)
        )
        ids = [
            str(c.container_id) for c in
            db.query(DimContainer)
            .filter(DimContainer.is_blacklisted == 0, ~has_volume)
            .all()
        ]
    if not ids:
        return SyncDispatchResponse(ok=True, already_running=False, message="Все контейнеры уже имеют volume.")
    return _enqueue({"type": "price_poll", "container_ids": ids}, f"price_poll/missing-volume ({len(ids)})")


# ── Backfill price history (all containers, optional type filter) ─────────────

@router.post("/backfill", response_model=SyncDispatchResponse)
def sync_backfill_endpoint(req: TypeFilterRequest = TypeFilterRequest()) -> SyncDispatchResponse:
    names = _names_for_type(req.container_type)
    if names is not None and not names:
        return SyncDispatchResponse(ok=False, already_running=False, message=f"Нет контейнеров типа «{req.container_type}».")
    label = f"backfill_history/{req.container_type} ({len(names)})" if names else "backfill_history"
    return _enqueue({"type": "backfill_history", "names": names}, label)


# ── Backfill price history (blacklisted containers) ───────────────────────────

@router.post("/backfill/blacklisted", response_model=SyncDispatchResponse)
def sync_backfill_blacklisted_endpoint() -> SyncDispatchResponse:
    from src.domain.connection import SessionLocal
    from src.domain.models import DimContainer
    with SessionLocal() as db:
        names = [str(r.container_name) for r in db.query(DimContainer.container_name).filter(DimContainer.is_blacklisted == 1).all()]
    if not names:
        return SyncDispatchResponse(ok=False, already_running=False, message="No blacklisted containers.")
    return _enqueue({"type": "backfill_history", "names": names}, f"backfill_history/blacklisted ({len(names)})")


# ── Sync prices (blacklisted containers) ──────────────────────────────────────

@router.post("/market/prices/blacklisted", response_model=SyncDispatchResponse)
def sync_prices_blacklisted_endpoint() -> SyncDispatchResponse:
    return _enqueue({"type": "price_poll", "include_blacklisted": True}, "price_poll/blacklisted")


# ── Backfill price history (containers with open positions only) ───────────────

@router.post("/backfill/active", response_model=SyncDispatchResponse)
def sync_backfill_active_endpoint() -> SyncDispatchResponse:
    from src.domain.connection import SessionLocal
    from src.domain.models import DimContainer, Position, PositionStatus
    with SessionLocal() as db:
        names = [
            row.container_name
            for row in (
                db.query(DimContainer.container_name)
                .join(Position, DimContainer.container_name == Position.market_hash_name)
                .filter(Position.status == PositionStatus.OPEN, DimContainer.is_blacklisted == 0)
                .distinct()
                .all()
            )
        ]
    if not names:
        return SyncDispatchResponse(ok=False, already_running=False, message="No open positions found.")
    return _enqueue({"type": "backfill_history", "names": names}, f"backfill_history ({len(names)} containers)")
