"""
Items endpoints (PV-08) — market overview via ItemService.

GET /items            — market overview (all containers, optional ?tier= filter)
GET /items/{id}       — detail snapshot for one container
GET /items/{id}/history — full price history as PriceHistoryDTO list
"""

from __future__ import annotations

from typing import Optional

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from domain.connection import get_db_dep
from domain.dtos import ItemDTO, PriceHistoryDTO

logger = structlog.get_logger()
router = APIRouter(prefix="/items", tags=["items"])


# ─── Dependency ───────────────────────────────────────────────────────────────


def _get_item_service(db: Session = Depends(get_db_dep)):
    """Inject ItemService bound to the request's DB session."""
    from domain.sql_repositories import SqlAlchemyInventoryRepository
    from domain.item_service import ItemService

    return ItemService(SqlAlchemyInventoryRepository(db))


def _get_tier_map(db: Session) -> dict[str, int]:
    """
    Return {container_id: tier} from SystemSettings rows keyed 'tier:{id}'.
    Returns empty dict on any error (tier filtering degrades gracefully).
    """
    try:
        from domain.models import SystemSettings
        from sqlalchemy import select

        rows = db.execute(
            select(SystemSettings).where(SystemSettings.key.like("tier:%"))
        ).scalars().all()
        return {
            row.key[len("tier:"):]: int(row.value)
            for row in rows
            if row.value is not None
        }
    except Exception as exc:
        logger.warning("tier_map_load_failed", service="items_route", error=str(exc))
        return {}


# ─── Endpoints ────────────────────────────────────────────────────────────────


@router.get(
    "",
    response_model=list[ItemDTO],
    summary="Market overview",
    description=(
        "Returns the current market snapshot for all tracked CS2 containers. "
        "Optional `tier` query parameter filters to items assigned that tier "
        "(1 = Active, 3 = Cold) — items with no tier assignment are included "
        "only when no filter is applied."
    ),
)
def list_items(
    tier: Optional[int] = Query(
        default=None,
        ge=1,
        le=3,
        description="Filter by tier (1 = Active, 2 = Watchlist, 3 = Cold)",
    ),
    db: Session = Depends(get_db_dep),
    svc=Depends(_get_item_service),
) -> list[ItemDTO]:
    items: list[ItemDTO] = svc.get_market_overview()

    if tier is not None:
        tier_map = _get_tier_map(db)
        items = [it for it in items if tier_map.get(it.id) == tier]

    return items


@router.get(
    "/{item_id}",
    response_model=ItemDTO,
    summary="Item detail",
    description=(
        "Returns the full market snapshot for a single container identified "
        "by its UUID. Includes volatility, net proceeds, and suspicion flag."
    ),
)
def get_item(
    item_id: str,
    svc=Depends(_get_item_service),
) -> ItemDTO:
    item = svc.get_item_details(item_id)
    if item is None:
        raise HTTPException(status_code=404, detail=f"Item '{item_id}' not found or has no price data")
    return item


@router.get(
    "/{item_id}/history",
    response_model=list[PriceHistoryDTO],
    summary="Price history",
    description=(
        "Returns the full ordered price history for a container. "
        "Includes both hourly detail rows and daily aggregates "
        "(source='daily_aggregate') for records older than 90 days."
    ),
)
def get_item_history(
    item_id: str,
    svc=Depends(_get_item_service),
) -> list[PriceHistoryDTO]:
    history = svc.get_price_history(item_id)
    if not history:
        # Distinguish 404 (unknown item) from 200-empty (no prices yet)
        from domain.connection import SessionLocal
        from domain.models import DimContainer
        with SessionLocal() as check_db:
            exists = check_db.get(DimContainer, item_id) is not None
        if not exists:
            raise HTTPException(status_code=404, detail=f"Item '{item_id}' not found")
    return history
