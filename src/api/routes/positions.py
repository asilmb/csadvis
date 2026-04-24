"""
Positions & Transaction Groups API — /api/v1/positions

Groups
------
GET    /positions/groups                  — all groups with link_status
GET    /positions/groups/suggestions      — auto-detected grouping candidates
POST   /positions/groups                  — create group from tx_ids
PATCH  /positions/groups/{id}/skip        — mark group as skipped

Positions
---------
GET    /positions                         — list positions (?type=flip|investment&status=hold)
POST   /positions                         — create position
POST   /positions/{id}/link/{group_id}    — link a group to a position
DELETE /positions/{id}/link/{group_id}    — unlink a group from a position
DELETE /positions/{id}                    — liquidate position
PATCH  /positions/{id}/close              — close position with balance_influence
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from src.domain.connection import get_db_dep
from src.domain.group_service import (
    GroupSuggestion,
    create_group,
    skip_transactions,
    suggest_groups,
)
from src.domain.models import (
    DimContainer,
    FactContainerPrice,
    InvestmentPosition,
    InvestmentPositionType,
    LinkStatus,
    PositionTransactionGroup,
    TransactionDirection,
    TransactionGroup,
)
from src.domain.position_service import (
    GroupNotFoundError,
    PositionAlreadyClosedError,
    PositionNotFoundError,
    close_position,
    create_position,
    link_group,
    liquidate_position,
    unlink_group,
)

logger = structlog.get_logger()

router = APIRouter(prefix="/positions", tags=["positions"])
get_db = get_db_dep


# ─── Request / Response schemas ───────────────────────────────────────────────


class CreateGroupRequest(BaseModel):
    tx_ids:       list[str]
    direction:    Literal["BUY", "SELL"]
    item_name:    str
    container_id: str | None = None


class CreatePositionRequest(BaseModel):
    container_id:      str
    position_type:     Literal["flip", "investment"]
    buy_price:         float = Field(gt=0)
    fixation_count:    int   = Field(ge=1)
    sale_target_price: float = Field(gt=0)
    name:              str | None = None
    group_id:          str | None = None


class ClosePositionRequest(BaseModel):
    balance_influence: float


class CreateArmoryPassPositionRequest(BaseModel):
    container_id:   str
    pass_cost:      float = Field(gt=0)
    stars_in_pass:  int   = Field(ge=1)
    stars_per_case: int   = Field(ge=1)
    name:           str | None = None


class UpdateProgressRequest(BaseModel):
    current_count: int = Field(ge=0)


def _group_to_dict(
    group: TransactionGroup,
    ptg:   PositionTransactionGroup | None,
) -> dict:
    return {
        "id":                   group.id,
        "name":                 group.name,
        "direction":            group.direction,
        "item_name":            group.item_name,
        "container_id":         group.container_id,
        "count":                group.count,
        "price":                group.price,
        "date_from":            group.date_from.isoformat(),
        "date_to":              group.date_to.isoformat(),
        "trade_ban_expires_at": group.trade_ban_expires_at.isoformat()
                                if group.trade_ban_expires_at else None,
        "created_at":           group.created_at.isoformat(),
        "link_status":          ptg.link_status if ptg else LinkStatus.undefined,
        "position_id":          ptg.position_id if ptg else None,
    }


def _position_to_dict(
    position:      InvestmentPosition,
    current_price: float | None,
) -> dict:
    return {
        "id":                position.id,
        "name":              position.name,
        "container_id":      position.container_id,
        "position_type":     position.position_type,
        "fixation_count":    position.fixation_count,
        "current_count":     position.current_count,
        "buy_price":         position.buy_price,
        "sale_target_price": position.sale_target_price,
        "status":            position.status,
        "opened_at":         position.opened_at.isoformat(),
        "closed_at":         position.closed_at.isoformat() if position.closed_at else None,
        "balance_influence": position.balance_influence,
        "current_price":     current_price,
    }


def _latest_price(db: Session, container_id: str) -> float | None:
    """Return the most recent price for a container via JOIN — not stored on position."""
    row = (
        db.query(FactContainerPrice)
        .filter(FactContainerPrice.container_id == container_id)
        .order_by(FactContainerPrice.timestamp.desc())
        .first()
    )
    return row.price if row else None


# ─── Groups ───────────────────────────────────────────────────────────────────


@router.get("/groups", response_model=list[dict])
def list_groups(
    link_status: str | None = Query(None, description="Filter by link_status: undefined|defined|skipped"),
    db: Session = Depends(get_db),
) -> list[dict]:
    """List all transaction groups with their current link_status."""
    q = db.query(TransactionGroup, PositionTransactionGroup).outerjoin(
        PositionTransactionGroup,
        PositionTransactionGroup.transaction_group_id == TransactionGroup.id,
    )
    if link_status:
        q = q.filter(PositionTransactionGroup.link_status == link_status)

    rows = q.order_by(TransactionGroup.date_from.desc()).all()
    return [_group_to_dict(g, ptg) for g, ptg in rows]


@router.get("/groups/suggestions", response_model=list[dict])
def list_suggestions(db: Session = Depends(get_db)) -> list[dict]:
    """Auto-detect candidate transaction groups from ungrouped fact_transactions."""
    suggestions: list[GroupSuggestion] = suggest_groups(db)
    return [
        {
            "tx_ids":       s.tx_ids,
            "item_name":    s.item_name,
            "direction":    s.direction,
            "count":        s.count,
            "avg_price":    round(s.avg_price, 2),
            "date_from":    s.date_from.isoformat(),
            "date_to":      s.date_to.isoformat(),
            "confidence":   round(s.confidence, 3),
            "price_bucket": s.price_bucket,
            "time_bucket":  s.time_bucket,
        }
        for s in suggestions
    ]


@router.post("/groups", response_model=dict, status_code=201)
def create_group_endpoint(
    body: CreateGroupRequest,
    db:   Session = Depends(get_db),
) -> dict:
    """Create a TransactionGroup from a list of transaction IDs."""
    try:
        direction = TransactionDirection(body.direction)
        group = create_group(
            db,
            tx_ids       = body.tx_ids,
            direction    = direction,
            item_name    = body.item_name,
            container_id = body.container_id,
        )
        db.commit()
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    ptg = (
        db.query(PositionTransactionGroup)
        .filter(PositionTransactionGroup.transaction_group_id == group.id)
        .first()
    )
    return _group_to_dict(group, ptg)


@router.patch("/groups/{group_id}/skip", response_model=dict)
def skip_group(group_id: str, db: Session = Depends(get_db)) -> dict:
    """Mark a TransactionGroup as skipped (removes it from the review queue)."""
    ptg = (
        db.query(PositionTransactionGroup)
        .filter(PositionTransactionGroup.transaction_group_id == group_id)
        .first()
    )
    if ptg is None:
        raise HTTPException(status_code=404, detail="Group not found or not yet queued")

    ptg.link_status = LinkStatus.skipped
    db.commit()

    group = db.get(TransactionGroup, group_id)
    return _group_to_dict(group, ptg)


# ─── Positions ────────────────────────────────────────────────────────────────


@router.get("", response_model=list[dict])
def list_positions(
    type:   str | None = Query(None, description="flip | investment"),
    status: str | None = Query(None, description="hold | on_sale | sold"),
    db:     Session    = Depends(get_db),
) -> list[dict]:
    """List investment positions with optional type/status filters."""
    q = db.query(InvestmentPosition)
    if type:
        q = q.filter(InvestmentPosition.position_type == type)
    if status:
        q = q.filter(InvestmentPosition.status == status)

    positions = q.order_by(InvestmentPosition.opened_at.desc()).all()
    return [
        _position_to_dict(p, _latest_price(db, p.container_id))
        for p in positions
    ]


@router.post("", response_model=dict, status_code=201)
def create_position_endpoint(
    body: CreatePositionRequest,
    db:   Session = Depends(get_db),
) -> dict:
    """Create a new flip or investment position."""
    container = db.get(DimContainer, body.container_id)
    if container is None:
        raise HTTPException(status_code=404, detail="Container not found")

    try:
        position = create_position(
            db,
            container_id      = body.container_id,
            position_type     = InvestmentPositionType(body.position_type),
            buy_price         = body.buy_price,
            fixation_count    = body.fixation_count,
            sale_target_price = body.sale_target_price,
            name              = body.name,
            group_id          = body.group_id,
        )
        db.commit()
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    return _position_to_dict(position, _latest_price(db, position.container_id))


@router.post("/{position_id}/link/{group_id}", response_model=dict)
def link_group_endpoint(
    position_id: str,
    group_id:    str,
    db:          Session = Depends(get_db),
) -> dict:
    """Link a TransactionGroup to an InvestmentPosition."""
    try:
        ptg = link_group(db, position_id, group_id)
        db.commit()
    except PositionNotFoundError:
        raise HTTPException(status_code=404, detail="Position not found")
    except GroupNotFoundError:
        raise HTTPException(status_code=404, detail="Group not found")
    except PositionAlreadyClosedError as exc:
        raise HTTPException(status_code=409, detail=str(exc))

    position = db.get(InvestmentPosition, position_id)
    return _position_to_dict(position, _latest_price(db, position.container_id))


@router.delete("/{position_id}/link/{group_id}", response_model=dict)
def unlink_group_endpoint(
    position_id: str,
    group_id:    str,
    db:          Session = Depends(get_db),
) -> dict:
    """Unlink a TransactionGroup from an InvestmentPosition."""
    try:
        unlink_group(db, position_id, group_id)
        db.commit()
    except PositionNotFoundError:
        raise HTTPException(status_code=404, detail="Position not found")
    except GroupNotFoundError:
        raise HTTPException(status_code=404, detail="Group not found")

    position = db.get(InvestmentPosition, position_id)
    return _position_to_dict(position, _latest_price(db, position.container_id))


@router.delete("/{position_id}", response_model=dict)
def liquidate_position_endpoint(
    position_id: str,
    db:          Session = Depends(get_db),
) -> dict:
    """Liquidate (delete) a position and return all linked groups to the queue."""
    try:
        liquidate_position(db, position_id)
        db.commit()
    except PositionNotFoundError:
        raise HTTPException(status_code=404, detail="Position not found")

    return {"ok": True, "position_id": position_id}


@router.patch("/{position_id}/close", response_model=dict)
def close_position_endpoint(
    position_id: str,
    body:        ClosePositionRequest,
    db:          Session = Depends(get_db),
) -> dict:
    """Manually close a position and record its actual financial result."""
    try:
        position = close_position(db, position_id, body.balance_influence)
        db.commit()
    except PositionNotFoundError:
        raise HTTPException(status_code=404, detail="Position not found")
    except PositionAlreadyClosedError as exc:
        raise HTTPException(status_code=409, detail=str(exc))

    return _position_to_dict(position, _latest_price(db, position.container_id))


# ─── Armory Pass positions ────────────────────────────────────────────────────


@router.post("/armorypass", response_model=dict)
def create_armorypass_position_endpoint(
    body: CreateArmoryPassPositionRequest,
    db:   Session = Depends(get_db),
) -> dict:
    """
    Create an Armory Pass position.

    Calculates:
      effective_cost  = pass_cost / stars_in_pass * stars_per_case
      fixation_count  = stars_in_pass // stars_per_case
      sale_target     = (effective_cost + 5) * 1.15  (breakeven listing price)
    """
    if body.stars_per_case > body.stars_in_pass:
        raise HTTPException(status_code=422, detail="stars_per_case cannot exceed stars_in_pass")

    fixation_count  = body.stars_in_pass // body.stars_per_case
    buy_price       = body.pass_cost / body.stars_in_pass * body.stars_per_case
    sale_target     = (buy_price + 5) * 1.15
    position_name   = body.name or f"Armory Pass · {fixation_count} кейс"

    position = create_position(
        db,
        container_id      = body.container_id,
        position_type     = InvestmentPositionType.armorypass,
        buy_price         = round(buy_price, 2),
        fixation_count    = fixation_count,
        sale_target_price = round(sale_target, 2),
        name              = position_name,
    )
    db.commit()
    return _position_to_dict(position, _latest_price(db, position.container_id))


@router.patch("/{position_id}/progress", response_model=dict)
def update_progress_endpoint(
    position_id: str,
    body:        UpdateProgressRequest,
    db:          Session = Depends(get_db),
) -> dict:
    """Update the current_count (progress) of a position manually."""
    position = db.get(InvestmentPosition, position_id)
    if not position:
        raise HTTPException(status_code=404, detail="Position not found")
    position.current_count = min(body.current_count, position.fixation_count)
    db.commit()
    return _position_to_dict(position, _latest_price(db, position.container_id))


@router.patch("/{position_id}/reset", response_model=dict)
def reset_position_endpoint(
    position_id: str,
    db:          Session = Depends(get_db),
) -> dict:
    """Reset an Armory Pass position progress back to 0 (new cycle)."""
    position = db.get(InvestmentPosition, position_id)
    if not position:
        raise HTTPException(status_code=404, detail="Position not found")
    if position.position_type != InvestmentPositionType.armorypass:
        raise HTTPException(status_code=422, detail="Only armorypass positions can be reset")
    position.current_count = 0
    db.commit()
    return _position_to_dict(position, _latest_price(db, position.container_id))
