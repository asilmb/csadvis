"""
Portfolio stats endpoint (PV-08).

GET /stats — aggregate portfolio summary: ROI, net value, position counts.
"""

from __future__ import annotations

import structlog
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session

from database.connection import get_db_dep

logger = structlog.get_logger()
router = APIRouter(prefix="/stats", tags=["stats"])


class PortfolioStats(BaseModel):
    """Aggregate portfolio summary returned by GET /stats."""

    items_tracked: int
    """Total non-blacklisted containers being monitored."""

    open_positions: int
    """Number of open inventory positions."""

    total_cost: float
    """Sum of (buy_price × quantity) across all open positions."""

    portfolio_value: float
    """Sum of (current_price × quantity) across all open positions."""

    net_proceeds: float
    """Portfolio value after Steam Market 15 % fee + fixed fee."""

    unrealized_roi_pct: float
    """(net_proceeds − total_cost) / total_cost × 100. 0.0 when no positions."""

    items_with_price: int
    """Containers that have at least one price observation."""


@router.get(
    "",
    response_model=PortfolioStats,
    summary="Portfolio statistics",
    description=(
        "Returns an aggregate summary of the portfolio: total tracked items, "
        "open position count, current market value, net proceeds after fees, "
        "and unrealized ROI across all open positions."
    ),
)
def get_stats(db: Session = Depends(get_db_dep)) -> PortfolioStats:
    from sqlalchemy import func, select

    from config import settings
    from database.models import DimContainer, FactContainerPrice, Position, PositionStatus

    # ── Items tracked (non-blacklisted) ──────────────────────────────────────
    items_tracked: int = db.execute(
        select(func.count()).select_from(DimContainer).where(
            DimContainer.is_blacklisted == 0
        )
    ).scalar_one()

    # ── Items with at least one price observation ─────────────────────────────
    items_with_price: int = db.execute(
        select(func.count(func.distinct(FactContainerPrice.container_id)))
    ).scalar_one()

    # ── Open positions ────────────────────────────────────────────────────────
    pos_rows = db.execute(
        select(Position.market_hash_name, Position.buy_price, Position.quantity)
        .where(Position.status == PositionStatus.OPEN)
    ).all()

    open_positions = len(pos_rows)
    total_cost = sum(float(r.buy_price) * int(r.quantity) for r in pos_rows)

    # Current prices keyed by market_hash_name
    names = list({r.market_hash_name for r in pos_rows})
    current_prices: dict[str, float] = {}
    if names:
        # Sub-query: latest price per container name via DimContainer join
        latest_subq = (
            select(
                DimContainer.container_name,
                func.max(FactContainerPrice.timestamp).label("max_ts"),
            )
            .join(
                FactContainerPrice,
                DimContainer.container_id == FactContainerPrice.container_id,
            )
            .where(
                DimContainer.container_name.in_(names),
                FactContainerPrice.price.isnot(None),
            )
            .group_by(DimContainer.container_name)
            .subquery()
        )
        price_rows = db.execute(
            select(DimContainer.container_name, FactContainerPrice.price)
            .join(
                FactContainerPrice,
                DimContainer.container_id == FactContainerPrice.container_id,
            )
            .join(
                latest_subq,
                (DimContainer.container_name == latest_subq.c.container_name)
                & (FactContainerPrice.timestamp == latest_subq.c.max_ts),
            )
        ).all()
        current_prices = {str(r.container_name): float(r.price) for r in price_rows}

    portfolio_value = sum(
        current_prices.get(r.market_hash_name, float(r.buy_price)) * int(r.quantity)
        for r in pos_rows
    )

    # ── Net proceeds after Steam fee ──────────────────────────────────────────
    net_unit_multiplier = 1.0 / settings.steam_fee_divisor
    net_fee_fixed_total = settings.steam_fee_fixed * open_positions
    net_proceeds = round(
        portfolio_value * net_unit_multiplier - net_fee_fixed_total, 2
    )

    # ── ROI ───────────────────────────────────────────────────────────────────
    if total_cost > 0:
        unrealized_roi_pct = round((net_proceeds / total_cost - 1) * 100, 2)
    else:
        unrealized_roi_pct = 0.0

    return PortfolioStats(
        items_tracked=items_tracked,
        open_positions=open_positions,
        total_cost=round(total_cost, 2),
        portfolio_value=round(portfolio_value, 2),
        net_proceeds=net_proceeds,
        unrealized_roi_pct=unrealized_roi_pct,
        items_with_price=items_with_price,
    )
