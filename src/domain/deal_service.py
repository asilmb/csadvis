"""
Deal service — creates and manages flip trade deals.

A Deal is created when a user buys a container for flipping.
Targets are fixed at creation time relative to entry_price:
  sell_target = entry_price × 1.21  (~4-5% net after Steam 15% fee)
  stop_loss   = entry_price × 0.92  (accept -8% max loss)
  unlock_date = entry_date + 7 days  (Steam trade ban)
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import structlog

from src.domain.models import DealStatus, DimDeal

logger = structlog.get_logger()

# ── Deal target constants ─────────────────────────────────────────────────────

_SELL_TARGET_MULTIPLIER = 1.21   # entry × 1.21 → ~4-5% net after Steam 15% fee
_STOP_LOSS_MULTIPLIER = 0.92     # entry × 0.92 → accept -8% max loss
_TRADE_BAN_DAYS = 7              # Steam trade ban duration in days


# ─── Public API ───────────────────────────────────────────────────────────────


def create_deal(
    container_id: str,
    entry_price: float,
    qty: int,
    entry_date: datetime | None = None,  # defaults to utcnow
) -> DimDeal:
    """
    Create a new Deal object (not persisted). Caller is responsible for
    session.add() + commit().

    Parameters
    ----------
    container_id:
        UUID of the container being purchased (FK → dim_containers).
    entry_price:
        Price paid per unit in KZT.
    qty:
        Number of units purchased.
    entry_date:
        Purchase timestamp in UTC (naive). Defaults to current UTC time.

    Returns
    -------
    DimDeal with status=LOCKED, targets computed from entry_price, and
    unlock_date set to entry_date + 7 days.
    """
    if entry_date is None:
        entry_date = datetime.now(UTC).replace(tzinfo=None)

    unlock_date = entry_date + timedelta(days=_TRADE_BAN_DAYS)
    sell_target = round(entry_price * _SELL_TARGET_MULTIPLIER, 2)
    stop_loss = round(entry_price * _STOP_LOSS_MULTIPLIER, 2)

    deal = DimDeal(
        container_id=container_id,
        entry_price=entry_price,
        entry_date=entry_date,
        unlock_date=unlock_date,
        qty=qty,
        sell_target=sell_target,
        stop_loss=stop_loss,
        status=DealStatus.LOCKED,
    )

    logger.debug(
        "deal_created",
        service="deal_service",
        container_id=container_id,
        entry_price=entry_price,
        qty=qty,
        sell_target=sell_target,
        stop_loss=stop_loss,
        unlock_date=unlock_date.isoformat(),
    )

    return deal


def activate_unlocked_deals(db) -> int:
    """
    Set status=ACTIVE for all LOCKED deals whose unlock_date has passed.

    Should be called periodically (e.g. by a scheduler) to transition deals
    out of the Steam trade ban window so they become eligible for selling.

    Parameters
    ----------
    db:
        SQLAlchemy Session. Caller is responsible for commit().

    Returns
    -------
    Count of deals activated.
    """
    now = datetime.now(UTC).replace(tzinfo=None)

    deals: list[DimDeal] = (
        db.query(DimDeal)
        .filter(
            DimDeal.status == DealStatus.LOCKED,
            DimDeal.unlock_date <= now,
        )
        .all()
    )

    for deal in deals:
        deal.status = DealStatus.ACTIVE

    count = len(deals)
    if count:
        logger.info(
            "deals_activated",
            service="deal_service",
            count=count,
        )

    return count


def get_stop_loss_alerts(db, current_prices: dict[str, float]) -> list[DimDeal]:
    """
    Return all ACTIVE deals where current_price <= stop_loss.

    Only deals whose unlock_date has already passed are considered — a locked
    deal cannot be sold, so alerting on it would be noise.

    Parameters
    ----------
    db:
        SQLAlchemy Session.
    current_prices:
        Mapping of {container_id: current_market_price}.

    Returns
    -------
    List of DimDeal objects that have breached their stop-loss threshold.
    """
    if not current_prices:
        return []

    now = datetime.now(UTC).replace(tzinfo=None)

    active_deals: list[DimDeal] = (
        db.query(DimDeal)
        .filter(
            DimDeal.status == DealStatus.ACTIVE,
            DimDeal.unlock_date <= now,
            DimDeal.container_id.in_(list(current_prices.keys())),
        )
        .all()
    )

    alerts = [
        deal
        for deal in active_deals
        if current_prices.get(deal.container_id, float("inf")) <= deal.stop_loss
    ]

    if alerts:
        logger.warning(
            "stop_loss_alerts",
            service="deal_service",
            count=len(alerts),
            deal_ids=[d.deal_id for d in alerts],
        )

    return alerts


def close_deal(deal: DimDeal, closed_price: float, status: DealStatus) -> None:
    """
    Mark a deal as SOLD or STOPPED. Sets closed_at and closed_price.

    Does NOT flush or commit — the caller owns the transaction.

    Parameters
    ----------
    deal:
        The DimDeal instance to close.
    closed_price:
        Actual price at which the container was sold or stopped out.
    status:
        Must be DealStatus.SOLD or DealStatus.STOPPED.

    Raises
    ------
    ValueError:
        When status is not SOLD or STOPPED, or the deal is already closed.
    """
    if status not in (DealStatus.SOLD, DealStatus.STOPPED):
        raise ValueError(
            f"close_deal requires status SOLD or STOPPED, got {status!r}"
        )

    if deal.status in (DealStatus.SOLD, DealStatus.STOPPED):
        raise ValueError(
            f"Deal {deal.deal_id!r} is already closed with status {deal.status!r}"
        )

    deal.status = status
    deal.closed_price = closed_price
    deal.closed_at = datetime.now(UTC).replace(tzinfo=None)

    logger.info(
        "deal_closed",
        service="deal_service",
        deal_id=deal.deal_id,
        container_id=deal.container_id,
        status=status,
        entry_price=deal.entry_price,
        closed_price=closed_price,
    )
