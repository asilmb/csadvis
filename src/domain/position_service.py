"""
Position Service — lifecycle management for InvestmentPosition.

An InvestmentPosition tracks a flip or investment trade from opening through
optional partial SELL-group linking to final closure.

current_count lifecycle:
  - Set to fixation_count at creation.
  - Decremented by group.count when a SELL TransactionGroup is linked.
  - When current_count reaches 0 → status auto-transitions to 'sold'.

current_price is NOT stored — callers should JOIN with dim_containers via
container_id → DimContainer.price_history (latest FactContainerPrice) when
they need the live market price.

Public API
----------
create_position(db, ...)           → InvestmentPosition
link_group(db, pos_id, group_id)   → PositionTransactionGroup
unlink_group(db, pos_id, group_id) → None
liquidate_position(db, pos_id)     → None   (delete + unlink all groups)
close_position(db, pos_id, balance_influence) → InvestmentPosition
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import structlog
from sqlalchemy.orm import Session

from src.domain.models import (
    InvestmentPosition,
    InvestmentPositionStatus,
    InvestmentPositionType,
    LinkStatus,
    PositionTransactionGroup,
    TransactionDirection,
    TransactionGroup,
)

logger = structlog.get_logger()


# ─── Exceptions ───────────────────────────────────────────────────────────────


class PositionNotFoundError(Exception):
    """Raised when a position_id resolves to nothing in the DB."""


class GroupNotFoundError(Exception):
    """Raised when a transaction_group_id resolves to nothing in the DB."""


class PositionAlreadyClosedError(Exception):
    """Raised when a state-change is attempted on a sold/closed position."""


# ─── Service functions ────────────────────────────────────────────────────────


def create_position(
    db:               Session,
    container_id:     str,
    position_type:    InvestmentPositionType,
    buy_price:        float,
    fixation_count:   int,
    sale_target_price: float,
    name:             str | None = None,
    group_id:         str | None = None,
) -> InvestmentPosition:
    """
    Create a new InvestmentPosition.

    Parameters
    ----------
    container_id:      FK to dim_containers — drives the current_price JOIN.
    position_type:     'flip' or 'investment'.
    buy_price:         Immutable entry price per unit.
    fixation_count:    Immutable total units — current_count starts here.
    sale_target_price: Target exit price per unit.
    name:              Human label; auto-generated from container + date if omitted.
    group_id:          Optionally link a BUY TransactionGroup at creation time.
    """
    if buy_price <= 0:
        raise ValueError(f"buy_price must be positive, got {buy_price!r}")
    if fixation_count < 1:
        raise ValueError(f"fixation_count must be ≥ 1, got {fixation_count!r}")

    now = datetime.now(UTC).replace(tzinfo=None)

    auto_name = name or (
        f"{position_type.value.capitalize()}: container={container_id}"
        f" ×{fixation_count} — {now.strftime('%Y-%m-%d')}"
    )

    position = InvestmentPosition(
        id                = str(uuid.uuid4()),
        name              = auto_name,
        container_id      = container_id,
        position_type     = position_type,
        fixation_count    = fixation_count,
        current_count     = fixation_count,
        buy_price         = buy_price,
        sale_target_price = sale_target_price,
        status            = InvestmentPositionStatus.hold,
        opened_at         = now,
    )
    db.add(position)
    db.flush()

    if group_id:
        link_group(db, position.id, group_id)

    logger.info(
        "create_position",
        position_id=position.id,
        type=position_type,
        container_id=container_id,
        fixation_count=fixation_count,
    )
    return position


def link_group(
    db:          Session,
    position_id: str,
    group_id:    str,
) -> PositionTransactionGroup:
    """
    Link a TransactionGroup to an InvestmentPosition.

    - Sets PositionTransactionGroup.link_status → 'defined'.
    - If the group direction is SELL: decrements position.current_count by group.count.
    - If current_count reaches 0: transitions position.status → 'sold' and sets closed_at.
    """
    position = db.get(InvestmentPosition, position_id)
    if position is None:
        raise PositionNotFoundError(position_id)
    if position.status == InvestmentPositionStatus.sold:
        raise PositionAlreadyClosedError(f"Position {position_id!r} is already sold.")

    group = db.get(TransactionGroup, group_id)
    if group is None:
        raise GroupNotFoundError(group_id)

    ptg: PositionTransactionGroup | None = (
        db.query(PositionTransactionGroup)
        .filter(PositionTransactionGroup.transaction_group_id == group_id)
        .first()
    )

    now = datetime.now(UTC).replace(tzinfo=None)

    if ptg is None:
        ptg = PositionTransactionGroup(
            id                   = str(uuid.uuid4()),
            position_id          = position_id,
            transaction_group_id = group_id,
            link_status          = LinkStatus.defined,
            linked_at            = now,
        )
        db.add(ptg)
    else:
        ptg.position_id  = position_id
        ptg.link_status  = LinkStatus.defined
        ptg.linked_at    = now

    # Decrement current_count for SELL groups
    if group.direction == TransactionDirection.SELL:
        position.current_count = max(0, position.current_count - group.count)
        if position.current_count == 0:
            position.status    = InvestmentPositionStatus.sold
            position.closed_at = now
            logger.info("position_auto_sold", position_id=position_id)

    logger.info(
        "link_group",
        position_id=position_id,
        group_id=group_id,
        direction=group.direction,
        current_count=position.current_count,
    )
    return ptg


def unlink_group(
    db:          Session,
    position_id: str,
    group_id:    str,
) -> None:
    """
    Remove the link between a TransactionGroup and an InvestmentPosition.

    Reverts PositionTransactionGroup to link_status='undefined' and
    restores current_count if the group was a SELL direction.
    """
    position = db.get(InvestmentPosition, position_id)
    if position is None:
        raise PositionNotFoundError(position_id)

    group = db.get(TransactionGroup, group_id)
    if group is None:
        raise GroupNotFoundError(group_id)

    ptg: PositionTransactionGroup | None = (
        db.query(PositionTransactionGroup)
        .filter(
            PositionTransactionGroup.position_id          == position_id,
            PositionTransactionGroup.transaction_group_id == group_id,
        )
        .first()
    )
    if ptg is None:
        return  # already unlinked — idempotent

    ptg.position_id = None
    ptg.link_status = LinkStatus.undefined
    ptg.linked_at   = None

    # Restore current_count if we're undoing a SELL link
    if group.direction == TransactionDirection.SELL:
        position.current_count = min(
            position.fixation_count,
            position.current_count + group.count,
        )
        if position.status == InvestmentPositionStatus.sold:
            position.status    = InvestmentPositionStatus.on_sale
            position.closed_at = None

    logger.info("unlink_group", position_id=position_id, group_id=group_id)


def liquidate_position(db: Session, position_id: str) -> None:
    """
    Delete a position and unlink all associated TransactionGroups.

    All linked PositionTransactionGroup rows are reset to link_status='undefined'
    before the position row is deleted so groups re-enter the review queue.
    """
    position = db.get(InvestmentPosition, position_id)
    if position is None:
        raise PositionNotFoundError(position_id)

    ptgs: list[PositionTransactionGroup] = (
        db.query(PositionTransactionGroup)
        .filter(PositionTransactionGroup.position_id == position_id)
        .all()
    )
    for ptg in ptgs:
        ptg.position_id = None
        ptg.link_status = LinkStatus.undefined
        ptg.linked_at   = None

    db.delete(position)
    logger.info("liquidate_position", position_id=position_id, unlinked=len(ptgs))


def close_position(
    db:                Session,
    position_id:       str,
    balance_influence: float,
) -> InvestmentPosition:
    """
    Manually close a position and record its actual financial result.

    Sets status → 'sold', closed_at = now, balance_influence = provided value.
    Raises PositionAlreadyClosedError if the position is already sold.
    """
    position = db.get(InvestmentPosition, position_id)
    if position is None:
        raise PositionNotFoundError(position_id)
    if position.status == InvestmentPositionStatus.sold:
        raise PositionAlreadyClosedError(f"Position {position_id!r} is already sold.")

    now = datetime.now(UTC).replace(tzinfo=None)
    position.status            = InvestmentPositionStatus.sold
    position.closed_at         = now
    position.balance_influence = balance_influence

    logger.info(
        "close_position",
        position_id=position_id,
        balance_influence=balance_influence,
    )
    return position
