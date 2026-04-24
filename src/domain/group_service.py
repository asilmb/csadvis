"""
Group Service — transaction grouping for the Positions module.

Steam Market transactions arrive as individual rows in fact_transactions.
This service clusters them into TransactionGroups by matching:
  - same item_name + action (BUY/SELL)
  - price within ±5 ₸ bucket  (price_bucket = round(price / 5) * 5)
  - trade_date within a 2-hour window (time_bucket = floor(epoch / 7200))

Public API
----------
suggest_groups(db)                          → list[GroupSuggestion]
create_group(db, tx_ids, direction, item)   → TransactionGroup
skip_transactions(db, tx_ids, item, direction) → TransactionGroup
"""

from __future__ import annotations

import math
import statistics
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import structlog
from sqlalchemy.orm import Session

from src.domain.models import (
    FactTransaction,
    LinkStatus,
    PositionTransactionGroup,
    TransactionDirection,
    TransactionGroup,
)

logger = structlog.get_logger()

_PRICE_BUCKET_SIZE = 5       # ₸  — quantisation window
_TIME_BUCKET_SECS  = 7_200  # 2 hours
_TRADE_BAN_DAYS    = 7


# ─── DTO ──────────────────────────────────────────────────────────────────────


@dataclass
class GroupSuggestion:
    """Candidate cluster returned by suggest_groups()."""

    tx_ids:       list[str]
    item_name:    str
    direction:    TransactionDirection
    count:        int
    avg_price:    float
    date_from:    datetime
    date_to:      datetime
    confidence:   float          # 0.0–1.0 — how tightly prices/times cluster
    price_bucket: float
    time_bucket:  int            # unix epoch // 7200


# ─── Helpers ──────────────────────────────────────────────────────────────────


def _price_bucket(price: float) -> float:
    return round(price / _PRICE_BUCKET_SIZE) * _PRICE_BUCKET_SIZE


def _time_bucket(dt: datetime) -> int:
    epoch = int(dt.replace(tzinfo=UTC).timestamp()) if dt.tzinfo else int(dt.timestamp())
    return epoch // _TIME_BUCKET_SECS


def _confidence(prices: list[float], dates: list[datetime]) -> float:
    """
    Measure how tightly the transactions cluster in price and time.

    confidence = 1 − (price_spread_score + time_spread_score) / 2

    price_spread_score = std(prices) / _PRICE_BUCKET_SIZE   (0 when all prices equal)
    time_spread_score  = time_range_hours / 2                (0 when all in same moment)

    Result is clamped to [0.0, 1.0].
    """
    if len(prices) < 2:
        return 1.0

    price_std   = statistics.stdev(prices)
    price_score = price_std / _PRICE_BUCKET_SIZE

    timestamps      = [d.timestamp() for d in dates]
    time_range_h    = (max(timestamps) - min(timestamps)) / 3600
    time_score      = time_range_h / 2

    raw = 1.0 - (price_score + time_score) / 2
    return max(0.0, min(1.0, raw))


def _action_to_direction(action: str) -> TransactionDirection:
    """Map fact_transactions.action ('BUY'/'SELL'/'FLIP') → TransactionDirection."""
    return TransactionDirection.SELL if action.upper() == "SELL" else TransactionDirection.BUY


# ─── Service functions ────────────────────────────────────────────────────────


def suggest_groups(db: Session) -> list[GroupSuggestion]:
    """
    Return candidate transaction groups from ungrouped fact_transactions.

    Only returns clusters with ≥ 2 transactions.
    """
    rows: list[FactTransaction] = (
        db.query(FactTransaction)
        .filter(FactTransaction.transaction_group_id.is_(None))
        .order_by(FactTransaction.trade_date)
        .all()
    )

    if not rows:
        return []

    # Bucket → list[FactTransaction]
    buckets: dict[tuple, list[FactTransaction]] = {}
    for tx in rows:
        key = (
            tx.item_name,
            _action_to_direction(tx.action),
            _price_bucket(tx.price),
            _time_bucket(tx.trade_date),
        )
        buckets.setdefault(key, []).append(tx)

    suggestions: list[GroupSuggestion] = []
    for (item_name, direction, pb, tb), txs in buckets.items():
        if len(txs) < 2:
            continue

        prices = [t.price for t in txs]
        dates  = [t.trade_date for t in txs]

        suggestions.append(GroupSuggestion(
            tx_ids       = [t.id for t in txs],
            item_name    = item_name,
            direction    = direction,
            count        = len(txs),
            avg_price    = sum(prices) / len(prices),
            date_from    = min(dates),
            date_to      = max(dates),
            confidence   = _confidence(prices, dates),
            price_bucket = pb,
            time_bucket  = tb,
        ))

    # Sort by confidence desc so the most obvious groups come first
    suggestions.sort(key=lambda s: s.confidence, reverse=True)
    logger.info("suggest_groups", count=len(suggestions))
    return suggestions


def create_group(
    db:        Session,
    tx_ids:    list[str],
    direction: TransactionDirection,
    item_name: str,
    container_id: str | None = None,
) -> TransactionGroup:
    """
    Persist a TransactionGroup and stamp transaction_group_id on each transaction.

    For BUY groups: trade_ban_expires_at = max(trade_date) + 7 days.
    Also creates a PositionTransactionGroup with link_status=undefined so the group
    immediately appears in the Balance tab queue.
    """
    txs: list[FactTransaction] = (
        db.query(FactTransaction)
        .filter(FactTransaction.id.in_(tx_ids))
        .all()
    )
    if not txs:
        raise ValueError(f"No transactions found for ids: {tx_ids}")

    prices = [t.price for t in txs]
    dates  = [t.trade_date for t in txs]
    avg    = sum(prices) / len(prices)
    now    = datetime.now(UTC).replace(tzinfo=None)

    trade_ban_expires_at = None
    if direction == TransactionDirection.BUY:
        trade_ban_expires_at = max(dates) + timedelta(days=_TRADE_BAN_DAYS)

    group = TransactionGroup(
        id                   = str(uuid.uuid4()),
        name                 = f"{direction}: {item_name} ×{len(txs)} — {min(dates).strftime('%Y-%m-%d %H:%M')}",
        direction            = direction,
        item_name            = item_name,
        container_id         = container_id,
        count                = len(txs),
        price                = avg,
        date_from            = min(dates),
        date_to              = max(dates),
        trade_ban_expires_at = trade_ban_expires_at,
        created_at           = now,
    )
    db.add(group)
    db.flush()  # get group.id before updating FK

    for tx in txs:
        tx.transaction_group_id = group.id

    ptg = PositionTransactionGroup(
        id                   = str(uuid.uuid4()),
        position_id          = None,
        transaction_group_id = group.id,
        link_status          = LinkStatus.undefined,
        linked_at            = None,
    )
    db.add(ptg)

    logger.info("create_group", group_id=group.id, item=item_name, count=len(txs))
    return group


def skip_transactions(
    db:        Session,
    tx_ids:    list[str],
    item_name: str,
    direction: TransactionDirection,
    container_id: str | None = None,
) -> TransactionGroup:
    """
    Group the given transactions and mark the cluster as skipped.

    Used when the user explicitly dismisses a suggestion without creating a position.
    """
    group = create_group(db, tx_ids, direction, item_name, container_id)

    ptg: PositionTransactionGroup | None = (
        db.query(PositionTransactionGroup)
        .filter(PositionTransactionGroup.transaction_group_id == group.id)
        .first()
    )
    if ptg:
        ptg.link_status = LinkStatus.skipped

    logger.info("skip_transactions", group_id=group.id, item=item_name)
    return group
