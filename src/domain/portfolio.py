"""
Services layer — portfolio data access.

Thin module that extracts the two most complex data-loading functions from
frontend/app.py so they can be unit-tested and reused outside the Dash callback
context.

Functions:
  get_portfolio_data()    — returns current Steam prices + investment signals
                            for all containers in one call (replaces the inline
                            _get_current_steam_prices + compute_all_investment_signals
                            pattern in callbacks)
  get_container_detail()  — returns a single container's full detail record
                            (DimContainer + latest price + price history) without
                            requiring a live ORM session in the caller
"""

from __future__ import annotations

import logging
import uuid
from collections import defaultdict
from datetime import UTC, datetime, timedelta

from config import settings
from src.domain.connection import SessionLocal
from src.domain.models import (
    DimAnnualSummary,
    DimContainer,
    FactContainerPrice,
    FactPortfolioSnapshot,
    FactTransaction,
)
from src.domain.repositories import InventoryRepository
from src.domain.services import compute_smart_buy_price
from src.domain.value_objects import Amount

logger = logging.getLogger(__name__)


def get_portfolio_data() -> dict:
    """
    Load current Steam prices and basic stats for all tracked containers.

    Returns:
        {
            container_name: {
                "current_price": float | None,
                "mean_price":    float | None,   # 30-day mean
                "quantity":      int,             # 7-day volume
                "lowest_price":  float | None,
            },
            ...
        }

    Uses exactly 2 bulk DB queries (no per-container loops):
      1. Latest price row per container (subquery join)
      2. All rows from the past 30 days (for mean price)
    """
    from sqlalchemy import func

    cutoff_30d = datetime.now(UTC).replace(tzinfo=None) - timedelta(days=30)

    db = SessionLocal()
    try:
        containers = db.query(DimContainer).all()
        id_to_name = {str(c.container_id): str(c.container_name) for c in containers}

        # Query 1: latest price row per container
        latest_ts_subq = (
            db.query(
                FactContainerPrice.container_id,
                func.max(FactContainerPrice.timestamp).label("max_ts"),
            )
            .filter(FactContainerPrice.price.isnot(None))
            .group_by(FactContainerPrice.container_id)
            .subquery()
        )
        latest_rows = (
            db.query(FactContainerPrice)
            .join(
                latest_ts_subq,
                (FactContainerPrice.container_id == latest_ts_subq.c.container_id)
                & (FactContainerPrice.timestamp == latest_ts_subq.c.max_ts),
            )
            .all()
        )
        latest_map = {str(r.container_id): r for r in latest_rows}

        # Query 2: last 30 days for mean price
        recent_rows = (
            db.query(FactContainerPrice)
            .filter(
                FactContainerPrice.timestamp >= cutoff_30d,
                FactContainerPrice.price.isnot(None),
            )
            .all()
        )
        prices_by_cid: dict[str, list[float]] = defaultdict(list)
        for r in recent_rows:
            prices_by_cid[str(r.container_id)].append(float(r.price))

        _fee_divisor = settings.steam_fee_divisor
        _fee_fixed = Amount(settings.steam_fee_fixed)
        _min_margin = settings.flip_min_net_margin

        result: dict = {}
        for cid, name in id_to_name.items():
            latest = latest_map.get(cid)
            if not latest:
                continue
            prices_30d = prices_by_cid.get(cid, [])
            mean_30d = sum(prices_30d) / len(prices_30d) if prices_30d else None
            smart_buy: float | None = None
            if mean_30d is not None:
                smart_buy = compute_smart_buy_price(
                    Amount(mean_30d),
                    fee_divisor=_fee_divisor,
                    fee_fixed=_fee_fixed,
                    min_margin=_min_margin,
                ).amount
            result[name] = {
                "current_price": latest.price,
                "mean_price": mean_30d,
                "quantity": latest.volume_7d or 0,
                "lowest_price": latest.lowest_price,
                "smart_buy_price": smart_buy,
            }
        return result
    finally:
        db.close()


def get_container_detail(container_id: str) -> dict | None:
    """
    Return full detail for a single container by its UUID.

    Returns:
        {
            "container_id":   str,
            "container_name": str,
            "container_type": str,        # ContainerType.value
            "base_cost":  float,
            "current_price":  float | None,
            "lowest_price":   float | None,
            "mean_price_30d": float | None,
            "volume_7d":      int,
        }
        or None if container_id not found.

    Uses 2 queries: one for the container row, one for the latest price snapshot.
    The caller can load full price history separately via _get_price_history() if needed.
    """
    from sqlalchemy import func

    db = SessionLocal()
    try:
        c = db.query(DimContainer).filter(DimContainer.container_id == container_id).first()
        if not c:
            logger.warning("get_container_detail: container_id=%r not found", container_id)
            return None

        cutoff_30d = datetime.now(UTC).replace(tzinfo=None) - timedelta(days=30)

        # Latest price snapshot
        latest_ts_subq = (
            db.query(func.max(FactContainerPrice.timestamp))
            .filter(
                FactContainerPrice.container_id == container_id,
                FactContainerPrice.price.isnot(None),
            )
            .scalar_subquery()
        )
        latest = (
            db.query(FactContainerPrice)
            .filter(
                FactContainerPrice.container_id == container_id,
                FactContainerPrice.timestamp == latest_ts_subq,
            )
            .first()
        )

        # 30-day prices for mean
        recent_prices = (
            db.query(FactContainerPrice.price)
            .filter(
                FactContainerPrice.container_id == container_id,
                FactContainerPrice.timestamp >= cutoff_30d,
                FactContainerPrice.price.isnot(None),
            )
            .all()
        )
        prices_30d = [float(r.price) for r in recent_prices]
        mean_30d = sum(prices_30d) / len(prices_30d) if prices_30d else None

        return {
            "container_id": str(c.container_id),
            "container_name": str(c.container_name),
            "container_type": c.container_type.value,
            "base_cost": float(c.base_cost),
            "current_price": float(latest.price) if latest and latest.price else None,
            "lowest_price": float(latest.lowest_price)
            if latest and latest.lowest_price
            else None,
            "mean_price_30d": mean_30d,
            "volume_7d": int(latest.volume_7d or 0) if latest else 0,
        }
    finally:
        db.close()


# ─── CACHE-1 reader functions ──────────────────────────────────────────────────


def get_cached_portfolio_advice() -> dict | None:
    """
    Return the latest FactPortfolioAdvice row as a dict matching allocate_portfolio()
    output shape, or None if the cache is empty.

    JSON blob columns are deserialised back to Python dicts/lists.
    """
    import json

    from src.domain.models import FactPortfolioAdvice

    db = SessionLocal()
    try:
        row = db.query(FactPortfolioAdvice).order_by(FactPortfolioAdvice.computed_at.desc()).first()
        if row is None:
            return None

        def _loads(val: str | None):  # type: ignore[return]
            if val is None:
                return None
            try:
                return json.loads(val)
            except (ValueError, TypeError):
                return None

        return {
            "computed_at": row.computed_at,
            "total_balance": row.wallet,
            "total_capital": row.total_capital,
            "inventory_value": row.inventory_value,
            "flip_budget": row.flip_budget,
            "invest_budget": row.invest_budget,
            "reserve_amount": row.reserve_amount,
            "flip": _loads(row.flip_json),
            "invest": _loads(row.invest_json),
            "top_flips": _loads(row.top_flips_json) or [],
            "top_invests": _loads(row.top_invests_json) or [],
            "sell": _loads(row.sell_json) or [],
            "correlation_warning": row.correlation_warning,
        }
    finally:
        db.close()


def get_cached_signals() -> dict[str, dict]:
    """
    Return all FactInvestmentSignal rows from the most-recent computed_at batch
    as a dict {container_id: signal_dict}, or an empty dict if the cache is empty.
    """
    from sqlalchemy import func

    from src.domain.models import FactInvestmentSignal

    db = SessionLocal()
    try:
        latest_ts = db.query(func.max(FactInvestmentSignal.computed_at)).scalar()
        if latest_ts is None:
            return {}

        rows = (
            db.query(FactInvestmentSignal)
            .filter(FactInvestmentSignal.computed_at == latest_ts)
            .all()
        )
        return {
            str(r.container_id): {
                "verdict": r.verdict,
                "score": r.score,
                "ratio_signal": r.ratio_signal,
                "momentum_signal": r.momentum_signal,
                "trend_signal": r.trend_signal,
                "event_signal": r.event_signal,
                "sell_at_loss": bool(r.sell_at_loss),
                "unrealized_pnl": r.unrealized_pnl,
                "computed_at": r.computed_at,
            }
            for r in rows
        }
    finally:
        db.close()


# ─── Balance domain (migrated from frontend/balance.py — PV-01) ───────────────


def compute_pnl(sell_price: float, buy_price: float) -> float:
    """Net P&L after Steam 15% fee: (sell / fee_divisor - fee_fixed) - buy."""
    return sell_price / settings.steam_fee_divisor - settings.steam_fee_fixed - buy_price


def get_snapshots(days: int = 30) -> list[dict]:
    """Return last N days of portfolio snapshots, oldest first.

    Returns:
        [{date: str, wallet: float, inventory: float, total: float}]
    """
    cutoff = datetime.now(UTC).replace(tzinfo=None) - timedelta(days=days)
    db = SessionLocal()
    try:
        rows = (
            db.query(FactPortfolioSnapshot)
            .filter(FactPortfolioSnapshot.snapshot_date >= cutoff)
            .order_by(FactPortfolioSnapshot.snapshot_date.asc())
            .all()
        )
        return [
            {
                "date": r.snapshot_date.strftime("%Y-%m-%d"),
                "wallet": float(r.wallet),
                "inventory": float(r.inventory or 0),
                "total": float(r.wallet) + float(r.inventory or 0),
            }
            for r in rows
        ]
    finally:
        db.close()


def save_snapshot(
    wallet: float,
    inventory: float = 0,
) -> None:
    """Upsert today's portfolio snapshot (one row per calendar day)."""
    today = (
        datetime.now(UTC).replace(tzinfo=None).replace(hour=0, minute=0, second=0, microsecond=0)
    )
    db = SessionLocal()
    try:
        existing = (
            db.query(FactPortfolioSnapshot)
            .filter(FactPortfolioSnapshot.snapshot_date == today)
            .first()
        )
        if existing:
            existing.wallet = wallet  # type: ignore[assignment]
            existing.inventory = inventory  # type: ignore[assignment]
        else:
            db.add(
                FactPortfolioSnapshot(
                    id=str(uuid.uuid4()),
                    snapshot_date=today,
                    wallet=wallet,
                    inventory=inventory,
                )
            )
        db.commit()
    finally:
        db.close()


def get_transactions(year: int | None = None) -> list[dict]:
    """Return trade records, optionally filtered to a calendar year.

    Returns:
        [{id, date, action, item_name, quantity, price, total, pnl, listing_id, notes}]
    """
    db = SessionLocal()
    try:
        q = db.query(FactTransaction).order_by(FactTransaction.trade_date.desc())
        if year:
            q = q.filter(
                FactTransaction.trade_date >= datetime(year, 1, 1),
                FactTransaction.trade_date < datetime(year + 1, 1, 1),
            )
        rows = q.all()
        return [
            {
                "id": r.id,
                "date": r.trade_date.strftime("%Y-%m-%d"),
                "action": r.action,
                "item_name": r.item_name,
                "quantity": r.quantity,
                "price": r.price,
                "total": r.total,
                "pnl": r.pnl,
                "listing_id": r.listing_id,
                "notes": r.notes or "",
            }
            for r in rows
        ]
    finally:
        db.close()


def add_transaction(
    action: str,
    item_name: str,
    quantity: int,
    price: float,
    *,
    pnl: float | None = None,
    listing_id: str | None = None,
    notes: str = "",
    trade_date: datetime | None = None,
) -> None:
    """Insert a trade record."""
    total = price * quantity
    db = SessionLocal()
    try:
        db.add(
            FactTransaction(
                id=str(uuid.uuid4()),
                trade_date=trade_date or datetime.now(UTC).replace(tzinfo=None),
                action=action.upper(),
                item_name=item_name,
                quantity=quantity,
                price=price,
                total=total,
                pnl=pnl,
                listing_id=listing_id,
                notes=notes,
            )
        )
        db.commit()
    finally:
        db.close()


def delete_transaction(tx_id: str) -> None:
    """Delete a trade record by primary key."""
    db = SessionLocal()
    try:
        db.query(FactTransaction).filter(FactTransaction.id == tx_id).delete()
        db.commit()
    finally:
        db.close()


def get_monthly_pnl(year: int) -> dict[int, float]:
    """
    Return monthly P&L totals for the given year as {1: pnl, ..., 12: pnl}.

    Replaces ``get_transactions(year)`` + Python loop in build_monthly_chart.
    One SQL GROUP BY query instead of loading every row into Python.

    Logic mirrors build_monthly_chart:
      - row.pnl present      → use as-is
      - action == 'SELL'     → +total
      - action == 'BUY'      → -total
      - other (FLIP w/o pnl) → 0
    """
    from sqlalchemy import case, extract, func

    pnl_expr = case(
        (FactTransaction.pnl.isnot(None), FactTransaction.pnl),
        (FactTransaction.action == "SELL", FactTransaction.total),
        (FactTransaction.action == "BUY", -FactTransaction.total),
        else_=0,
    )
    db = SessionLocal()
    try:
        rows = (
            db.query(
                extract("month", FactTransaction.trade_date).label("month"),
                func.sum(pnl_expr).label("pnl"),
            )
            .filter(
                FactTransaction.trade_date >= datetime(year, 1, 1),
                FactTransaction.trade_date < datetime(year + 1, 1, 1),
            )
            .group_by(extract("month", FactTransaction.trade_date))
            .all()
        )
        return {int(r.month): float(r.pnl or 0) for r in rows}
    finally:
        db.close()


def get_annual_summaries() -> list[dict]:
    """Return yearly P&L summary rows, newest year first.

    Returns:
        [{year: int, pnl: float, notes: str}]
    """
    db = SessionLocal()
    try:
        rows = db.query(DimAnnualSummary).order_by(DimAnnualSummary.year.desc()).all()
        return [{"year": r.year, "pnl": r.pnl, "notes": r.notes or ""} for r in rows]
    finally:
        db.close()


def upsert_annual(year: int, pnl: float, notes: str = "") -> None:
    """Upsert a yearly P&L summary row."""
    db = SessionLocal()
    try:
        existing = db.query(DimAnnualSummary).filter(DimAnnualSummary.year == year).first()
        if existing:
            existing.pnl = pnl  # type: ignore[assignment]
            existing.notes = notes  # type: ignore[assignment]
        else:
            db.add(DimAnnualSummary(year=year, pnl=pnl, notes=notes))
        db.commit()
    finally:
        db.close()


def get_balance_data(
    wallet_balance: float,
    inventory_data: list | None,
    *,
    repo: InventoryRepository | None = None,
) -> dict:
    """Aggregate balance summary for the Balance tab.

    Encapsulates: current prices lookup + inventory valuation + 30-day delta.
    Monetary aggregation uses Amount value objects internally; .amount (float)
    is extracted at the return boundary for Dash compatibility.

    Parameters
    ----------
    wallet_balance:
        Steam wallet balance.
    inventory_data:
        Raw inventory item dicts from Steam API (market_hash_name, count).
    repo:
        Optional InventoryRepository for DI / testing.  When None the function
        falls back to the default get_portfolio_data() DB call so all existing
        callers remain unaffected.

    Returns:
        {
            wallet:    float,
            inventory: float,
            total:     float,
            delta:     float | None,   # current total vs oldest snapshot in window
            snapshots: list[dict],     # pass-through to build_30d_chart()
        }
    """
    # Resolve price source via repository or direct DB call (backward compat)
    if repo is not None:
        prices_map = {item["name"]: item for item in repo.get_all_items()}
    else:
        prices_map = {
            name: {"current_price": data.get("current_price")}
            for name, data in get_portfolio_data().items()
        }

    inventory = Amount(0)
    if inventory_data:
        for item in inventory_data:
            name = item.get("market_hash_name", "")
            qty = item.get("count", 1)
            price = (prices_map.get(name) or {}).get("current_price") or 0
            inventory = inventory + Amount(price * qty)

    wallet = Amount(wallet_balance)
    total = wallet + inventory

    snapshots = get_snapshots(30)
    delta: float | None = None
    if snapshots:
        oldest_total = Amount(snapshots[0]["total"])
        delta = (total - oldest_total).amount

    return {
        "wallet": wallet.amount,
        "inventory": inventory.amount,
        "total": total.amount,
        "delta": delta,
        "snapshots": snapshots,
    }
