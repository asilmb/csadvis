"""
Cache writer service — CACHE-1.

Computes and persists allocate_portfolio() and compute_all_investment_signals()
results into fact_portfolio_advice and fact_investment_signals tables so that
tab renders become pure SELECT calls.

Public API:
    refresh_cache(db)                                    — full recompute + write
    write_portfolio_advice(db, result)                   — overwrite advice table
    write_investment_signals(db, signals, computed_at)   — overwrite signals table
    (reader functions live in services/portfolio.py)
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import UTC, datetime

from sqlalchemy.orm import Session

from src.domain.value_objects import Amount, ROI

logger = logging.getLogger(__name__)


# ─── Signal label helpers ──────────────────────────────────────────────────────


def _get_ratio_label(value: ROI | float | None) -> str:
    if value is None:
        return "NEUTRAL"
    pct = value.value * 100.0 if isinstance(value, ROI) else float(value)
    if pct <= -10.0:
        return "CHEAP"
    if pct >= 10.0:
        return "EXPENSIVE"
    return "NEUTRAL"


def _get_momentum_label(value: ROI | float | None) -> str:
    if value is None:
        return "STABLE"
    pct = value.value * 100.0 if isinstance(value, ROI) else float(value)
    if pct >= 5.0:
        return "RISING"
    if pct <= -5.0:
        return "FALLING"
    return "STABLE"


# ─── Write helpers (callers own the transaction / commit) ──────────────────────


def write_portfolio_advice(db: Session, result: dict) -> None:
    """
    Overwrite fact_portfolio_advice with a single row from allocate_portfolio() output.

    Does NOT call db.commit() — the caller owns the transaction.
    """
    from src.domain.models import FactPortfolioAdvice

    db.query(FactPortfolioAdvice).delete()

    row = FactPortfolioAdvice(
        id=str(uuid.uuid4()),
        computed_at=datetime.now(UTC).replace(tzinfo=None),
        wallet=float(result.get("total_balance") or 0),
        total_capital=float(result.get("total_capital") or 0),
        inventory_value=float(result.get("inventory_value") or 0),
        flip_budget=float(result.get("flip_budget") or 0),
        invest_budget=float(result.get("invest_budget") or 0),
        reserve_amount=float(result.get("reserve_amount") or 0),
        flip_json=json.dumps(result.get("flip")) if result.get("flip") is not None else None,
        invest_json=json.dumps(result.get("invest")) if result.get("invest") is not None else None,
        top_flips_json=json.dumps(result.get("top_flips") or []),
        top_invests_json=json.dumps(result.get("top_invests") or []),
        sell_json=json.dumps(result.get("sell") or []),
        correlation_warning=result.get("correlation_warning"),
    )
    db.add(row)


def write_investment_signals(db: Session, signals: dict[str, dict], computed_at: datetime) -> None:
    """
    Overwrite fact_investment_signals with one row per container from
    compute_all_investment_signals() output.

    signals: {container_id: signal_dict}
    Does NOT call db.commit() — the caller owns the transaction.

    Guard: if signals is empty, logs a warning and returns without touching the DB.
    This prevents a silent DELETE leaving the table permanently empty when the engine
    returns no results (e.g. no containers, failed price load, or upstream error).
    """
    from src.domain.models import FactInvestmentSignal

    def _ratio_roi(sig: dict) -> ROI | None:
        pct = sig.get("price_ratio_pct")
        return ROI(pct / 100.0) if pct is not None else None

    def _momentum_roi(sig: dict) -> ROI | None:
        pct = sig.get("momentum_pct")
        return ROI(pct / 100.0) if pct is not None else None

    rows = [
        FactInvestmentSignal(
            id=str(uuid.uuid4()),
            container_id=str(container_id),
            computed_at=computed_at,
            verdict=str(sig.get("verdict") or "UNCERTAIN"),
            score=int(sig.get("score") or 0),
            ratio_signal=_get_ratio_label(_ratio_roi(sig)),
            momentum_signal=_get_momentum_label(_momentum_roi(sig)),
            trend_signal=sig.get("trend_signal"),
            event_signal=sig.get("event_signal"),
            sell_at_loss=1 if sig.get("sell_at_loss") else 0,
            unrealized_pnl=sig.get("unrealized_pnl"),
        )
        for container_id, sig in signals.items()
    ]

    if not rows:
        logger.warning(
            "write_investment_signals: signals dict is empty — skipping DELETE to avoid data loss."
        )
        return

    db.query(FactInvestmentSignal).delete()
    db.bulk_save_objects(rows)


# ─── Order book pre-fetch helper (WALL-1) ─────────────────────────────────────


def _fetch_order_book_data(
    containers: list,
    trade_advice: dict,
    price_data: dict,
) -> dict | None:
    """
    Fetch order book (itemordershistogram) data for containers that are plausible
    flip candidates (have a sell_target and a current price).

    Uses SteamMarketClient.fetch_nameid() + fetch_order_book() with nameid cache.
    Runs synchronously via asyncio.run() — safe because refresh_cache() is called
    from a thread (scheduler/sync button), never inside a running event loop.

    Returns {container_id: {"sell_order_graph": [...], "buy_order_graph": [...]}}
    or None if no Steam cookie is configured.
    """
    import asyncio

    from src.domain.wall_filter import compute_wall_metrics, get_best_buy_order  # noqa: F401
    from scrapper.nameid_cache import load_nameid_cache, save_nameid_cache
    from scrapper.steam.client import SteamMarketClient
    from infra.steam_credentials import get_login_secure

    if not get_login_secure():
        logger.debug("_fetch_order_book_data: no Steam cookie — skipping wall fetch")
        return None

    async def _fetch_all() -> dict:
        client = SteamMarketClient()
        nameid_cache = load_nameid_cache()
        result: dict = {}
        cache_dirty = False

        for c in containers:
            cid = str(c.container_id)
            name = str(c.container_name)
            adv = trade_advice.get(cid, {})
            if not adv.get("sell_target"):
                continue  # not a flip pre-screened container — skip

            # Get or fetch item_nameid
            nameid = nameid_cache.get(name)
            if nameid is None:
                nameid = await client.fetch_nameid(name)
                if nameid is not None:
                    nameid_cache[name] = nameid
                    cache_dirty = True

            if nameid is None:
                logger.debug("_fetch_order_book_data: no nameid for %s — skipping", name)
                continue

            ob = await client.fetch_order_book(nameid)
            if ob:
                result[cid] = ob

        if cache_dirty:
            save_nameid_cache(nameid_cache)

        return result

    try:
        return asyncio.run(_fetch_all())
    except RuntimeError as exc:
        # asyncio.run() fails if called from inside a running event loop (e.g. tests)
        logger.warning(
            "_fetch_order_book_data: asyncio.run() failed (%s) — wall filter disabled", exc
        )
        return None


# ─── Full refresh (engine recompute + write) ───────────────────────────────────


def refresh_cache(db: Session) -> None:
    """
    Recompute allocate_portfolio() + compute_all_investment_signals() and persist
    both result sets to their cache tables.

    Called from:
      - scheduler/tasks.py poll_container_prices() after hourly price commit
      - frontend/callbacks.py sync_all() after Steam data sync

    Does NOT call db.commit() — the caller owns the transaction.
    On any engine error the exception propagates so the caller can log + rollback.
    """
    from src.domain.connection import SessionLocal
    from src.domain.models import DimContainer, DimUserPosition, FactContainerPrice
    from src.domain.investment import compute_all_investment_signals
    from src.domain.portfolio_advisor import allocate_portfolio
    from src.domain.trade_advisor import compute_trade_advice
    from scrapper.steam_wallet import get_saved_balance
    from src.domain.portfolio import get_portfolio_data

    # ── Gather inputs ──────────────────────────────────────────────────────────

    balance = float(get_saved_balance() or 0)
    price_data = get_portfolio_data()  # {name: {current_price, mean_price, quantity, lowest_price}}

    # Load containers and positions (separate session to avoid nested tx issues)
    _db = SessionLocal()
    try:
        containers = _db.query(DimContainer).all()
        positions = _db.query(DimUserPosition).all()
    finally:
        _db.close()

    positions_map: dict = {str(p.container_name): p.buy_date for p in positions}
    positions_buy_price: dict[str, float] = {
        str(p.container_name): float(p.buy_price) for p in positions
    }

    if not containers:
        logger.warning("refresh_cache: no containers — skipping.")
        return

    cids = [str(c.container_id) for c in containers]

    # Bulk-fetch price histories (3 queries, mirrors _render_portfolio logic)
    _db2 = SessionLocal()
    try:
        from sqlalchemy import asc

        def _fetch_histories(source: str | None) -> dict[str, list[dict]]:
            q = _db2.query(FactContainerPrice).filter(FactContainerPrice.container_id.in_(cids))
            if source:
                q = q.filter(FactContainerPrice.source == source)
            rows = q.order_by(asc(FactContainerPrice.timestamp)).all()
            hist: dict[str, list[dict]] = {cid: [] for cid in cids}
            for r in rows:
                cid = str(r.container_id)
                if cid in hist:
                    hist[cid].append(
                        {
                            "timestamp": r.timestamp.strftime("%Y-%m-%d %H:%M"),
                            "price": r.price,
                            "mean_price": r.mean_price,
                            "volume_7d": r.volume_7d,
                        }
                    )
            return hist

        live_histories = _fetch_histories("steam_live")
        full_histories = _fetch_histories(None)
        market_histories = _fetch_histories("steam_market")
    finally:
        _db2.close()

    # ── Compute investment signals ─────────────────────────────────────────────
    invest_signals = compute_all_investment_signals(
        containers, price_data, positions_buy_price=positions_buy_price
    )

    # ── Compute trade advice ───────────────────────────────────────────────────
    trade_advice: dict = {}
    for c in containers:
        cid = str(c.container_id)
        live_hist = live_histories.get(cid, [])
        hist = live_hist if len(live_hist) >= 5 else full_histories.get(cid, [])
        trade_advice[cid] = compute_trade_advice(
            str(c.container_name),
            float(c.base_cost),
            c.container_type.value,
            hist,
        )

    # ── Compute allocation plan ────────────────────────────────────────────────
    price_history: dict = {
        str(c.container_id): market_histories.get(str(c.container_id), []) for c in containers
    }

    # WALL-1: pre-fetch order book data for flip candidate pre-screening
    order_book_data: dict | None = None
    try:
        order_book_data = _fetch_order_book_data(containers, trade_advice, price_data)
    except Exception as _wb_exc:
        logger.warning(
            "refresh_cache: order book pre-fetch failed (%s) — wall filter disabled", _wb_exc
        )

    plan = allocate_portfolio(
        balance=balance,
        inventory_items=[],  # no live inventory in scheduler context
        containers=containers,
        price_data=price_data,
        trade_advice=trade_advice,
        price_history=price_history,
        invest_signals=invest_signals,
        positions_map=positions_map,
        order_book_data=order_book_data,
    )

    # ── Write both caches ──────────────────────────────────────────────────────
    computed_at = datetime.now(UTC).replace(tzinfo=None)
    write_portfolio_advice(db, plan)
    write_investment_signals(db, invest_signals, computed_at)

    logger.info(
        "refresh_cache: wrote portfolio advice + %d investment signals. balance=%s",
        len(invest_signals),
        Amount(balance),
    )
