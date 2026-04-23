"""
Shared DB and chart helpers for the CS2 dashboard.

All renderer modules and callbacks import from here to avoid
circular dependencies and code duplication.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import plotly.graph_objects as go
from dash import html

from config import settings as _settings
from src.domain.connection import SessionLocal
from src.domain.models import DimContainer
from src.domain.portfolio import get_portfolio_data
from ui.cache import cache
from ui.theme import COLORS

# ─── Fee constants ─────────────────────────────────────────────────────────────
_FEE_DIV = _settings.steam_fee_divisor  # 1.15
_FEE_FIXED = _settings.steam_fee_fixed  # ~5 (Steam fixed fee per transaction)

# ─── Design token aliases (mirrors app.py constants) ──────────────────────────

_BG = COLORS["bg"]
_BG2 = COLORS["bg2"]
_BG3 = "#1a2e40"  # owned badge background
_BG_SEL = "#1e3045"  # selected card background
_BG_WARN = "#3d2b00"  # stale calendar warning background
_BORDER = COLORS["border"]
_TEXT = COLORS["text"]
_MUTED = COLORS["muted"]
_GOLD = COLORS["gold"]
_GREEN = COLORS["green"]
_YELLOW = COLORS["yellow"]
_RED = COLORS["red"]
_ORANGE = COLORS["orange"]
_BLUE = COLORS["blue"]
_HEATMAP_NEU = "#2d2d00"  # heatmap neutral (zero-correlation, yellow-tinted)


# ─── Async helpers ─────────────────────────────────────────────────────────────


def _run_async(coro: Any) -> Any:
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


# ─── DB helpers ────────────────────────────────────────────────────────────────


@cache.memoize(timeout=300)
def _get_current_steam_prices() -> dict:
    """
    Read the latest Steam Market prices for all tracked containers.

    Returns {container_name: {current_price, mean_price, quantity, lowest_price}}.

    Routes through ItemService (PV-05 service layer). Result is cached in Redis
    for 300 s (PV-10) — data refreshes hourly so 5-minute staleness is safe.
    Falls back to portfolio.get_portfolio_data() on ItemService failure.
    """
    try:
        from src.domain.item_service import ItemService

        svc = ItemService.open()
        try:
            items = svc.get_market_overview()
        finally:
            svc.close()

        if items:
            return {item.name: item.to_price_dict() for item in items}
    except Exception as exc:
        import logging as _logging
        _logging.getLogger(__name__).warning(
            "[helpers] ItemService.get_market_overview failed, falling back: %s", exc
        )

    return get_portfolio_data()


_containers_cache: list = []
_containers_bl_cache: list = []


def _get_containers(force: bool = False, blacklisted: bool = False) -> list:
    global _containers_cache, _containers_bl_cache
    if blacklisted:
        if not force and _containers_bl_cache:
            return _containers_bl_cache
        db = SessionLocal()
        try:
            _containers_bl_cache = db.query(DimContainer).filter(DimContainer.is_blacklisted == 1).all()
            return _containers_bl_cache
        finally:
            db.close()
    else:
        if not force and _containers_cache:
            return _containers_cache
        db = SessionLocal()
        try:
            _containers_cache = db.query(DimContainer).filter(DimContainer.is_blacklisted == 0).all()
            return _containers_cache
        finally:
            db.close()


def invalidate_containers_cache() -> None:
    global _containers_cache, _containers_bl_cache
    _containers_cache = []
    _containers_bl_cache = []


def _get_container_db_map() -> dict:
    """Return {container_name: {base_cost, container_type}} for inventory recommendations."""
    db = SessionLocal()
    try:
        return {
            str(c.container_name): {
                "base_cost": c.base_cost,
                "container_type": c.container_type.value,
            }
            for c in db.query(DimContainer).all()
        }
    finally:
        db.close()


def _get_all_price_histories(
    container_ids: list[str], source: str | None = None
) -> dict[str, list[dict]]:
    """Bulk-fetch price histories for a list of container IDs via ItemService (PV-10).

    Returns {container_id: [{timestamp, price, mean_price, volume_7d}, ...]} sorted by time.
    Single query (N+1 → 1) via ItemService.get_bulk_price_histories().
    """
    if not container_ids:
        return {}
    from src.domain.item_service import ItemService

    svc = ItemService.open()
    try:
        bulk = svc.get_bulk_price_histories(container_ids, source=source)
        return {cid: [d.to_chart_dict() for d in dtos] for cid, dtos in bulk.items()}
    finally:
        svc.close()


def _get_price_history(container_id: str, source: str | None = None) -> list:
    """Return list of {timestamp, price, mean_price, volume_7d} sorted by time.

    Routes through ItemService (PV-10) — no direct FactContainerPrice access.

    source=None         → all records (full history for chart)
    source="steam_live" → live price snapshots only (used for trade targets)
    """
    from src.domain.item_service import ItemService

    svc = ItemService.open()
    try:
        dtos = svc.get_price_history(container_id, source=source)
        return [d.to_chart_dict() for d in dtos]
    finally:
        svc.close()


# ─── Chart helpers ─────────────────────────────────────────────────────────────


def _build_sparkline(
    history: list, buy_target: float | None = None, sell_target: float | None = None
) -> go.Figure:
    """Compact 90-day trend line embedded in the trade advice card.

    Shows price movement only — no axes, no labels, minimal chrome.
    Horizontal dashed lines for buy / sell targets if provided.
    """
    from datetime import timedelta

    cutoff = datetime.now(UTC).replace(tzinfo=None) - timedelta(days=90)
    h90 = [h for h in history if h.get("price")]
    try:
        h90 = [
            h
            for h in h90
            if datetime.fromisoformat(
                h["timestamp"][:16].replace(" ", "T")
                if len(h["timestamp"]) >= 16
                else h["timestamp"]
            )
            >= cutoff
        ]
    except Exception:
        h90 = history[-90:] if len(history) > 90 else history

    fig = go.Figure()
    y_range = None

    if h90:
        xs = [h["timestamp"][:10] for h in h90]
        ys = [h["price"] for h in h90]
        first, last = ys[0], ys[-1]
        line_color = _GREEN if last >= first else _RED

        fig.add_trace(
            go.Scatter(
                x=xs,
                y=ys,
                mode="lines",
                line=dict(color=line_color, width=1.5),
                hovertemplate=f"%{{x}}: %{{y:,.0f}}{_settings.currency_symbol}<extra></extra>",
            )
        )

        # Compute y-range that always includes both price data and target lines
        candidates = [min(ys), max(ys)]
        if buy_target:
            candidates.append(buy_target)
        if sell_target:
            candidates.append(sell_target)
        y_min, y_max = min(candidates), max(candidates)
        pad = (y_max - y_min) * 0.10 or y_max * 0.05
        y_range = [y_min - pad, y_max + pad]

        if buy_target:
            fig.add_hline(
                y=buy_target,
                line=dict(color=_GREEN, width=1, dash="dot"),
                annotation_text=f"  buy {int(buy_target):,}{_settings.currency_symbol}",
                annotation_font=dict(color=_GREEN, size=9),
            )
        if sell_target:
            fig.add_hline(
                y=sell_target,
                line=dict(color=_ORANGE, width=1, dash="dot"),
                annotation_text=f"  sell {int(sell_target):,}{_settings.currency_symbol}",
                annotation_font=dict(color=_ORANGE, size=9),
            )
    else:
        fig.add_annotation(
            text="No data",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
            font=dict(color=_MUTED, size=11),
        )

    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=0, r=40, t=4, b=4),
        xaxis=dict(visible=False),
        yaxis=dict(visible=False, range=y_range if h90 else None),
        showlegend=False,
        height=70,
    )
    return fig


def _build_price_chart(history: list, container_name: str) -> go.Figure:
    fig = go.Figure()
    if not history:
        fig.add_annotation(
            text="No price history yet — prices are collected every hour.<br>Check back later.",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
            font=dict(color=_MUTED, size=14),
        )
    else:
        timestamps = [h["timestamp"] for h in history]
        prices = [h["price"] for h in history]
        means = [h.get("mean_price") for h in history]  # allow None per entry

        fig.add_trace(
            go.Scatter(
                x=timestamps,
                y=prices,
                mode="lines+markers",
                name="Median Price",
                line=dict(color=_GREEN, width=2),
                marker=dict(size=4),
            )
        )
        if any(m is not None for m in means):
            fig.add_trace(
                go.Scatter(
                    x=timestamps,
                    y=means,
                    mode="lines",
                    name="Mean Price",
                    line=dict(color=_YELLOW, width=1, dash="dot"),
                    connectgaps=True,
                )
            )

    fig.update_layout(
        title=dict(text=container_name, font=dict(color=_TEXT, size=13)),
        paper_bgcolor=_BG,
        plot_bgcolor=_BG2,
        font=dict(color=_TEXT),
        xaxis=dict(gridcolor=_BORDER, color=_MUTED),
        yaxis=dict(gridcolor=_BORDER, color=_MUTED, ticksuffix=_settings.currency_symbol),
        legend=dict(bgcolor=_BG2, bordercolor=_BORDER),
        margin=dict(l=50, r=20, t=40, b=40),
        hovermode="x unified",
    )
    return fig


# ─── Header helpers ────────────────────────────────────────────────────────────


def _scheduler_badge() -> html.Span:
    """Return a Celery Beat health badge (checks Redis connectivity)."""
    try:
        import os

        import redis as _redis_lib
        _url = os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0")
        _redis_lib.from_url(_url, socket_connect_timeout=1).ping()
        running = True
    except Exception:
        running = False

    if running:
        return html.Span(
            "Scheduler ON",
            style={
                "color": _GREEN,
                "fontSize": "11px",
                "paddingTop": "8px",
                "paddingRight": "12px",
            },
        )
    # Dashboard runs standalone — scheduler lives in cli/service.py (Celery process).
    # Start `python -m cli` to enable auto-polling.
    return html.Span(
        "Scheduler: запусти python -m cli",
        style={
            "color": _MUTED,
            "fontSize": "11px",
            "paddingTop": "8px",
            "paddingRight": "12px",
        },
    )


# ─── UI helpers ────────────────────────────────────────────────────────────────


def _kpi_card(label: str, value: str, css_class: str) -> Any:
    import dash_bootstrap_components as dbc

    return dbc.Col(
        dbc.Card(
            dbc.CardBody(
                [
                    html.P(label, className="text-muted mb-0", style={"fontSize": "11px"}),
                    html.H5(value, className=css_class, style={"margin": "0"}),
                ]
            ),
            style={"backgroundColor": _BG2, "border": f"1px solid {_BORDER}"},
        ),
        width=3,
        className="mb-3",
    )


def _no_data(msg: str = "Select a container to begin.") -> html.Div:
    return html.Div(
        msg,
        style={"color": _MUTED, "textAlign": "center", "paddingTop": "60px", "fontSize": "16px"},
    )
