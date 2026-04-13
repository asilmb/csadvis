"""
Plotly chart builders for the Balance tab.

Functions:
    build_30d_chart(snapshots)          -> go.Figure
    build_monthly_chart(year, transactions) -> go.Figure
"""

from __future__ import annotations

import logging

import plotly.graph_objects as go

from config import settings
from ui.theme import COLORS as _COLORS

logger = logging.getLogger(__name__)

_BG = _COLORS["bg"]
_BG2 = _COLORS["bg2"]
_BORDER = _COLORS["border"]
_TEXT = _COLORS["text"]
_MUTED = _COLORS["muted"]
_GOLD = _COLORS["gold"]
_GREEN = _COLORS["green"]
_RED = _COLORS["red"]
_BLUE = _COLORS["blue"]


def build_30d_chart(snapshots: list[dict]) -> go.Figure:
    """Three-line chart: total / wallet / inventory over last 30 days."""
    fig = go.Figure()
    if not snapshots:
        fig.add_annotation(
            text="Нет данных — нажми «Сохранить снимок»",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
            font=dict(color=_MUTED, size=13),
        )
    else:
        dates = [s["date"] for s in snapshots]
        totals = [s["total"] for s in snapshots]
        wallets = [s["wallet"] for s in snapshots]
        invs = [s["inventory"] for s in snapshots]

        fig.add_trace(
            go.Scatter(
                x=dates,
                y=totals,
                mode="lines",
                name="Итого",
                line=dict(color=_GOLD, width=2),
            )
        )
        fig.add_trace(
            go.Scatter(
                x=dates,
                y=wallets,
                mode="lines",
                name="Кошелёк",
                line=dict(color=_BLUE, width=1.5, dash="dot"),
            )
        )
        fig.add_trace(
            go.Scatter(
                x=dates,
                y=invs,
                mode="lines",
                name="Инвентарь",
                line=dict(color=_GREEN, width=1.5, dash="dash"),
            )
        )

    fig.update_layout(
        paper_bgcolor=_BG,
        plot_bgcolor=_BG2,
        font=dict(color=_TEXT),
        xaxis=dict(gridcolor=_BORDER, color=_MUTED),
        yaxis=dict(gridcolor=_BORDER, color=_MUTED, tickformat=",.0f", ticksuffix=f" {settings.currency_symbol}"),
        legend=dict(
            bgcolor=_BG2,
            bordercolor=_BORDER,
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
        ),
        margin=dict(l=60, r=20, t=40, b=40),
        hovermode="x unified",
        height=240,
    )
    return fig


def build_monthly_chart(year: int, pnl_by_month: dict[int, float]) -> go.Figure:
    """Bar chart: net P&L per month for the given year.

    Parameters
    ----------
    year:
        Calendar year shown in the chart title.
    pnl_by_month:
        Pre-aggregated monthly P&L dict ``{month_int: pnl}`` as returned by
        ``domain.portfolio.get_monthly_pnl(year)``.  Missing months default to 0.
    """
    months = list(range(1, 13))
    month_names = [
        "Янв", "Фев", "Мар", "Апр", "Май", "Июн",
        "Июл", "Авг", "Сен", "Окт", "Ноя", "Дек",
    ]
    # Fill missing months with 0 (SQL GROUP BY omits months with no transactions)
    pnl_by_month = {m: pnl_by_month.get(m, 0.0) for m in months}

    colors = [_GREEN if pnl_by_month[m] >= 0 else _RED for m in months]
    vals = [pnl_by_month[m] for m in months]

    fig = go.Figure(
        go.Bar(
            x=month_names,
            y=vals,
            marker_color=colors,
            text=[f"{int(v):+,} {settings.currency_symbol}" for v in vals],
            textposition="outside",
            textfont=dict(size=10, color=_TEXT),
            hovertemplate=f"%{{x}}: %{{y:+,.0f}} {settings.currency_symbol}<extra></extra>",
        )
    )
    fig.update_layout(
        paper_bgcolor=_BG,
        plot_bgcolor=_BG2,
        font=dict(color=_TEXT),
        xaxis=dict(gridcolor=_BORDER, color=_MUTED),
        yaxis=dict(gridcolor=_BORDER, color=_MUTED, tickformat=",.0f", ticksuffix=f" {settings.currency_symbol}"),
        margin=dict(l=60, r=20, t=10, b=40),
        showlegend=False,
        height=200,
        title=dict(text=f"P&L по месяцам  {year}", font=dict(color=_MUTED, size=11)),
    )
    return fig
