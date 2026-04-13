"""
Balance page — portfolio progress tracker.

Sections:
  KPI row     — кошелёк | инвентарь | итого | Δ за 30 дней
  30-day chart — три линии: итого / кошелёк / инвентарь
  Monthly bars — текущий год по месяцам (bar chart)
  Annual history — строки за прошлые годы
  Transaction log — таблица сделок
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

import dash_bootstrap_components as dbc
from dash import dcc, html

from config import settings as _settings
from ui.charts import build_30d_chart, build_monthly_chart
from ui.theme import COLORS as _COLORS
from scrapper.steam_wallet import get_saved_balance
from domain.portfolio import get_annual_summaries, get_balance_data, get_monthly_pnl, get_transactions

logger = logging.getLogger(__name__)

_BG = _COLORS["bg"]
_BG2 = _COLORS["bg2"]
_BORDER = _COLORS["border"]
_TEXT = _COLORS["text"]
_MUTED = _COLORS["muted"]
_GOLD = _COLORS["gold"]
_GREEN = _COLORS["green"]
_YELLOW = _COLORS["yellow"]
_RED = _COLORS["red"]
_ORANGE = _COLORS["orange"]
_BLUE = _COLORS["blue"]


# ─── Main renderer ─────────────────────────────────────────────────────────────


def render_balance(wallet_balance: float | None, inventory_data: list | None) -> html.Div:
    """Render the entire Balance tab."""
    if not wallet_balance:
        wallet_balance = get_saved_balance() or 0.0

    data = get_balance_data(wallet_balance, inventory_data)
    wallet = data["wallet"]
    inventory = data["inventory"]
    total = data["total"]
    delta = data["delta"]
    snapshots = data["snapshots"]

    current_year = datetime.now(UTC).replace(tzinfo=None).year
    monthly_pnl = get_monthly_pnl(current_year)   # SQL GROUP BY — no per-row Python loop
    all_tx = get_transactions()
    annual_rows = get_annual_summaries()

    def _fmt(v: float) -> str:
        return f"{int(v):,} {_settings.currency_symbol}"

    def _kpi(label: str, value: str, css: str) -> Any:
        return dbc.Col(
            dbc.Card(
                dbc.CardBody(
                    [
                        html.P(label, className="text-muted mb-0", style={"fontSize": "11px"}),
                        html.H5(value, className=css, style={"margin": "0", "fontSize": "16px"}),
                    ]
                ),
                style={"backgroundColor": _BG2, "border": f"1px solid {_BORDER}"},
            ),
            width=3,
            className="mb-3",
        )

    delta_str = f"{int(delta):+,} {_settings.currency_symbol}" if delta is not None else "—"
    delta_css = "text-success" if (delta or 0) >= 0 else "text-danger"

    kpi_row = dbc.Row(
        [
            _kpi("Кошелёк", _fmt(wallet), "text-info"),
            _kpi("Инвентарь", _fmt(inventory), "text-success"),
            _kpi("Итого", _fmt(total), "text-warning"),
            _kpi("Δ 30 дней", delta_str, delta_css),
        ]
    )

    # ── Snapshot controls ─────────────────────────────────────────────────────
    snapshot_controls = dbc.Card(
        dbc.CardBody(
            [
                dbc.Row(
                    [
                        dbc.Col(
                            html.Div(
                                [
                                    html.Span(
                                        "Кошелёк: ", style={"color": _MUTED, "fontSize": "12px"}
                                    ),
                                    html.Span(
                                        _fmt(wallet),
                                        style={"color": _BLUE, "fontWeight": "bold"},
                                    ),
                                    html.Span(
                                        "   Инвентарь: ",
                                        style={"color": _MUTED, "fontSize": "12px"},
                                    ),
                                    html.Span(
                                        _fmt(inventory),
                                        style={"color": _GREEN, "fontWeight": "bold"},
                                    ),
                                    html.Span(
                                        "   Итого: ", style={"color": _MUTED, "fontSize": "12px"}
                                    ),
                                    html.Span(
                                        _fmt(total),
                                        style={"color": _GOLD, "fontWeight": "bold"},
                                    ),
                                ]
                            ),
                            width=8,
                        ),
                        html.Div(id="balance-snapshot-status", style={"display": "none"}),
                    ],
                    className="align-items-center",
                ),
            ]
        ),
        style={"backgroundColor": _BG2, "border": f"1px solid {_BORDER}", "marginBottom": "16px"},
    )

    # ── 30-day chart ──────────────────────────────────────────────────────────
    chart_30d = dbc.Card(
        dbc.CardBody(
            [
                html.Div(
                    "30 ДНЕЙ — КОШЕЛЁК / ИНВЕНТАРЬ / ИТОГО",
                    style={
                        "color": _MUTED,
                        "fontSize": "10px",
                        "letterSpacing": "1.5px",
                        "marginBottom": "6px",
                    },
                ),
                dcc.Graph(
                    figure=build_30d_chart(snapshots),
                    style={"height": "260px"},
                    config={"displayModeBar": False},
                ),
            ]
        ),
        style={"backgroundColor": _BG2, "border": f"1px solid {_BORDER}", "marginBottom": "16px"},
    )

    # ── Monthly bar chart (current year) ─────────────────────────────────────
    monthly_chart = dbc.Card(
        dbc.CardBody(
            [
                dcc.Graph(
                    figure=build_monthly_chart(current_year, monthly_pnl),
                    style={"height": "230px"},
                    config={"displayModeBar": False},
                ),
            ]
        ),
        style={"backgroundColor": _BG2, "border": f"1px solid {_BORDER}", "marginBottom": "16px"},
    )

    # ── Annual history ────────────────────────────────────────────────────────
    annual_els = []
    for row in annual_rows:
        color = _GREEN if row["pnl"] >= 0 else _RED
        annual_els.append(
            html.Div(
                [
                    html.Span(
                        str(row["year"]) + ": ",
                        style={
                            "color": _MUTED,
                            "fontSize": "13px",
                            "width": "50px",
                            "display": "inline-block",
                        },
                    ),
                    html.Span(
                        f"{int(row['pnl']):+,} {_settings.currency_symbol}",
                        style={
                            "color": color,
                            "fontSize": "14px",
                            "fontWeight": "bold",
                            "width": "150px",
                            "display": "inline-block",
                        },
                    ),
                    html.Span(row["notes"], style={"color": _MUTED, "fontSize": "11px"}),
                ],
                style={"marginBottom": "4px"},
            )
        )

    annual_section = dbc.Card(
        dbc.CardBody(
            [
                html.Div(
                    "ИСТОРИЯ ЛЕТ",
                    style={
                        "color": _MUTED,
                        "fontSize": "10px",
                        "letterSpacing": "1.5px",
                        "marginBottom": "10px",
                    },
                ),
                html.Div(
                    annual_els
                    or [
                        html.Div(
                            "Нет данных — загрузи историю Steam ниже.",
                            style={"color": _MUTED, "fontSize": "12px", "marginBottom": "8px"},
                        )
                    ]
                ),
                html.Div(
                    "Рассчитывается автоматически из истории сделок Steam.",
                    style={
                        "color": _MUTED,
                        "fontSize": "10px",
                        "fontStyle": "italic",
                        "marginTop": "6px",
                    },
                ),
            ]
        ),
        style={"backgroundColor": _BG2, "border": f"1px solid {_BORDER}", "marginBottom": "16px"},
    )

    # ── Transaction log ───────────────────────────────────────────────────────
    _tx_th = {
        "color": _MUTED,
        "fontSize": "11px",
        "backgroundColor": _BG2,
        "border": "none",
        "paddingBottom": "6px",
        "whiteSpace": "nowrap",
    }

    tx_rows = []
    for tx in all_tx:
        ac = tx["action"]
        pnl_cell = html.Td(
            f"{int(tx['pnl']):+,} {_settings.currency_symbol}" if tx.get("pnl") is not None else "—",
            style={
                "color": _GREEN if (tx.get("pnl") or 0) >= 0 else _RED,
                "textAlign": "right",
                "fontWeight": "bold",
            },
        )
        tx_rows.append(
            html.Tr(
                [
                    html.Td(
                        tx["date"],
                        style={"color": _MUTED, "fontSize": "11px", "whiteSpace": "nowrap"},
                    ),
                    html.Td(
                        dbc.Badge(
                            ac,
                            color={"BUY": "success", "SELL": "danger", "FLIP": "warning"}.get(
                                ac, "secondary"
                            ),
                            className="px-2",
                        )
                    ),
                    html.Td(
                        html.Span(tx["item_name"], style={"color": _TEXT, "fontSize": "12px"}),
                        style={
                            "maxWidth": "200px",
                            "overflow": "hidden",
                            "textOverflow": "ellipsis",
                            "whiteSpace": "nowrap",
                        },
                    ),
                    html.Td(
                        str(tx["quantity"]),
                        style={"color": _BLUE, "textAlign": "center", "fontWeight": "bold"},
                    ),
                    html.Td(
                        f"{int(tx['price']):,} {_settings.currency_symbol}", style={"color": _TEXT, "textAlign": "right"}
                    ),
                    html.Td(
                        f"{int(tx['total']):,} {_settings.currency_symbol}",
                        style={"color": _TEXT, "textAlign": "right", "fontWeight": "bold"},
                    ),
                    pnl_cell,
                    html.Td(
                        tx["notes"],
                        style={
                            "color": _MUTED,
                            "fontSize": "10px",
                            "maxWidth": "150px",
                            "overflow": "hidden",
                            "textOverflow": "ellipsis",
                            "whiteSpace": "nowrap",
                        },
                    ),
                ],
                style={"borderBottom": f"1px solid {_BG2}"},
            )
        )

    tx_table = dbc.Table(
        [
            html.Thead(
                html.Tr(
                    [
                        html.Th("Дата", style=_tx_th),
                        html.Th("Тип", style=_tx_th),
                        html.Th("Предмет", style=_tx_th),
                        html.Th("Кол.", style={**_tx_th, "textAlign": "center"}),
                        html.Th("Цена/шт.", style={**_tx_th, "textAlign": "right"}),
                        html.Th("Итого", style={**_tx_th, "textAlign": "right"}),
                        html.Th("P&L", style={**_tx_th, "textAlign": "right"}),
                        html.Th("Заметка", style=_tx_th),
                    ]
                )
            ),
            html.Tbody(
                tx_rows
                or [
                    html.Tr(
                        [
                            html.Td(
                                "Нет сделок — добавь ниже.",
                                colSpan=8,
                                style={
                                    "color": _MUTED,
                                    "textAlign": "center",
                                    "padding": "16px",
                                    "fontSize": "12px",
                                },
                            )
                        ]
                    )
                ]
            ),
        ],
        bordered=False,
        hover=True,
        responsive=True,
        style={"backgroundColor": _BG, "fontSize": "12px"},
    )

    tx_section = dbc.Card(
        dbc.CardBody(
            [
                dbc.Row(
                    [
                        dbc.Col(
                            html.Div(
                                "ИСТОРИЯ СДЕЛОК",
                                style={
                                    "color": _MUTED,
                                    "fontSize": "10px",
                                    "letterSpacing": "1.5px",
                                    "paddingTop": "6px",
                                },
                            ),
                            width=True,
                        ),
                        dbc.Col(
                            dbc.Button(
                                [html.I(className="fa fa-download me-2"), "Загрузить из Steam"],
                                id="steam-history-load-btn",
                                color="secondary",
                                size="sm",
                                n_clicks=0,
                            ),
                            width="auto",
                        ),
                        dbc.Col(
                            html.Div(
                                id="steam-history-status",
                                style={"color": _MUTED, "fontSize": "11px", "paddingTop": "6px"},
                            ),
                            width="auto",
                        ),
                    ],
                    className="align-items-center mb-2",
                ),
                tx_table,
            ]
        ),
        style={"backgroundColor": _BG2, "border": f"1px solid {_BORDER}"},
    )

    return html.Div(
        [
            kpi_row,
            snapshot_controls,
            chart_30d,
            monthly_chart,
            annual_section,
            tx_section,
        ]
    )
