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
import math
from datetime import UTC, datetime
from typing import Any

import dash_bootstrap_components as dbc
from dash import dcc, html

from config import settings as _settings
from scrapper.steam_wallet import get_saved_balance
from src.domain.connection import SessionLocal as _SessionLocal
from src.domain.models import LinkStatus, PositionTransactionGroup, TransactionGroup
from src.domain.portfolio import (
    get_annual_summaries,
    get_balance_data,
    get_monthly_pnl,
    get_transactions,
)
from ui.charts import build_30d_chart, build_monthly_chart
from ui.theme import COLORS as _COLORS

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


_TX_PAGE_SIZE = 25


def render_balance(wallet_balance: float | None, inventory_data: list | None, page: int = 1) -> html.Div:
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

    # ── Pagination ────────────────────────────────────────────────────────────
    total_tx = len(all_tx)
    total_pages = max(1, math.ceil(total_tx / _TX_PAGE_SIZE))
    page = max(1, min(page, total_pages))
    paged_tx = all_tx[(page - 1) * _TX_PAGE_SIZE : page * _TX_PAGE_SIZE]

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
    for tx in paged_tx:
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
                html.Div(
                    [
                        dbc.Button(
                            "←", id="tx-prev-btn", size="sm", color="secondary",
                            outline=True, n_clicks=0, disabled=(page == 1),
                            style={"minWidth": "36px"},
                        ),
                        html.Span(
                            f"стр. {page} / {total_pages}  ·  {total_tx} сделок",
                            style={"color": _MUTED, "fontSize": "12px", "padding": "0 12px"},
                        ),
                        dbc.Button(
                            "→", id="tx-next-btn", size="sm", color="secondary",
                            outline=True, n_clicks=0, disabled=(page == total_pages),
                            style={"minWidth": "36px"},
                        ),
                    ],
                    style={
                        "display": "flex", "alignItems": "center",
                        "justifyContent": "center", "marginTop": "10px", "gap": "4px",
                    },
                ),
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
            # Groups section rendered separately — updated by groups-refresh-store callback
            # so group actions (skip/create/link) don't reload the whole balance tab.
            html.Div(id="balance-groups-content", children=render_groups_section()),
            tx_section,
        ]
    )


def render_groups_section() -> dbc.Card:
    """Render the ГРУППЫ ТРАНЗАКЦИЙ card. Called on initial balance load and on every
    groups-refresh-store tick so only this section updates, not the whole balance tab."""
    _grp_db = _SessionLocal()
    try:
        all_groups = (
            _grp_db.query(TransactionGroup, PositionTransactionGroup)
            .outerjoin(
                PositionTransactionGroup,
                PositionTransactionGroup.transaction_group_id == TransactionGroup.id,
            )
            .order_by(TransactionGroup.date_from.desc())
            .all()
        )
    finally:
        _grp_db.close()

    now = datetime.now(UTC).replace(tzinfo=None)

    def _groups_tab_rows(link_status_val: str) -> list:
        rows = []
        for g, ptg in all_groups:
            status = ptg.link_status if ptg else LinkStatus.undefined
            if status != link_status_val:
                continue
            dir_color = _GREEN if g.direction.value == "BUY" else _RED
            ban_el: Any = html.Span()
            if g.trade_ban_expires_at and g.trade_ban_expires_at > now:
                diff = g.trade_ban_expires_at - now
                ban_el = dbc.Badge(
                    f"🔒 {diff.days}d {diff.seconds//3600}h",
                    color="warning", style={"fontSize": "9px", "fontFamily": "monospace"},
                )
            elif link_status_val == "undefined":
                ban_el = dbc.Badge("✓ TRADEABLE", color="success", style={"fontSize": "9px"})

            action_btns: Any = html.Span()
            if link_status_val == "undefined":
                action_btns = html.Div([
                    dbc.Button("Создать", id={"type": "btn-grp-create", "group_id": g.id},
                               size="sm", color="success", outline=True, n_clicks=0,
                               style={"fontSize": "10px", "marginRight": "4px"}),
                    dbc.Button("Привязать", id={"type": "btn-grp-link", "group_id": g.id},
                               size="sm", color="info", outline=True, n_clicks=0,
                               style={"fontSize": "10px", "marginRight": "4px"}),
                    dbc.Button("Пропустить", id={"type": "btn-grp-skip", "group_id": g.id},
                               size="sm", color="secondary", outline=True, n_clicks=0,
                               style={"fontSize": "10px"}),
                ], style={"display": "flex", "gap": "2px"})
            elif link_status_val == "defined":
                action_btns = dbc.Button(
                    "Отвязать", id={"type": "btn-grp-unlink", "group_id": g.id},
                    size="sm", color="secondary", outline=True, n_clicks=0,
                    style={"fontSize": "10px"},
                )
            elif link_status_val == "skipped":
                action_btns = dbc.Button(
                    "Восстановить", id={"type": "btn-grp-restore", "group_id": g.id},
                    size="sm", color="info", outline=True, n_clicks=0,
                    style={"fontSize": "10px"},
                )

            link_el: Any = html.Span()
            if link_status_val == "defined" and ptg and ptg.position_id:
                link_el = html.Span(f"→ pos:{ptg.position_id[:8]}…",
                                    style={"color": _GREEN, "fontSize": "10px"})
            elif link_status_val == "skipped":
                link_el = html.Span("SKIPPED", style={"color": _MUTED, "fontSize": "10px",
                                                       "textDecoration": "line-through"})
            else:
                link_el = ban_el

            rows.append(html.Tr([
                html.Td(html.Span(g.direction.value, style={"color": dir_color, "fontWeight": "bold", "fontSize": "10px"})),
                html.Td(html.Span(g.item_name, style={"color": _TEXT, "fontSize": "12px"})),
                html.Td(f"×{g.count}", style={"color": _BLUE, "fontWeight": "bold", "textAlign": "center",
                                              "fontFamily": "monospace"}),
                html.Td(f"{g.price:,.0f} ₸", style={"textAlign": "right", "fontFamily": "monospace"}),
                html.Td(g.date_from.strftime("%m-%d %H:%M"), style={"color": _MUTED, "fontSize": "11px",
                                                                     "fontFamily": "monospace"}),
                html.Td(link_el),
                html.Td(action_btns),
            ], style={"borderBottom": f"1px solid {_BG}"}))
        return rows or [html.Tr([html.Td(
            "Нет групп.", colSpan=7,
            style={"color": _MUTED, "textAlign": "center", "padding": "14px", "fontSize": "12px", "fontStyle": "italic"},
        )])]

    _grp_th = {"color": _MUTED, "fontSize": "10px", "letterSpacing": "1.2px",
               "textTransform": "uppercase", "backgroundColor": _BG2, "border": "none"}

    def _groups_table(link_status_val: str) -> dbc.Table:
        return dbc.Table(
            [
                html.Thead(html.Tr([
                    html.Th("DIR", style=_grp_th),
                    html.Th("ПРЕДМЕТ", style=_grp_th),
                    html.Th("КОЛ.", style={**_grp_th, "textAlign": "center"}),
                    html.Th("ЦЕНА", style={**_grp_th, "textAlign": "right"}),
                    html.Th("ДИАПАЗОН", style=_grp_th),
                    html.Th("СТАТУС", style=_grp_th),
                    html.Th("ДЕЙСТВИЯ", style=_grp_th),
                ])),
                html.Tbody(_groups_tab_rows(link_status_val)),
            ],
            bordered=False, hover=True, responsive=True,
            style={"backgroundColor": _BG, "fontSize": "12px"},
        )

    undef_count = sum(1 for _, ptg in all_groups if (ptg.link_status if ptg else "undefined") == LinkStatus.undefined)
    def_count   = sum(1 for _, ptg in all_groups if ptg and ptg.link_status == LinkStatus.defined)
    skip_count  = sum(1 for _, ptg in all_groups if ptg and ptg.link_status == LinkStatus.skipped)

    return dbc.Card(
        dbc.CardBody([
            dbc.Row([
                dbc.Col(
                    html.Div("ГРУППЫ ТРАНЗАКЦИЙ",
                             style={"color": _MUTED, "fontSize": "10px", "letterSpacing": "1.5px", "paddingTop": "6px"}),
                    width=True,
                ),
                dbc.Col(
                    dbc.Button(
                        [html.I(className="fa fa-magic me-1"), f"Разобрать предложения ({undef_count})"],
                        id="btn-open-wizard",
                        size="sm", color="info", outline=True, n_clicks=0,
                        style={"fontSize": "11px"},
                    ),
                    width="auto",
                ),
            ], className="align-items-center mb-2"),
            dcc.Tabs(
                value="undefined",
                style={"backgroundColor": _BG2},
                colors={"border": _BORDER, "primary": _BLUE, "background": _BG2},
                children=[
                    dcc.Tab(label=f"Неопределённые ({undef_count})", value="undefined",
                            className="custom-tab",
                            children=[_groups_table("undefined")]),
                    dcc.Tab(label=f"Привязанные ({def_count})", value="defined",
                            className="custom-tab",
                            children=[_groups_table("defined")]),
                    dcc.Tab(label=f"Пропущенные ({skip_count})", value="skipped",
                            className="custom-tab",
                            children=[_groups_table("skipped")]),
                ],
            ),
        ]),
        style={"backgroundColor": _BG2, "border": f"1px solid {_BORDER}", "marginTop": "16px"},
    )
