"""
Portfolio tab renderer — 40/40/20 allocation plan, stop-loss alerts, Armory Pass widget.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import dash_bootstrap_components as dbc
from dash import dcc, html

from config import settings
from scrapper.steam_wallet import get_saved_balance
from src.domain.connection import SessionLocal
from src.domain.models import (
    DimUserPosition,
    FactContainerPrice,
    InvestmentPosition,
    InvestmentPositionStatus,
    InvestmentPositionType,
)
from src.domain.portfolio_advisor import allocate_portfolio
from src.domain.trade_advisor import compute_trade_advice
from ui.helpers import (
    _BG,
    _BG2,
    _BLUE,
    _BORDER,
    _FEE_DIV,
    _FEE_FIXED,
    _GOLD,
    _GREEN,
    _MUTED,
    _ORANGE,
    _RED,
    _TEXT,
    _YELLOW,
    _get_all_price_histories,
    _get_containers,
    _get_current_steam_prices,
    _kpi_card,
)
from ui.url_generator import item_link

logger = logging.getLogger(__name__)


def _get_positions_with_price(db, position_type: InvestmentPositionType) -> list[tuple]:
    """Return [(InvestmentPosition, current_price | None)] for open positions."""
    positions = (
        db.query(InvestmentPosition)
        .filter(
            InvestmentPosition.position_type == position_type,
            InvestmentPosition.status != InvestmentPositionStatus.sold,
        )
        .order_by(InvestmentPosition.opened_at.desc())
        .all()
    )
    result = []
    for p in positions:
        latest = (
            db.query(FactContainerPrice)
            .filter(FactContainerPrice.container_id == p.container_id)
            .order_by(FactContainerPrice.timestamp.desc())
            .first()
        )
        result.append((p, latest.price if latest else None))
    return result


def _position_card(p: InvestmentPosition, current_price: float | None) -> dbc.Card:
    """Render a single InvestmentPosition card."""
    accent = _ORANGE if p.position_type == InvestmentPositionType.flip else _GREEN
    pct = (p.current_count / p.fixation_count * 100) if p.fixation_count else 0

    status_colors = {
        InvestmentPositionStatus.hold:    _YELLOW,
        InvestmentPositionStatus.on_sale: _BLUE,
        InvestmentPositionStatus.sold:    _MUTED,
    }
    status_color = status_colors.get(p.status, _MUTED)

    # Trade ban from linked BUY groups — fetched lazily via relationship
    from src.domain.models import PositionTransactionGroup, TransactionGroup, TransactionDirection
    db_session = SessionLocal()
    try:
        buy_ptg = (
            db_session.query(PositionTransactionGroup)
            .join(TransactionGroup,
                  TransactionGroup.id == PositionTransactionGroup.transaction_group_id)
            .filter(
                PositionTransactionGroup.position_id == p.id,
                TransactionGroup.direction == TransactionDirection.BUY,
                TransactionGroup.trade_ban_expires_at.isnot(None),
            )
            .order_by(TransactionGroup.trade_ban_expires_at.asc())
            .first()
        )
        ban_expires = (
            buy_ptg.transaction_group.trade_ban_expires_at if buy_ptg else None
        )
    finally:
        db_session.close()

    now = datetime.now(UTC).replace(tzinfo=None)
    if ban_expires and ban_expires > now:
        diff = ban_expires - now
        days = diff.days
        hours = diff.seconds // 3600
        ban_el = dbc.Badge(
            f"🔒 TRADE BAN {days}d {hours}h",
            color="warning",
            style={"fontSize": "10px", "fontFamily": "monospace"},
        )
    elif ban_expires:
        ban_el = dbc.Badge("✓ TRADEABLE", color="success", style={"fontSize": "10px"})
    else:
        ban_el = html.Span()

    delta_el = html.Span()
    if current_price is not None:
        delta_pct = (current_price - p.buy_price) / p.buy_price * 100
        delta_color = _GREEN if delta_pct >= 0 else _RED
        sign = "+" if delta_pct >= 0 else ""
        delta_el = html.Span(
            f"{sign}{delta_pct:.1f}%",
            style={"color": delta_color, "fontSize": "10px", "marginLeft": "6px", "fontWeight": "bold"},
        )

    return dbc.Card(
        dbc.CardBody(
            [
                dbc.Row([
                    dbc.Col([
                        html.Div(p.name, style={"color": _TEXT, "fontSize": "13px", "fontWeight": "bold"}),
                        html.Div(
                            [
                                html.Span(
                                    p.status.value.upper().replace("_", " "),
                                    style={"color": status_color, "fontWeight": "bold", "fontSize": "10px", "letterSpacing": "0.5px"},
                                ),
                                html.Span(f" · открыта {p.opened_at.strftime('%Y-%m-%d')}", style={"color": _MUTED, "fontSize": "10px"}),
                            ],
                            style={"marginTop": "2px"},
                        ),
                    ], width=5),
                    dbc.Col([
                        html.Div("BUY", style={"color": _MUTED, "fontSize": "10px"}),
                        html.Div(f"{p.buy_price:,.0f} ₸", style={"color": _TEXT, "fontWeight": "bold", "fontSize": "12px"}),
                    ], width=2),
                    dbc.Col([
                        html.Div("NOW", style={"color": _MUTED, "fontSize": "10px"}),
                        html.Div(
                            [
                                html.Span(
                                    f"{current_price:,.0f} ₸" if current_price else "—",
                                    style={"color": _BLUE, "fontWeight": "bold", "fontSize": "12px"},
                                ),
                                delta_el,
                            ]
                        ),
                    ], width=2),
                    dbc.Col([
                        html.Div("TARGET", style={"color": _MUTED, "fontSize": "10px"}),
                        html.Div(f"{p.sale_target_price:,.0f} ₸", style={"color": _GOLD, "fontWeight": "bold", "fontSize": "12px"}),
                    ], width=3),
                ]),
                # Progress bar
                html.Div(
                    [
                        dbc.Progress(
                            value=pct,
                            style={"height": "5px", "marginTop": "10px", "marginBottom": "4px"},
                            color="warning" if p.position_type == InvestmentPositionType.flip else "success",
                        ),
                        html.Div(
                            [
                                html.Span(
                                    f"{p.current_count}/{p.fixation_count} units",
                                    style={"color": _MUTED, "fontSize": "10px", "fontFamily": "monospace"},
                                ),
                                ban_el,
                            ],
                            style={"display": "flex", "justifyContent": "space-between"},
                        ),
                    ]
                ),
                # Action row
                html.Div(
                    dbc.Button(
                        [html.I(className="fa fa-trash me-1"), "Удалить"],
                        id={"type": "btn-pos-delete", "pos_id": p.id},
                        size="sm",
                        color="danger",
                        outline=True,
                        n_clicks=0,
                        style={"fontSize": "10px", "marginTop": "8px"},
                    ),
                ),
            ],
            style={"padding": "10px 14px"},
        ),
        style={
            "backgroundColor": "#1a2433",
            "borderLeft": f"3px solid {accent}",
            "border": f"1px solid {_BORDER}",
            "marginBottom": "8px",
        },
    )


def _render_positions_section(positions_with_price: list, label: str) -> html.Div:
    """Render a section of position cards with a header."""
    if not positions_with_price:
        return html.Div()
    return html.Div(
        [
            html.Div(
                label,
                style={
                    "color": _MUTED, "fontSize": "10px", "letterSpacing": "1.5px",
                    "textTransform": "uppercase", "fontWeight": "600",
                    "margin": "14px 0 8px",
                },
            ),
            html.Div([_position_card(p, cp) for p, cp in positions_with_price]),
        ]
    )


def _armorypass_card(p: InvestmentPosition, current_price: float | None) -> dbc.Card:
    """Render one Armory Pass position card with editable progress."""
    import json as _j
    try:
        linked_ids = _j.loads(p.linked_asset_ids or "[]")
    except Exception:
        linked_ids = []
    linked_count = len(linked_ids)
    pct = (p.current_count / p.fixation_count * 100) if p.fixation_count else 0
    net_per_unit = round(current_price / 1.15 - 5, 0) if current_price else None
    net_total = net_per_unit * p.current_count if net_per_unit else None
    profit_color = (
        _GREEN if net_per_unit and net_per_unit > p.buy_price else _RED
    ) if net_per_unit else _MUTED

    return dbc.Card(
        dbc.CardBody(
            [
                dbc.Row(
                    [
                        dbc.Col(
                            [
                                html.Div(p.name, style={"color": _TEXT, "fontSize": "13px", "fontWeight": "bold"}),
                                html.Div(
                                    f"Себестоимость: {p.buy_price:,.0f} ₸/шт.  ·  "
                                    f"Безубыток: {p.sale_target_price:,.0f} ₸",
                                    style={"color": _MUTED, "fontSize": "10px", "marginTop": "2px"},
                                ),
                            ],
                            width=8,
                        ),
                        dbc.Col(
                            html.Div(
                                f"{current_price:,.0f} ₸" if current_price else "Нет цены",
                                style={
                                    "color": profit_color,
                                    "fontWeight": "bold",
                                    "fontSize": "12px",
                                    "textAlign": "right",
                                },
                            ),
                            width=4,
                        ),
                    ]
                ),
                dbc.Progress(
                    value=pct,
                    label=f"{p.current_count} / {p.fixation_count}",
                    style={"height": "16px", "marginTop": "10px", "marginBottom": "8px"},
                    color="warning",
                ),
                dbc.Row(
                    [
                        dbc.Col(
                            dbc.Input(
                                id={"type": "ap-pos-count-input", "pos_id": p.id},
                                type="number",
                                min=0,
                                max=p.fixation_count,
                                step=1,
                                value=p.current_count,
                                style={
                                    "backgroundColor": "#0f1923",
                                    "color": _TEXT,
                                    "fontSize": "12px",
                                    "border": f"1px solid {_BORDER}",
                                },
                            ),
                            width=2,
                        ),
                        dbc.Col(
                            html.Span(
                                f"/ {p.fixation_count} кейсов",
                                style={"color": _MUTED, "fontSize": "12px", "lineHeight": "38px"},
                            ),
                            width=2,
                        ),
                        dbc.Col(
                            dbc.Button(
                                [html.I(className="fa fa-save me-1"), "Сохранить"],
                                id={"type": "ap-pos-save", "pos_id": p.id},
                                size="sm",
                                color="success",
                                outline=True,
                                n_clicks=0,
                                style={"fontSize": "11px"},
                            ),
                            width="auto",
                        ),
                        dbc.Col(
                            dbc.Button(
                                [html.I(className="fa fa-refresh me-1"), "Сбросить"],
                                id={"type": "ap-pos-reset", "pos_id": p.id},
                                size="sm",
                                color="secondary",
                                outline=True,
                                n_clicks=0,
                                style={"fontSize": "11px"},
                            ),
                            width="auto",
                        ),
                        dbc.Col(
                            dbc.Button(
                                [
                                    html.I(className="fa fa-link me-1"),
                                    f"Инвентарь ({linked_count}/{p.fixation_count})",
                                ],
                                id={"type": "ap-pos-link-inv", "pos_id": p.id},
                                size="sm",
                                color="info" if linked_count == p.fixation_count else "warning",
                                outline=True,
                                n_clicks=0,
                                style={"fontSize": "11px"},
                            ),
                            width="auto",
                        ),
                        dbc.Col(
                            dbc.Button(
                                [html.I(className="fa fa-trash me-1"), "Удалить"],
                                id={"type": "btn-pos-delete", "pos_id": p.id},
                                size="sm",
                                color="danger",
                                outline=True,
                                n_clicks=0,
                                style={"fontSize": "11px"},
                            ),
                            width="auto",
                        ),
                        dbc.Col(
                            html.Div(
                                id={"type": "ap-pos-status", "pos_id": p.id},
                                style={"fontSize": "11px", "lineHeight": "38px"},
                            ),
                            width=True,
                        ),
                    ],
                    className="align-items-center g-2",
                ),
                html.Div(
                    html.Span(
                        f"Чистыми: {int(net_per_unit):,} ₸/шт.  ·  "
                        f"Итого {p.current_count} шт.: {int(net_total):,} ₸",
                        style={"color": profit_color, "fontSize": "11px"},
                    )
                    if net_per_unit is not None else html.Span(),
                    style={"marginTop": "6px"},
                ),
            ],
            style={"padding": "10px 14px"},
        ),
        style={
            "backgroundColor": "#1a2433",
            "borderLeft": f"3px solid {_GOLD}",
            "border": f"1px solid {_BORDER}",
            "marginBottom": "8px",
        },
    )


def _render_armorypass_section(
    positions_with_price: list,
    armory_store: dict,
    containers,
) -> html.Div:
    """Armory Pass position tracker — placed right after the AP calculator."""
    _ap = armory_store or {}
    selected_name = _ap.get("container")

    # Resolve container_id from selected name for the create button
    cid_by_name = {str(c.container_name): str(c.container_id) for c in containers}
    selected_cid = cid_by_name.get(selected_name) if selected_name else None

    children: list = [
        html.Div(
            "ARMORY PASS — ПОЗИЦИИ",
            style={
                "color": _MUTED,
                "fontSize": "10px",
                "letterSpacing": "1.5px",
                "textTransform": "uppercase",
                "fontWeight": "600",
                "margin": "16px 0 8px",
            },
        ),
        dbc.Row(
            [
                dbc.Col(
                    dbc.Button(
                        [html.I(className="fa fa-star me-1"), "Создать AP позицию"],
                        id="btn-create-ap-position",
                        size="sm",
                        color="warning",
                        outline=True,
                        n_clicks=0,
                        disabled=not bool(selected_cid and _ap.get("pass_cost")),
                        style={"fontSize": "11px"},
                    ),
                    width="auto",
                ),
                dbc.Col(
                    html.Div(
                        id="ap-pos-create-status",
                        style={"fontSize": "11px", "color": _MUTED, "lineHeight": "32px"},
                    ),
                    width=True,
                ),
            ],
            className="align-items-center mb-2 g-2",
        ),
    ]

    if not positions_with_price:
        children.append(
            html.Div(
                "Нет активных AP позиций. Выбери контейнер в калькуляторе выше и нажми «Создать AP позицию».",
                style={"color": _MUTED, "fontSize": "12px", "fontStyle": "italic"},
            )
        )
    else:
        children.extend([_armorypass_card(p, cp) for p, cp in positions_with_price])

    return html.Div(children)


def _pfrow(label: str, value: str, sub: str = "", color: str = _TEXT) -> html.Div:
    """One row in a portfolio card."""
    return html.Div(
        [
            html.Span(
                label + ":",
                style={
                    "color": _MUTED,
                    "fontSize": "12px",
                    "width": "130px",
                    "display": "inline-block",
                },
            ),
            html.Span(
                value,
                style={
                    "color": color,
                    "fontSize": "13px",
                    "fontWeight": "bold",
                    "marginRight": "8px",
                },
            ),
            html.Span(sub, style={"color": _MUTED, "fontSize": "11px"}),
        ],
        style={"marginBottom": "5px"},
    )


_RANK_LABELS = ["#1  ЛУЧШИЙ", "#2", "#3", "#4"]
_RANK_COLORS = [_GOLD, _TEXT, _MUTED, _MUTED]


def _flip_candidate_card(c: dict, rank: int, show_create_btn: bool = False) -> dbc.Card:
    """Compact flip candidate card for 2×2 grid."""
    is_top = rank == 1
    border_col = _ORANGE if is_top else _BORDER
    return dbc.Card(
        dbc.CardBody([
            html.Div([
                html.Span(
                    _RANK_LABELS[rank - 1],
                    style={"color": _RANK_COLORS[rank - 1], "fontSize": "9px",
                           "letterSpacing": "1.5px", "fontWeight": "bold"},
                ),
                html.Br(),
                item_link(c["name"], font_size="12px"),
            ], style={"marginBottom": "8px"}),
            dbc.Row([
                dbc.Col([
                    html.Div("МАРЖА", style={"color": _MUTED, "fontSize": "9px", "letterSpacing": "1px"}),
                    html.Div(f"+{c['net_margin_pct']:.0f}%",
                             style={"color": _GREEN, "fontWeight": "bold", "fontSize": "16px"}),
                ], width=6),
                dbc.Col([
                    html.Div("SCORE", style={"color": _MUTED, "fontSize": "9px", "letterSpacing": "1px"}),
                    html.Div(f"{c['flip_score']:.4f}",
                             style={"color": _GOLD, "fontWeight": "bold", "fontSize": "16px"}),
                ], width=6),
            ], className="mb-2"),
            dbc.Row([
                dbc.Col([
                    html.Div("ОБЪ/НЕД", style={"color": _MUTED, "fontSize": "9px", "letterSpacing": "1px"}),
                    html.Div(f"{c['weekly_volume']} шт.",
                             style={"color": _TEXT, "fontSize": "11px"}),
                ], width=6),
                dbc.Col([
                    html.Div("СПРЕД", style={"color": _MUTED, "fontSize": "9px", "letterSpacing": "1px"}),
                    html.Div(f"{c.get('spread_pct', 0):.1f}%",
                             style={"color": _GREEN if c.get("spread_pct", 0) < 8 else _YELLOW,
                                    "fontSize": "11px"}),
                ], width=6),
            ]),
            *([dbc.Button(
                [html.I(className="fa fa-plus me-1"), "Создать позицию"],
                id="btn-create-flip-position",
                size="sm", color="success", outline=True, n_clicks=0,
                style={"fontSize": "10px", "marginTop": "8px", "width": "100%"},
            )] if show_create_btn else []),
        ], style={"padding": "10px"}),
        style={"backgroundColor": _BG2, "border": f"1px solid {border_col}", "height": "100%"},
    )


def _invest_candidate_card(c: dict, rank: int, show_create_btn: bool = False) -> dbc.Card:
    """Compact invest candidate card for 2×2 grid."""
    is_top = rank == 1
    border_col = _GREEN if is_top else _BORDER
    return dbc.Card(
        dbc.CardBody([
            html.Div([
                html.Span(
                    _RANK_LABELS[rank - 1],
                    style={"color": _RANK_COLORS[rank - 1], "fontSize": "9px",
                           "letterSpacing": "1.5px", "fontWeight": "bold"},
                ),
                html.Br(),
                item_link(c["name"], font_size="12px"),
            ], style={"marginBottom": "8px"}),
            dbc.Row([
                dbc.Col([
                    html.Div("CAGR", style={"color": _MUTED, "fontSize": "9px", "letterSpacing": "1px"}),
                    html.Div(f"{c['cagr_pct']:+.1f}%",
                             style={"color": _GREEN if c["cagr_pct"] >= 15 else _YELLOW,
                                    "fontWeight": "bold", "fontSize": "16px"}),
                ], width=6),
                dbc.Col([
                    html.Div("ВОЛАТ.", style={"color": _MUTED, "fontSize": "9px", "letterSpacing": "1px"}),
                    html.Div(f"{c['volatility_pct']:.1f}%",
                             style={"color": _GREEN if c["volatility_pct"] < 20 else _YELLOW,
                                    "fontWeight": "bold", "fontSize": "16px"}),
                ], width=6),
            ], className="mb-2"),
            dbc.Row([
                dbc.Col([
                    html.Div("ИСТОРИЯ", style={"color": _MUTED, "fontSize": "9px", "letterSpacing": "1px"}),
                    html.Div(f"{c['history_years']:.1f} лет",
                             style={"color": _TEXT, "fontSize": "11px"}),
                ], width=6),
                dbc.Col([
                    html.Div("SCORE", style={"color": _MUTED, "fontSize": "9px", "letterSpacing": "1px"}),
                    html.Div(f"{c['invest_score']:.4f}",
                             style={"color": _GOLD, "fontSize": "11px"}),
                ], width=6),
            ]),
            *([dbc.Button(
                [html.I(className="fa fa-plus me-1"), "Создать позицию"],
                id="btn-create-invest-position",
                size="sm", color="success", outline=True, n_clicks=0,
                style={"fontSize": "10px", "marginTop": "8px", "width": "100%"},
            )] if show_create_btn else []),
        ], style={"padding": "10px"}),
        style={"backgroundColor": _BG2, "border": f"1px solid {border_col}", "height": "100%"},
    )


def _render_portfolio(
    balance: float | None,
    inventory_data: list | None,
    invest_signals: dict,
    armory_store: dict | None = None,
) -> html.Div:
    """Portfolio advisor tab — 40/40/20 allocation plan."""
    # Try saved balance if not yet submitted
    if not balance:
        balance = get_saved_balance()

    if not balance or balance <= 0:
        return html.Div(
            [
                html.P(
                    "Баланс кошелька загружается автоматически. Нажми «Обновить баланс Steam» если не загрузился.",
                    style={"color": _MUTED, "textAlign": "center", "paddingTop": "40px"},
                ),
                html.P(
                    "Если нет инвентаря — всё равно можно рассчитать план для флипа и инвестиции.",
                    style={"color": _MUTED, "textAlign": "center", "fontSize": "12px"},
                ),
            ]
        )

    containers = _get_containers()
    inv_items = inventory_data or []

    # CACHE-1: try reading cached plan first — avoids full engine recompute on every render
    from src.domain.portfolio import get_cached_portfolio_advice

    _cached_plan = get_cached_portfolio_advice()

    if _cached_plan is not None:
        plan = _cached_plan
    else:
        # Cache miss (cold start or first run) — fall through to full live compute
        price_data = _get_current_steam_prices()

        # F-03: load user positions for 7-day trade ban gate
        _pos_db = SessionLocal()
        try:
            _positions = _pos_db.query(DimUserPosition).all()
            positions_map: dict = {str(p.container_name): p.buy_date for p in _positions}
        finally:
            _pos_db.close()

        # Bulk-fetch all price histories in 2 queries instead of N*2
        cids = [str(c.container_id) for c in containers]
        _live_histories = _get_all_price_histories(cids, source="steam_live")
        _full_histories = _get_all_price_histories(cids, source=None)
        _market_histories = _get_all_price_histories(cids, source="steam_market")

        # Compute trade advice for all containers
        trade_advice: dict = {}
        for c in containers:
            cid = str(c.container_id)
            live_hist = _live_histories.get(cid, [])
            hist = live_hist if len(live_hist) >= 5 else _full_histories.get(cid, [])
            trade_advice[c.container_id] = compute_trade_advice(
                str(c.container_name),
                float(c.base_cost),
                c.container_type.value,
                hist,
            )

        # Price history for CAGR (use steam_market = backfill, has years of data)
        price_history: dict = {
            str(c.container_id): _market_histories.get(str(c.container_id), []) for c in containers
        }

        plan = allocate_portfolio(
            balance=float(balance),
            inventory_items=inv_items,
            containers=containers,
            price_data=price_data,
            trade_advice=trade_advice,
            price_history=price_history,
            invest_signals=invest_signals,
            positions_map=positions_map,
        )

    # ── Load investment positions ─────────────────────────────────────────────
    _pos_session = SessionLocal()
    try:
        flip_positions    = _get_positions_with_price(_pos_session, InvestmentPositionType.flip)
        invest_positions  = _get_positions_with_price(_pos_session, InvestmentPositionType.investment)
        armorypass_positions = _get_positions_with_price(_pos_session, InvestmentPositionType.armorypass)
    finally:
        _pos_session.close()

    def _fmt_amount(v: float) -> str:
        return f"{int(v):,} {settings.currency_symbol}"

    # ── KPI bar ──────────────────────────────────────────────────────────────
    kpi_row = dbc.Row(
        [
            _kpi_card("Баланс", _fmt_amount(plan["total_balance"]), "text-info"),
            _kpi_card("Флип (40%)", _fmt_amount(plan["flip_budget"]), "text-warning"),
            _kpi_card("Инвестиция (40%)", _fmt_amount(plan["invest_budget"]), "text-success"),
            _kpi_card("Резерв (20%)", _fmt_amount(plan["reserve_amount"]), "text-secondary"),
        ],
        className="mb-3",
    )

    sections: list = [kpi_row]

    # ── Stop-loss alert (Batch C — F-01) ─────────────────────────────────────
    cid_to_container = {str(c.container_id): c for c in containers}
    inv_map: dict[str, int] = {}
    for item in inv_items:
        n = item.get("market_hash_name", "")
        inv_map[n] = inv_map.get(n, 0) + item.get("count", 1)

    stop_loss_items = []
    for cid, sig in invest_signals.items():
        if sig.get("sell_at_loss") and sig.get("current_price"):
            c = cid_to_container.get(cid)
            if c is None:
                continue
            owned_qty = inv_map.get(str(c.container_name), 0)
            if owned_qty == 0:
                continue
            current_p = sig["current_price"]
            cost_basis = float(c.base_cost)
            net_per_unit = current_p / _FEE_DIV - _FEE_FIXED
            loss_per_unit = net_per_unit - cost_basis
            stop_loss_items.append(
                html.Li(
                    [
                        html.Span(
                            str(c.container_name),
                            style={"fontWeight": "bold", "marginRight": "8px"},
                        ),
                        html.Span(
                            f"x{owned_qty}  |  "
                            f"Цена: {int(current_p):,}{settings.currency_symbol}  |  "
                            f"Себестоимость: {int(cost_basis):,}{settings.currency_symbol}  |  "
                            f"Убыток/шт.: {int(loss_per_unit):,}{settings.currency_symbol}",
                            style={"fontSize": "12px"},
                        ),
                    ],
                    style={"marginBottom": "4px"},
                )
            )
    if stop_loss_items:
        sections.append(
            dbc.Alert(
                [
                    html.Strong("Стоп-лосс: продавай с убытком"),
                    html.P(
                        "Следующие контейнеры имеют SELL-сигнал, но цена ниже себестоимости. "
                        "Решай сам: зафиксировать убыток сейчас или держать до восстановления.",
                        style={"fontSize": "12px", "marginTop": "4px", "marginBottom": "8px"},
                    ),
                    html.Ul(stop_loss_items, style={"marginBottom": "0", "paddingLeft": "20px"}),
                ],
                color="danger",
                style={"marginBottom": "16px", "fontSize": "13px"},
            )
        )

    # ── Step 0: SELL ──────────────────────────────────────────────────────────
    sell_header = html.Div(
        "ШАГ 0 — ПРОДАТЬ (сначала реализовать активы из инвентаря)",
        style={
            "color": _RED,
            "fontWeight": "bold",
            "fontSize": "12px",
            "letterSpacing": "1px",
            "marginBottom": "8px",
            "marginTop": "4px",
        },
    )

    if not plan["sell"]:
        sell_section = html.Div(
            [
                sell_header,
                html.Div(
                    "Нет SELL-сигналов в инвентаре — ничего продавать сейчас не нужно."
                    if inv_items
                    else "Загрузи инвентарь (вкладка Inventory), чтобы увидеть SELL-сигналы.",
                    style={
                        "color": _MUTED,
                        "fontSize": "12px",
                        "paddingLeft": "12px",
                        "marginBottom": "16px",
                    },
                ),
            ]
        )
    else:
        _th = {
            "color": _MUTED,
            "fontSize": "11px",
            "backgroundColor": _BG2,
            "border": "none",
            "paddingBottom": "6px",
            "whiteSpace": "nowrap",
        }
        sell_rows = []
        for s in plan["sell"]:
            sell_rows.append(
                html.Tr(
                    [
                        html.Td(dbc.Badge(s["verdict"], color="danger", className="px-2")),
                        html.Td(item_link(s["name"], color=_TEXT, font_size="12px")),
                        html.Td(
                            str(s["qty"]),
                            style={"color": _TEXT, "textAlign": "center", "fontWeight": "bold"},
                        ),
                        html.Td(
                            f"{s['sell_target']:,} {settings.currency_symbol}",
                            style={"color": _ORANGE, "textAlign": "right"},
                        ),
                        html.Td(
                            f"{int(s['net_each']):,} {settings.currency_symbol}",
                            style={"color": _MUTED, "textAlign": "right", "fontSize": "11px"},
                        ),
                        html.Td(
                            f"{int(s['net_total']):,} {settings.currency_symbol}",
                            style={"color": _GREEN, "textAlign": "right", "fontWeight": "bold"},
                        ),
                    ],
                    style={"borderBottom": f"1px solid {_BG2}"},
                )
            )

        sell_table = dbc.Table(
            [
                html.Thead(
                    html.Tr(
                        [
                            html.Th("Сигнал", style=_th),
                            html.Th("Контейнер", style=_th),
                            html.Th("Кол.", style={**_th, "textAlign": "center"}),
                            html.Th("Цена выхода", style={**_th, "textAlign": "right"}),
                            html.Th("Чистыми/шт.", style={**_th, "textAlign": "right"}),
                            html.Th("Итого чистыми", style={**_th, "textAlign": "right"}),
                        ]
                    )
                ),
                html.Tbody(sell_rows),
            ],
            bordered=False,
            hover=True,
            responsive=True,
            style={"backgroundColor": _BG, "fontSize": "12px", "marginBottom": "0"},
        )
        sell_section = html.Div(
            [
                sell_header,
                dbc.Card(
                    dbc.CardBody(sell_table),
                    style={
                        "backgroundColor": _BG2,
                        "border": f"1px solid {_RED}33",
                        "marginBottom": "16px",
                    },
                ),
            ]
        )

    sections.append(sell_section)

    # ── Step 1: FLIP ──────────────────────────────────────────────────────────
    flip_header = html.Div(
        f"ШАГ 1 — ФЛИП  (40% = {_fmt_amount(plan['flip_budget'])})",
        style={
            "color": _ORANGE,
            "fontWeight": "bold",
            "fontSize": "12px",
            "letterSpacing": "1px",
            "marginBottom": "8px",
        },
    )

    if not plan["flip"]:
        flip_section = html.Div(
            [
                flip_header,
                html.Div(
                    "Нет подходящих кандидатов для флипа: нет прибыльных позиций "
                    "с достаточным объёмом и низкой волатильностью. "
                    "Накопи больше snapshots (cs2 poll) или добавь steam_market backfill.",
                    style={
                        "color": _MUTED,
                        "fontSize": "12px",
                        "paddingLeft": "12px",
                        "marginBottom": "16px",
                    },
                ),
            ]
        )
    else:
        top4_flips = plan["top_flips"][:4]
        # Build 2×2 grid — pad with None if fewer than 4 candidates
        while len(top4_flips) < 4:
            top4_flips.append(None)

        def _flip_col(cand, rank):
            if cand is None:
                return dbc.Col(width=6)
            return dbc.Col(_flip_candidate_card(cand, rank, show_create_btn=(rank == 1)), width=6,
                           style={"marginBottom": "8px"})

        flip_grid = html.Div([
            dbc.Row([_flip_col(top4_flips[0], 1), _flip_col(top4_flips[1], 2)],
                    className="g-2 mb-2"),
            dbc.Row([_flip_col(top4_flips[2], 3), _flip_col(top4_flips[3], 4)],
                    className="g-2"),
        ], style={"marginBottom": "12px"})

        # Detail strip for top candidate (buy/sell/profit summary)
        f = plan["flip"]
        flip_detail = dbc.Card(
            dbc.CardBody(
                dbc.Row([
                    dbc.Col(_pfrow("Покупать",
                                   f"{f['qty']} шт. × {settings.currency_symbol}{f['buy_price']:.2f}",
                                   f"= {int(f['buy_price'] * f['qty']):,} {settings.currency_symbol}",
                                   _GREEN), width=3),
                    dbc.Col(_pfrow("Продавать",
                                   f"{settings.currency_symbol}{f['sell_price']:.2f} / шт.",
                                   f"target", _ORANGE), width=3),
                    dbc.Col(_pfrow("Чистыми/шт.",
                                   f"{settings.currency_symbol}{f['net_per_unit']:.2f}",
                                   "после Steam 15%", _GREEN), width=3),
                    dbc.Col(_pfrow("Итого профит",
                                   f"{int(f['expected_net_total']):,} {settings.currency_symbol}",
                                   f"выход ~{f.get('estimated_days', 0):.1f} дн.", _GREEN), width=3),
                ])
            ),
            style={"backgroundColor": _BG2, "border": f"1px solid {_ORANGE}33", "marginBottom": "8px"},
        )

        flip_section = html.Div([
            flip_header,
            flip_grid,
            flip_detail,
            _render_positions_section(flip_positions, f"Ваши флип-позиции · {len(flip_positions)}"),
        ])

    sections.append(flip_section)

    # ── Step 2: INVEST ────────────────────────────────────────────────────────
    inv_header = html.Div(
        f"ШАГ 2 — ИНВЕСТИЦИЯ  (40% = {_fmt_amount(plan['invest_budget'])})",
        style={
            "color": _GREEN,
            "fontWeight": "bold",
            "fontSize": "12px",
            "letterSpacing": "1px",
            "marginBottom": "8px",
        },
    )

    if not plan["invest"]:
        inv_section = html.Div(
            [
                inv_header,
                html.Div(
                    "Нет данных для CAGR-расчёта. Запусти cs2 backfill, "
                    "чтобы загрузить 7-летнюю историю цен Steam Market.",
                    style={
                        "color": _MUTED,
                        "fontSize": "12px",
                        "paddingLeft": "12px",
                        "marginBottom": "16px",
                    },
                ),
            ]
        )
    else:
        iv = plan["invest"]
        top4_invests = plan["top_invests"][:4]
        while len(top4_invests) < 4:
            top4_invests.append(None)

        def _inv_col(cand, rank):
            if cand is None:
                return dbc.Col(width=6)
            return dbc.Col(_invest_candidate_card(cand, rank, show_create_btn=(rank == 1)), width=6,
                           style={"marginBottom": "8px"})

        invest_grid = html.Div([
            dbc.Row([_inv_col(top4_invests[0], 1), _inv_col(top4_invests[1], 2)],
                    className="g-2 mb-2"),
            dbc.Row([_inv_col(top4_invests[2], 3), _inv_col(top4_invests[3], 4)],
                    className="g-2"),
        ], style={"marginBottom": "12px"})

        # Detail strip for top candidate
        invest_detail = dbc.Card(
            dbc.CardBody(
                dbc.Row([
                    dbc.Col(_pfrow("Покупать",
                                   f"{iv['qty']} шт. × {settings.currency_symbol}{iv['buy_price']:.2f}",
                                   f"= {int(iv['buy_price'] * iv['qty']):,} {settings.currency_symbol}",
                                   _GREEN), width=3),
                    dbc.Col(_pfrow("Бюджет", _fmt_amount(iv["budget_used"]), "", _GREEN), width=3),
                    dbc.Col(_pfrow("История",
                                   f"{iv['history_years']} лет",
                                   "данных Steam Market", _TEXT), width=3),
                    dbc.Col(html.Div(
                        "Держать минимум 6–12 мес.",
                        style={"color": _MUTED, "fontSize": "10px", "fontStyle": "italic",
                               "paddingTop": "8px"},
                    ), width=3),
                ])
            ),
            style={"backgroundColor": _BG2, "border": f"1px solid {_GREEN}33", "marginBottom": "8px"},
        )

        inv_section = html.Div([
            inv_header,
            invest_grid,
            invest_detail,
            _render_positions_section(invest_positions, f"Ваши инвест-позиции · {len(invest_positions)}"),
        ])

    sections.append(inv_section)

    # ── Step 3: RESERVE ───────────────────────────────────────────────────────
    reserve_section = dbc.Card(
        dbc.CardBody(
            [
                html.Div(
                    f"ШАГ 3 — РЕЗЕРВ  (20% = {_fmt_amount(plan['reserve_amount'])})",
                    style={
                        "color": _MUTED,
                        "fontWeight": "bold",
                        "fontSize": "12px",
                        "letterSpacing": "1px",
                        "marginBottom": "8px",
                    },
                ),
                html.Div(
                    f"Держи {int(plan['reserve_amount']):,} {settings.currency_symbol} свободными. "
                    "Резерв нужен для внезапных возможностей, аварийных выходов "
                    "и покрытия убытков если флип пойдёт не по плану.",
                    style={"color": _TEXT, "fontSize": "12px"},
                ),
            ]
        ),
        style={"backgroundColor": _BG2, "border": f"1px solid {_BORDER}", "marginBottom": "16px"},
    )
    sections.append(reserve_section)

    # ── Step 4 — Armory Pass calculator (F-09) ────────────────────────────────
    _ap = armory_store or {}
    container_options = [
        {"label": str(c.container_name), "value": str(c.container_name)} for c in containers
    ]
    armory_section = dbc.Card(
        dbc.CardBody(
            [
                html.Div(
                    "ШАГ 4 — ARMORY PASS vs MARKET",
                    style={
                        "color": _MUTED,
                        "fontWeight": "bold",
                        "fontSize": "12px",
                        "letterSpacing": "1px",
                        "marginBottom": "8px",
                    },
                ),
                html.Div(
                    "Сравни, что выгоднее: получить контейнер через Armory Pass или купить на рынке.",
                    style={"color": _TEXT, "fontSize": "12px", "marginBottom": "10px"},
                ),
                dbc.Row(
                    [
                        dbc.Col(
                            [
                                html.Label(
                                    "Контейнер",
                                    style={"color": _MUTED, "fontSize": "11px"},
                                ),
                                dcc.Dropdown(
                                    id="ap-container-dropdown",
                                    options=container_options,
                                    value=_ap.get("container"),
                                    placeholder="Выбери контейнер…",
                                    style={
                                        "backgroundColor": _BG2,
                                        "color": _TEXT,
                                        "fontSize": "12px",
                                    },
                                    className="dark-dropdown",
                                ),
                            ],
                            md=4,
                        ),
                        dbc.Col(
                            [
                                html.Label(
                                    f"Цена Armory Pass ({settings.currency_symbol})",
                                    style={"color": _MUTED, "fontSize": "11px"},
                                ),
                                dbc.Input(
                                    id="ap-pass-cost-input",
                                    type="number",
                                    min=0,
                                    step=0.01,
                                    value=_ap.get("pass_cost"),
                                    placeholder="напр. 3.99",
                                    style={
                                        "backgroundColor": _BG2,
                                        "color": _TEXT,
                                        "fontSize": "12px",
                                    },
                                ),
                            ],
                            md=3,
                        ),
                        dbc.Col(
                            [
                                html.Label(
                                    "Звёзд в пассе",
                                    style={"color": _MUTED, "fontSize": "11px"},
                                ),
                                dbc.Input(
                                    id="ap-stars-in-pass-input",
                                    type="number",
                                    min=1,
                                    step=1,
                                    value=_ap.get("stars_in_pass", 5),
                                    placeholder="5",
                                    style={
                                        "backgroundColor": _BG2,
                                        "color": _TEXT,
                                        "fontSize": "12px",
                                    },
                                ),
                            ],
                            md=2,
                        ),
                        dbc.Col(
                            [
                                html.Label(
                                    "Звёзд за контейнер",
                                    style={"color": _MUTED, "fontSize": "11px"},
                                ),
                                dbc.Input(
                                    id="ap-stars-per-case-input",
                                    type="number",
                                    min=1,
                                    step=1,
                                    value=_ap.get("stars_per_case", 1),
                                    placeholder="1",
                                    style={
                                        "backgroundColor": _BG2,
                                        "color": _TEXT,
                                        "fontSize": "12px",
                                    },
                                ),
                            ],
                            md=2,
                        ),
                        dbc.Col(
                            [
                                html.Label(" ", style={"fontSize": "11px"}),
                                dbc.Button(
                                    "Подсчитать",
                                    id="ap-calculate-btn",
                                    color="primary",
                                    size="sm",
                                    style={"width": "100%", "fontSize": "12px"},
                                ),
                            ],
                            md=2,
                            style={"display": "flex", "flexDirection": "column", "justifyContent": "flex-end"},
                        ),
                    ],
                    className="g-2 mb-2",
                ),
                html.Div(id="ap-result-output", style={"marginTop": "8px"}),
            ]
        ),
        style={"backgroundColor": _BG2, "border": f"1px solid {_BORDER}", "marginBottom": "16px"},
    )
    sections.append(armory_section)

    # ── Armory Pass positions ─────────────────────────────────────────────────
    sections.append(
        _render_armorypass_section(armorypass_positions, _ap, containers)
    )

    # ── Correlation warning (M-06) ────────────────────────────────────────────
    corr_warn = plan.get("correlation_warning")
    if corr_warn:
        sections.append(
            dbc.Alert(corr_warn, color="warning", style={"fontSize": "12px", "marginTop": "4px"})
        )

    note = dbc.Alert(
        [
            html.Strong("Важно: "),
            "Сначала продай SELL-позиции (Шаг 0), вырученное добавь к балансу и пересчитай план. "
            "Флип — строго один контейнер на весь бюджет 40 %. Трейд-бан 7 дней после покупки в Steam Market. "
            "Инвестиция — долгосрочно, не трогать при просадках.",
        ],
        color="secondary",
        style={"fontSize": "11px", "marginTop": "4px"},
    )
    sections.append(note)

    return html.Div(sections)
