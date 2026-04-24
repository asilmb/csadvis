"""
Market tab renderer — investment signal card + trade advice panel.
"""

from __future__ import annotations

from typing import Any

import dash_bootstrap_components as dbc
from dash import dcc, html

from config import settings
from src.domain.connection import SessionLocal
from src.domain.models import (
    DimContainer,
    DimUserPosition,
    InvestmentPosition,
    InvestmentPositionStatus,
)
from src.domain.trade_advisor import compute_trade_advice
from ui.helpers import (
    _BG2,
    _BLUE,
    _BORDER,
    _FEE_DIV,
    _FEE_FIXED,
    _GREEN,
    _MUTED,
    _ORANGE,
    _RED,
    _TEXT,
    _YELLOW,
    _build_sparkline,
    _get_price_history,
    _no_data,
)
from ui.theme import verdict_color

_VERDICT_INFO = {
    v: (verdict_color(v), v, desc)
    for v, desc in {
        "BUY": "Price is well below baseline and momentum is negative — good entry point.",
        "LEAN BUY": "One of two buy signals is active — consider accumulating.",
        "HOLD": "Price is near fair value. Hold existing position.",
        "LEAN SELL": "One of two sell signals is active — consider trimming position.",
        "SELL": "Price is elevated and momentum is positive — good time to sell.",
        "NO DATA": "No price data available for this container. Run: cs2 backfill",
    }.items()
}


def _render_signal_breakdown(sig: dict) -> list:
    """Visual breakdown of why the verdict is BUY/HOLD/SELL."""
    if not sig.get("current_price"):
        return [
            html.P(
                "Нет данных о ценах. Запустите: cs2 backfill",
                style={"color": _MUTED, "fontSize": "12px"},
            )
        ]

    rp = sig.get("price_ratio_pct", 0)
    mom = sig.get("momentum_pct", 0)

    def _row(icon: Any, label: Any, value: Any, good: bool) -> Any:
        color = _GREEN if good else _RED
        return html.Div(
            [
                html.Span(icon + " ", style={"color": color, "fontSize": "14px"}),
                html.Span(label, style={"color": _MUTED, "fontSize": "12px", "marginRight": "6px"}),
                html.Span(value, style={"color": color, "fontSize": "12px", "fontWeight": "bold"}),
            ],
            style={"marginBottom": "8px"},
        )

    price_good = rp < -5  # below baseline = good to buy
    mom_good = mom < -2  # falling price = good to buy

    rows = [
        _row(
            "↓" if rp < 0 else "↑",
            "Price vs baseline:",
            f"{'−' if rp < 0 else '+'}{abs(rp):.1f}%"
            + (" (cheap)" if rp < -15 else " (expensive)" if rp > 15 else " (fair)"),
            price_good,
        ),
        _row(
            "↓" if mom < 0 else "↑",
            "Price momentum:",
            f"{'−' if mom < 0 else '+'}{abs(mom):.1f}%"
            + (" (falling)" if mom < -5 else " (rising)" if mom > 5 else " (stable)"),
            mom_good,
        ),
    ]

    score = sig.get("score", 0)
    score_text = {
        2: "Strong buy signal",
        1: "Weak buy signal",
        0: "Neutral",
        -1: "Weak sell signal",
        -2: "Strong sell signal",
    }.get(score, "—")
    rows.append(
        html.Div(
            [
                html.Span("●  ", style={"color": _MUTED}),
                html.Span("Combined score: ", style={"color": _MUTED, "fontSize": "12px"}),
                html.Span(
                    score_text,
                    style={
                        "fontSize": "12px",
                        "fontWeight": "bold",
                        "color": _GREEN if score > 0 else _RED if score < 0 else _YELLOW,
                    },
                ),
            ],
            style={"marginTop": "12px"},
        )
    )

    return rows


def _render_market(
    container_id: str | None, invest: dict, raw_items: list, inventory_data: list | None = None
) -> html.Div:
    if not container_id:
        return _no_data("Select a container from the list.")

    db = SessionLocal()
    try:
        c = db.query(DimContainer).filter(DimContainer.container_id == container_id).first()
    finally:
        db.close()

    if not c:
        return _no_data()

    sig = invest.get(container_id, {})
    verdict = sig.get("verdict", "NO DATA")
    vc, vl, vdesc = _VERDICT_INFO.get(verdict, _VERDICT_INFO["NO DATA"])

    # ── Signal card ──────────────────────────────────────────────────────────
    is_weapon_case = c.container_type.value not in {
        "Sticker Capsule",
        "Autograph Capsule",
        "Event Capsule",
    }
    key_note = " (case only, key not included)" if is_weapon_case else ""

    # Owned quantity and unrealized P&L (M-12)
    owned_count = 0
    if inventory_data:
        for item in inventory_data:
            if item.get("market_hash_name") == str(c.container_name):
                owned_count += item.get("count", 1)

    metrics = []
    if sig.get("current_price"):
        rp = sig.get("price_ratio_pct", 0)
        mom = sig.get("momentum_pct", 0)
        metrics = [
            ("Current Price", f"{settings.currency_symbol}{sig['current_price']:.2f}"),
            ("Baseline" + key_note, f"{settings.currency_symbol}{sig['baseline_price']:.2f}"),
            (
                "vs Baseline",
                f"{'↑' if rp >= 0 else '↓'} {abs(rp):.1f}%",
                _GREEN if rp < 0 else _RED,
            ),
            (
                "Momentum",
                f"{'↑' if mom >= 0 else '↓'} {abs(mom):.1f}%",
                _RED if mom > 5 else _GREEN if mom < -5 else _YELLOW,
            ),
            ("Listings (7d)", str(sig.get("quantity", "—"))),
        ]

        # Unrealized P&L — shown only when user owns the container (M-12)
        # Formula: (current_price / 1.15 − fee − cost_basis) × qty
        # cost_basis = buy_price from dim_user_positions (user's actual purchase price)
        # Falls back to base_cost if no position record found.
        if owned_count > 0:
            from src.domain.connection import SessionLocal as _SL

            _pos_db2 = _SL()
            try:
                _pos = (
                    _pos_db2.query(DimUserPosition)
                    .filter(DimUserPosition.container_name == str(c.container_name))
                    .first()
                )
                cost_basis = float(_pos.buy_price) if _pos else float(c.base_cost)
            finally:
                _pos_db2.close()
            net_per_unit = sig["current_price"] / _FEE_DIV - _FEE_FIXED - cost_basis
            unrealized_total = net_per_unit * owned_count
            pnl_color = _GREEN if unrealized_total >= 0 else _RED
            pnl_label = f"{'↑' if unrealized_total >= 0 else '↓'} {abs(unrealized_total):,.0f}{settings.currency_symbol}"
            if owned_count > 1:
                pnl_label += f"  ({owned_count} × {abs(net_per_unit):,.0f}{settings.currency_symbol}/шт)"
            metrics.append((f"Unrealized P&L (×{owned_count})", pnl_label, pnl_color))

    metric_els = []
    for m in metrics:
        label, val = m[0], m[1]
        color = m[2] if len(m) > 2 else _TEXT
        metric_els.append(
            html.Div(
                [
                    html.Span(
                        label + ": ",
                        style={
                            "color": _MUTED,
                            "fontSize": "12px",
                            "width": "120px",
                            "display": "inline-block",
                        },
                    ),
                    html.Span(
                        val,
                        style={
                            "color": color,
                            "fontSize": "12px",
                            "fontWeight": "bold",
                        },
                    ),
                ],
                style={"marginBottom": "5px"},
            )
        )

    signal_card = dbc.Card(
        dbc.CardBody(
            [
                dbc.Row(
                    [
                        dbc.Col(
                            [
                                html.Div(
                                    vl,
                                    style={
                                        "fontSize": "32px",
                                        "fontWeight": "bold",
                                        "color": vc,
                                        "letterSpacing": "2px",
                                        "marginBottom": "4px",
                                    },
                                ),
                                html.Div(
                                    str(c.container_name),
                                    style={
                                        "color": _TEXT,
                                        "fontSize": "15px",
                                        "fontWeight": "bold",
                                    },
                                ),
                                html.Div(
                                    c.container_type.value,
                                    style={
                                        "color": _MUTED,
                                        "fontSize": "11px",
                                        "marginBottom": "12px",
                                    },
                                ),
                                html.Div(
                                    vdesc,
                                    style={
                                        "color": _MUTED,
                                        "fontSize": "12px",
                                        "fontStyle": "italic",
                                        "marginBottom": "16px",
                                    },
                                ),
                                *metric_els,
                            ],
                            width=5,
                        ),
                        dbc.Col(
                            [
                                html.Div(
                                    "DECISION BREAKDOWN",
                                    style={
                                        "color": _MUTED,
                                        "fontSize": "10px",
                                        "letterSpacing": "1.5px",
                                        "marginBottom": "8px",
                                    },
                                ),
                                *_render_signal_breakdown(sig),
                            ],
                            width=7,
                        ),
                    ]
                ),
            ]
        ),
        style={
            "backgroundColor": _BG2,
            "border": f"1px solid {vc}",
        },
        className="mb-3",
    )

    # ── Trade advice panel ───────────────────────────────────────────────────
    # Use steam_live records for percentile targets (fiat Steam prices only).
    steam_live_history = _get_price_history(container_id, source="steam_live")
    steam_live_count = len(steam_live_history)
    # Fall back to all sources if not enough live snapshots yet
    steam_history = _get_price_history(container_id) if steam_live_count < 5 else steam_live_history
    adv = compute_trade_advice(
        str(c.container_name),
        float(c.base_cost),
        c.container_type.value,
        steam_history,
    )
    snap_count = steam_live_count
    if adv["data_source"] == "90d_steam":
        src_label = f"{snap_count} Steam snapshots, 90-day percentiles"
    else:
        src_label = f"baseline estimate ({snap_count} snapshot(s) — run cs2 poll to accumulate)"
    margin_color = (
        _GREEN if adv["net_margin_pct"] >= 10 else _YELLOW if adv["net_margin_pct"] >= 0 else _RED
    )

    def _adv_row(label: Any, val: Any, color: Any = _TEXT) -> Any:
        return html.Div(
            [
                html.Span(
                    label + ":",
                    style={
                        "color": _MUTED,
                        "fontSize": "12px",
                        "width": "100px",
                        "display": "inline-block",
                    },
                ),
                html.Span(
                    f"{int(val):,}{settings.currency_symbol}",
                    style={
                        "color": color,
                        "fontSize": "13px",
                        "fontWeight": "bold",
                    },
                ),
            ],
            style={"marginBottom": "5px"},
        )

    # ── Quantity action block ─────────────────────────────────────────────────
    action_els = []
    sell_t = adv["sell_target"]
    buy_t = adv["buy_target"]

    if verdict in ("SELL", "LEAN SELL") and owned_count > 0:
        net_each = round(sell_t / _FEE_DIV - _FEE_FIXED, 0)
        net_total = round(net_each * owned_count, 0)
        action_els = [
            html.Div(
                [
                    html.Span(
                        "ВЫСТАВИТЬ НА ПРОДАЖУ",
                        style={
                            "color": _RED,
                            "fontSize": "11px",
                            "fontWeight": "bold",
                            "letterSpacing": "1px",
                        },
                    ),
                ],
                style={"marginBottom": "6px"},
            ),
            html.Div(
                f"{owned_count} шт.  ×  {int(sell_t):,}{settings.currency_symbol}",
                style={"color": _ORANGE, "fontSize": "14px", "fontWeight": "bold"},
            ),
            html.Div(
                f"Чистыми получишь: {int(net_total):,}{settings.currency_symbol}",
                style={"color": _GREEN, "fontSize": "12px", "marginTop": "4px"},
            ),
            html.Div("(после 15% комиссии Steam)", style={"color": _MUTED, "fontSize": "10px"}),
        ]
    elif verdict in ("BUY", "LEAN BUY"):
        action_els = [
            html.Div(
                [
                    html.Span(
                        "ПОКУПАТЬ",
                        style={
                            "color": _GREEN,
                            "fontSize": "11px",
                            "fontWeight": "bold",
                            "letterSpacing": "1px",
                        },
                    ),
                ],
                style={"marginBottom": "6px"},
            ),
            html.Div(
                f"Цена входа: {int(buy_t):,}{settings.currency_symbol}",
                style={"color": _GREEN, "fontSize": "14px", "fontWeight": "bold"},
            ),
            html.Div(
                f"Цель выхода: {int(sell_t):,}{settings.currency_symbol}",
                style={"color": _ORANGE, "fontSize": "12px", "marginTop": "4px"},
            ),
            html.Div(
                f"Потенциал: {adv['net_margin_pct']:+.0f}% чистыми после Steam",
                style={"color": _MUTED, "fontSize": "10px"},
            ),
        ]
    elif verdict == "HOLD" and owned_count > 0:
        net_each = round(sell_t / _FEE_DIV - _FEE_FIXED, 0)
        action_els = [
            html.Div(
                [
                    html.Span(
                        "ДЕРЖАТЬ",
                        style={
                            "color": _YELLOW,
                            "fontSize": "11px",
                            "fontWeight": "bold",
                            "letterSpacing": "1px",
                        },
                    ),
                ],
                style={"marginBottom": "6px"},
            ),
            html.Div(
                f"У тебя: {owned_count} шт.",
                style={"color": _BLUE, "fontSize": "13px", "fontWeight": "bold"},
            ),
            html.Div(
                f"Продавать когда цена достигнет {int(sell_t):,}{settings.currency_symbol}",
                style={"color": _TEXT, "fontSize": "12px", "marginTop": "4px"},
            ),
            html.Div(
                f"Ожидаемый доход: {int(net_each * owned_count):,}{settings.currency_symbol} чистыми",
                style={"color": _GREEN, "fontSize": "11px"},
            ),
        ]
    else:
        action_els = [
            html.Div(
                adv["hold_detail"],
                style={"color": _TEXT, "fontSize": "12px", "fontStyle": "italic"},
            ),
        ]

    # ── Sparkline (90-day trend embedded in advice card) ─────────────────────
    # Reuse steam_history already fetched above (steam_live or full fallback).
    spark_fig = _build_sparkline(steam_history, adv["buy_target"], adv["sell_target"])

    advice_card = dbc.Card(
        dbc.CardBody(
            [
                dbc.Row(
                    [
                        dbc.Col(
                            [
                                html.Div(
                                    "ТОРГОВЫЕ ТАРГЕТЫ",
                                    style={
                                        "color": _MUTED,
                                        "fontSize": "10px",
                                        "letterSpacing": "1.5px",
                                        "marginBottom": "10px",
                                    },
                                ),
                                _adv_row("Покупать", adv["buy_target"], _GREEN),
                                _adv_row("Продавать", adv["sell_target"], _ORANGE),
                                html.Div(
                                    [
                                        html.Span(
                                            "Маржа:",
                                            style={
                                                "color": _MUTED,
                                                "fontSize": "12px",
                                                "width": "100px",
                                                "display": "inline-block",
                                            },
                                        ),
                                        html.Span(
                                            f"{adv['net_margin_pct']:+.1f}%",
                                            style={
                                                "color": margin_color,
                                                "fontSize": "13px",
                                                "fontWeight": "bold",
                                            },
                                        ),
                                        html.Span(
                                            " после Steam 15%",
                                            style={
                                                "color": _MUTED,
                                                "fontSize": "11px",
                                                "marginLeft": "6px",
                                            },
                                        ),
                                    ],
                                    style={"marginTop": "8px"},
                                ),
                                html.Div(
                                    src_label,
                                    style={
                                        "color": _MUTED,
                                        "fontSize": "9px",
                                        "marginTop": "8px",
                                        "fontStyle": "italic",
                                    },
                                ),
                                html.Div(
                                    "Тренд 90 дней:",
                                    style={
                                        "color": _MUTED,
                                        "fontSize": "9px",
                                        "marginTop": "12px",
                                        "letterSpacing": "1px",
                                    },
                                ),
                                dcc.Graph(
                                    figure=spark_fig,
                                    style={"height": "80px", "marginTop": "2px"},
                                    config={"displayModeBar": False, "staticPlot": True},
                                ),
                            ],
                            width=5,
                        ),
                        dbc.Col(
                            [
                                html.Div(
                                    "РЕКОМЕНДАЦИЯ",
                                    style={
                                        "color": _MUTED,
                                        "fontSize": "10px",
                                        "letterSpacing": "1.5px",
                                        "marginBottom": "10px",
                                    },
                                ),
                                *action_els,
                            ],
                            width=7,
                        ),
                    ]
                ),
            ]
        ),
        style={"backgroundColor": _BG2, "border": f"1px solid {_BORDER}"},
        className="mb-3",
    )

    # ── Investment positions for this container ──────────────────────────────
    _pos_db3 = SessionLocal()
    try:
        open_positions = (
            _pos_db3.query(InvestmentPosition)
            .filter(
                InvestmentPosition.container_id == container_id,
                InvestmentPosition.status != InvestmentPositionStatus.sold,
            )
            .order_by(InvestmentPosition.opened_at.desc())
            .all()
        )
    finally:
        _pos_db3.close()

    positions_section = html.Div()
    if open_positions:
        _status_colors = {"hold": "#ffd600", "on_sale": "#66c0f4", "sold": "#8f98a0"}
        pos_rows = []
        for p in open_positions:
            sc = _status_colors.get(p.status.value, _MUTED)
            pos_rows.append(
                html.Div(
                    [
                        html.Span(p.name, style={"color": _TEXT, "fontSize": "12px", "fontWeight": "bold"}),
                        html.Span(
                            f"  {p.current_count}/{p.fixation_count} шт.",
                            style={"color": _MUTED, "fontSize": "11px", "fontFamily": "monospace"},
                        ),
                        dbc.Badge(
                            p.status.value.upper().replace("_", " "),
                            style={"backgroundColor": sc, "color": "#000", "fontSize": "9px", "marginLeft": "8px"},
                        ),
                        html.Span(
                            f"  buy {p.buy_price:,.0f} ₸  →  target {p.sale_target_price:,.0f} ₸",
                            style={"color": _MUTED, "fontSize": "11px"},
                        ),
                    ],
                    style={"padding": "6px 0", "borderBottom": f"1px solid {_BORDER}"},
                )
            )

        positions_section = dbc.Card(
            dbc.CardBody([
                html.Div(
                    "ПОЗИЦИИ",
                    style={"color": _MUTED, "fontSize": "10px", "letterSpacing": "1.5px", "marginBottom": "8px"},
                ),
                *pos_rows,
            ]),
            style={"backgroundColor": _BG2, "border": f"1px solid {_BORDER}"},
            className="mb-3",
        )

    return html.Div([signal_card, advice_card, positions_section])
