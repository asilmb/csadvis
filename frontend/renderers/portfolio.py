"""
Portfolio tab renderer — 40/40/20 allocation plan, stop-loss alerts, Armory Pass widget.
"""

from __future__ import annotations

import logging

import dash_bootstrap_components as dbc
from dash import dcc, html

from config import settings
from database.connection import SessionLocal
from database.models import DimUserPosition
from engine.portfolio_advisor import allocate_portfolio
from engine.trade_advisor import compute_trade_advice
from frontend.helpers import (
    _BG,
    _BG2,
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
from frontend.url_generator import item_link
from ingestion.steam_wallet import get_saved_balance

logger = logging.getLogger(__name__)


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


def _render_portfolio(
    balance: float | None,
    inventory_data: list | None,
    invest_signals: dict,
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

    # CACHE-1: try reading cached plan first — avoids full engine recompute on every render
    from services.portfolio import get_cached_portfolio_advice

    _cached_plan = get_cached_portfolio_advice()

    if _cached_plan is not None:
        plan = _cached_plan
    else:
        # Cache miss (cold start or first run) — fall through to full live compute
        containers = _get_containers()
        price_data = _get_current_steam_prices()
        inv_items = inventory_data or []

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
        f = plan["flip"]
        flip_card = dbc.Card(
            dbc.CardBody(
                [
                    dbc.Row(
                        [
                            dbc.Col(
                                [
                                    item_link(
                                        f["name"],
                                        color=_GOLD,
                                        font_size="15px",
                                    ),
                                    html.Div(
                                        "Лучший кандидат для флипа",
                                        style={
                                            "color": _MUTED,
                                            "fontSize": "11px",
                                            "marginBottom": "12px",
                                        },
                                    ),
                                    _pfrow(
                                        "Покупать",
                                        f"{f['qty']} шт. × {settings.currency_symbol}{f['buy_price']:.2f}",
                                        f"= {int(f['buy_price'] * f['qty']):,} {settings.currency_symbol} total",
                                        _GREEN,
                                    ),
                                    _pfrow(
                                        "Продавать",
                                        f"{settings.currency_symbol}{f['sell_price']:.2f} / шт.",
                                        f"{int(f['sell_price']):,} {settings.currency_symbol}",
                                        _ORANGE,
                                    ),
                                    _pfrow(
                                        "Чистыми/шт.",
                                        f"{settings.currency_symbol}{f['net_per_unit']:.2f}",
                                        f"{int(f['net_per_unit']):,} {settings.currency_symbol}",
                                        _GREEN,
                                    ),
                                    _pfrow(
                                        "Итого профит:",
                                        f"{settings.currency_symbol}{f['expected_net_total']:.2f}",
                                        f"{int(f['expected_net_total']):,} {settings.currency_symbol}",
                                        _GREEN,
                                    ),
                                ],
                                width=6,
                            ),
                            dbc.Col(
                                [
                                    html.Div(
                                        "МЕТРИКИ",
                                        style={
                                            "color": _MUTED,
                                            "fontSize": "10px",
                                            "letterSpacing": "1.5px",
                                            "marginBottom": "8px",
                                        },
                                    ),
                                    _pfrow(
                                        "Маржа (net)",
                                        f"{f['net_margin_pct']:+.1f}%",
                                        "после Steam 15%",
                                        _GREEN if f["net_margin_pct"] >= 10 else _YELLOW,
                                    ),
                                    _pfrow(
                                        "Объём/неделю",
                                        str(f["weekly_volume"]) + " шт.",
                                        "7-day volume",
                                        _TEXT,
                                    ),
                                    _pfrow(
                                        "Волатильность",
                                        f"{f['volatility_pct']:.1f}%",
                                        "30d std/mean",
                                        _GREEN if f["volatility_pct"] < 8 else _YELLOW,
                                    ),
                                    _pfrow(
                                        "Bid-ask spread",
                                        f"{f.get('spread_pct', 0):.1f}%",
                                        "узкий = хорошо для выхода",
                                        _GREEN if f.get("spread_pct", 0) < 8 else _YELLOW,
                                    ),
                                    _pfrow(
                                        "Ликвидность",
                                        f"{f.get('avg_daily_vol', 0):.1f} шт/день",
                                        "средний дневной объём",
                                        _TEXT,
                                    ),
                                    _pfrow(
                                        "Flip score",
                                        f"{f['flip_score']:.4f}",
                                        "выше = лучше",
                                        _GOLD,
                                    ),
                                    # WALL-1: order book metrics
                                    _pfrow(
                                        "Объём до цели",
                                        f"{f.get('volume_to_target', 0):,} шт.",
                                        "лоты в стене до target",
                                        _RED
                                        if f.get("volume_to_target", 0) > 200
                                        else _YELLOW
                                        if f.get("volume_to_target", 0) > 50
                                        else _MUTED,
                                    ),
                                    _pfrow(
                                        "Дней выхода",
                                        f"{f.get('estimated_days', 0.0):.1f} дн.",
                                        "объём / avg_daily_vol",
                                        _GREEN
                                        if f.get("estimated_days", 0.0) <= 3
                                        else _YELLOW
                                        if f.get("estimated_days", 0.0) <= 7
                                        else _RED,
                                    ),
                                    _pfrow(
                                        "Безубыток",
                                        f"{int(f['buy_price'] * 1.15):,} {settings.currency_symbol}",
                                        "покупка × 1.15 (Steam fee)",
                                        _TEXT,
                                    ),
                                    _pfrow(
                                        "Лучший bid",
                                        f"{f.get('best_buy_order', 0):,} {settings.currency_symbol}"
                                        if f.get("best_buy_order", 0) > 0
                                        else "—",
                                        "цена немедленной ликвидации",
                                        _TEXT if f.get("best_buy_order", 0) > 0 else _MUTED,
                                    ),
                                    html.Div(
                                        "Трейд-бан 7 дней после покупки на Steam Market",
                                        style={
                                            "color": _MUTED,
                                            "fontSize": "10px",
                                            "marginTop": "12px",
                                            "fontStyle": "italic",
                                        },
                                    ),
                                ],
                                width=6,
                            ),
                        ]
                    ),
                ]
            ),
            style={
                "backgroundColor": _BG2,
                "border": f"1px solid {_ORANGE}",
                "marginBottom": "8px",
            },
        )

        alt_flips = plan["top_flips"][1:4]
        alt_els = []
        if alt_flips:
            alt_els = [
                html.Div(
                    "Альтернативы:",
                    style={"color": _MUTED, "fontSize": "10px", "marginBottom": "4px"},
                ),
            ] + [
                html.Span(
                    f"{a['name']} (score {a['flip_score']:.4f}, +{a['net_margin_pct']:.0f}%)",
                    style={"color": _MUTED, "fontSize": "11px", "display": "block"},
                )
                for a in alt_flips
            ]

        flip_section = html.Div(
            [
                flip_header,
                flip_card,
                html.Div(alt_els, style={"paddingLeft": "8px", "marginBottom": "16px"}),
            ]
        )

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
        invest_card = dbc.Card(
            dbc.CardBody(
                [
                    dbc.Row(
                        [
                            dbc.Col(
                                [
                                    item_link(
                                        iv["name"],
                                        color=_GOLD,
                                        font_size="15px",
                                    ),
                                    html.Div(
                                        "CS2 золото — лучший долгосрочный актив",
                                        style={
                                            "color": _MUTED,
                                            "fontSize": "11px",
                                            "marginBottom": "12px",
                                        },
                                    ),
                                    _pfrow(
                                        "Покупать",
                                        f"{iv['qty']} шт. × {settings.currency_symbol}{iv['buy_price']:.2f}",
                                        f"= {int(iv['buy_price'] * iv['qty']):,} {settings.currency_symbol}",
                                        _GREEN,
                                    ),
                                    _pfrow("Бюджет", _fmt_amount(iv["budget_used"]), "", _GREEN),
                                    _pfrow(
                                        "История",
                                        f"{iv['history_years']} лет",
                                        "данных Steam Market",
                                        _TEXT,
                                    ),
                                ],
                                width=6,
                            ),
                            dbc.Col(
                                [
                                    html.Div(
                                        "МЕТРИКИ",
                                        style={
                                            "color": _MUTED,
                                            "fontSize": "10px",
                                            "letterSpacing": "1.5px",
                                            "marginBottom": "8px",
                                        },
                                    ),
                                    _pfrow(
                                        "CAGR",
                                        f"{iv['cagr_pct']:+.1f}% / год",
                                        "среднегодовой рост",
                                        _GREEN if iv["cagr_pct"] >= 15 else _YELLOW,
                                    ),
                                    _pfrow(
                                        "Волатильность",
                                        f"{iv['volatility_pct']:.1f}%",
                                        "180d std/mean",
                                        _GREEN if iv["volatility_pct"] < 20 else _YELLOW,
                                    ),
                                    _pfrow(
                                        "Invest score",
                                        f"{iv['invest_score']:.4f}",
                                        "CAGR × (1 − vol)",
                                        _GOLD,
                                    ),
                                    html.Div(
                                        "Держать минимум 6–12 месяцев. Не трогать при краткосрочных просадках.",
                                        style={
                                            "color": _MUTED,
                                            "fontSize": "10px",
                                            "marginTop": "12px",
                                            "fontStyle": "italic",
                                        },
                                    ),
                                ],
                                width=6,
                            ),
                        ]
                    ),
                ]
            ),
            style={
                "backgroundColor": _BG2,
                "border": f"1px solid {_GREEN}",
                "marginBottom": "8px",
            },
        )

        alt_invs = plan["top_invests"][1:4]
        alt_inv_els = []
        if alt_invs:
            alt_inv_els = [
                html.Div(
                    "Альтернативы:",
                    style={"color": _MUTED, "fontSize": "10px", "marginBottom": "4px"},
                ),
            ] + [
                html.Span(
                    f"{a['name']} (CAGR {a['cagr_pct']:+.1f}%, vol {a['volatility_pct']:.1f}%)",
                    style={"color": _MUTED, "fontSize": "11px", "display": "block"},
                )
                for a in alt_invs
            ]

        inv_section = html.Div(
            [
                inv_header,
                invest_card,
                html.Div(alt_inv_els, style={"paddingLeft": "8px", "marginBottom": "16px"}),
            ]
        )

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
                                    "Звёзд в пассе / за контейнер",
                                    style={"color": _MUTED, "fontSize": "11px"},
                                ),
                                dbc.Row(
                                    [
                                        dbc.Col(
                                            dbc.Input(
                                                id="ap-stars-in-pass-input",
                                                type="number",
                                                min=1,
                                                step=1,
                                                value=5,
                                                placeholder="5",
                                                style={
                                                    "backgroundColor": _BG2,
                                                    "color": _TEXT,
                                                    "fontSize": "12px",
                                                },
                                            ),
                                        ),
                                        dbc.Col(
                                            dbc.Input(
                                                id="ap-stars-per-case-input",
                                                type="number",
                                                min=1,
                                                step=1,
                                                value=1,
                                                placeholder="1",
                                                style={
                                                    "backgroundColor": _BG2,
                                                    "color": _TEXT,
                                                    "fontSize": "12px",
                                                },
                                            ),
                                        ),
                                    ],
                                    className="g-1",
                                ),
                            ],
                            md=3,
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
