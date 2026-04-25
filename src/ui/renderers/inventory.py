"""
Inventory tab renderer — item table with sell recommendations.
"""

from __future__ import annotations

import logging

import dash_bootstrap_components as dbc
from dash import html

logger = logging.getLogger(__name__)

from config import settings as _settings
from ui.helpers import (
    _BG,
    _BG2,
    _BLUE,
    _BORDER,
    _FEE_DIV,
    _FEE_FIXED,
    _GREEN,
    _MUTED,
    _RED,
    _TEXT,
    _get_containers,
    _get_current_steam_prices,
    _kpi_card,
)
from ui.url_generator import get_market_url

# ── Inventory filtering policy ──────────────────────────────────────────────
# Centralised here so adding a new asset class (e.g. "package") is a one-liner.


class InventoryPolicy:
    """
    Decides which Steam inventory items belong to our tracked universe.

    Rules (in priority order):
    1. Item is in dim_containers DB → in-scope (regardless of type string).
    2. item_type contains one of SCOPE_KEYWORDS → in-scope.
    3. Everything else → out-of-scope (shown dimmed in debug mode, hidden by default).
    """

    SCOPE_KEYWORDS: tuple[str, ...] = (
        "container",
        "capsule",
        "pass",
        "case",
        "terminal",
        "sticker",
        "package",
    )

    def is_in_scope(self, item_type: str, in_db: bool, name: str = "") -> bool:
        if in_db:
            return True
        # Check both the Steam `type` field AND the market_hash_name so that
        # trade-banned cases (marketable=0) whose `type` might be ambiguous
        # are still recognised as in-scope by their name, e.g. "Fever Dream Case".
        haystack = f"{item_type} {name}".lower()
        return any(kw in haystack for kw in self.SCOPE_KEYWORDS)

    def should_show(self, item: dict, show_all: bool) -> bool:
        """Return True if the row should appear in the table."""
        if show_all:
            return True
        # Always show in-scope items regardless of marketable/price status —
        # trade-banned cases still need to be visible for position creation.
        if item["in_scope"]:
            return True
        return item["marketable"] and item["price"] > 0


_POLICY = InventoryPolicy()


# ── Public renderer ──────────────────────────────────────────────────────────


def _render_inventory(
    inventory_data: list | None,
    invest: dict | None = None,
    show_all: bool = False,
) -> html.Div:
    if not inventory_data:
        return html.P(  # type: ignore[return-value]
            "Введи Steam ID выше и нажми «Загрузить инвентарь».",
            style={"color": _MUTED, "textAlign": "center", "paddingTop": "40px"},
        )

    invest = invest or {}

    containers = _get_containers()
    name_to_cid = {str(c.container_name): str(c.container_id) for c in containers}
    price_data = _get_current_steam_prices()

    # ── Enrich all items (one pass, no N+1) ──────────────────────────────────
    all_items: list[dict] = []
    for item in inventory_data:
        name = item.get("market_hash_name") or item.get("name", "")
        count = item.get("count", 1)
        marketable = int(item.get("marketable", 1))
        item_type = item.get("item_type", "")

        cid = name_to_cid.get(name)
        in_scope = _POLICY.is_in_scope(item_type, cid is not None, name=name)

        if not marketable:
            logger.info(
                "[INV] non-marketable item: %r  type=%r  in_scope=%s  in_db=%s",
                name, item_type, in_scope, cid is not None,
            )

        pd = price_data.get(name, {})
        steam_price = pd.get("current_price")
        price = float(steam_price) if steam_price else float(item.get("price") or 0)

        net_per_unit = max(0.0, price / _FEE_DIV - _FEE_FIXED) if price > 0 else 0.0

        verdict = invest.get(cid, {}).get("verdict") if cid else None

        all_items.append(
            {
                "name": name,
                "count": count,
                "price": price,
                "net_per_unit": net_per_unit,
                "total_net": net_per_unit * count,
                "verdict": verdict,
                "marketable": marketable,
                "item_type": item_type,
                "in_scope": in_scope,
            }
        )

    # ── Counters (computed before any filtering) ──────────────────────────────
    total_found = len(all_items)
    scope_count = sum(1 for x in all_items if x["in_scope"])
    hidden_count = total_found - sum(
        1 for x in all_items if _POLICY.should_show(x, show_all=False)
    )

    # ── Apply visibility policy ───────────────────────────────────────────────
    visible = []
    for x in all_items:
        if _POLICY.should_show(x, show_all):
            visible.append(x)
        else:
            logger.debug(
                "[INV] Hidden: %r  in_scope=%s marketable=%s price=%.0f item_type=%r",
                x["name"], x["in_scope"], x["marketable"], x["price"], x["item_type"],
            )
    visible.sort(key=lambda x: (not x["in_scope"], -x["total_net"]))

    # ── Stats bar ─────────────────────────────────────────────────────────────
    stats_bar = dbc.Row(
        [
            dbc.Col(
                html.Span(
                    f"Найдено: {total_found}",
                    style={"color": _TEXT, "fontSize": "12px", "fontWeight": "bold"},
                ),
                width="auto",
            ),
            dbc.Col(
                html.Span(
                    f"Целевых: {scope_count}",
                    style={"color": _GREEN, "fontSize": "12px"},
                ),
                width="auto",
            ),
            dbc.Col(
                html.Span(
                    f"Скрыто: {hidden_count}" + (" (показать всё)" if not show_all and hidden_count else ""),
                    style={"color": _MUTED, "fontSize": "12px"},
                ),
                width="auto",
            ),
        ],
        className="mb-2 g-3",
    )

    # ── Smart empty state ─────────────────────────────────────────────────────
    if not visible:
        msg = (
            f"Кейсы не обнаружены. "
            f"Система скрыла {hidden_count} предм. "
            f"({'скины/наклейки' if hidden_count else 'нет предметов'}), "
            f"так как они не являются контейнерами или не имеют рыночной цены."
        )
        return html.Div(
            [
                stats_bar,
                dbc.Alert(msg, color="warning", style={"fontSize": "12px", "marginTop": "8px"}),
            ]
        )

    # ── KPI cards ─────────────────────────────────────────────────────────────
    total_items_count = sum(x["count"] for x in visible if x["in_scope"])
    total_gross = sum(x["price"] * x["count"] for x in visible if x["in_scope"])
    total_net_all = sum(x["total_net"] for x in visible if x["in_scope"])

    kpi_cards = dbc.Row(
        [
            _kpi_card("Предметов (целевых)", str(total_items_count), "text-info"),
            _kpi_card("Рыночная стоимость", f"{int(total_gross):,} {_settings.currency_symbol}", "text-warning"),
            _kpi_card("Получишь (−15%)", f"{int(total_net_all):,} {_settings.currency_symbol}", "text-success"),
        ],
        className="mb-3",
    )

    # ── Table styles ──────────────────────────────────────────────────────────
    _th = {
        "color": _MUTED,
        "fontSize": "11px",
        "letterSpacing": "1px",
        "backgroundColor": _BG2,
        "border": "none",
        "paddingBottom": "8px",
        "whiteSpace": "nowrap",
    }
    _td: dict = {"border": "none", "padding": "6px 8px", "verticalAlign": "middle"}

    _VERDICT_BADGE_COLOR = {
        "BUY": "success",
        "LEAN BUY": "success",
        "HOLD": "warning",
        "LEAN SELL": "warning",
        "SELL": "danger",
    }

    # ── Table rows ────────────────────────────────────────────────────────────
    rows = []
    for item in visible:
        is_ignored = not item["in_scope"]
        row_style: dict = {"borderBottom": f"1px solid {_BG2}"}
        if is_ignored:
            row_style["opacity"] = "0.45"

        # ROI / verdict cell
        if is_ignored:
            roi_cell = html.Td(
                dbc.Badge("Ignored", color="secondary", style={"fontSize": "9px"}),
                style={**_td, "textAlign": "right"},
            )
        else:
            badge = (
                dbc.Badge(
                    item["verdict"],
                    color=_VERDICT_BADGE_COLOR.get(item["verdict"], "secondary"),
                    style={"fontSize": "9px", "marginLeft": "6px", "verticalAlign": "middle"},
                )
                if item["verdict"]
                else None
            )
            roi_cell = html.Td(
                badge or "—",
                style={**_td, "textAlign": "right"},
            )

        rows.append(
            html.Tr(
                [
                    html.Td(
                        html.Span(
                            [
                                html.Span(item["name"], style={"color": _TEXT, "fontSize": "12px"}),
                                html.A(
                                    html.I(className="fa fa-external-link"),
                                    href=get_market_url(item["name"]),
                                    target="_blank",
                                    rel="noopener noreferrer",
                                    title="Steam Market",
                                    style={"color": _MUTED, "fontSize": "10px", "marginLeft": "6px", "verticalAlign": "middle"},
                                ),
                            ]
                        ),
                        style={
                            **_td,
                            "maxWidth": "340px",
                            "overflow": "hidden",
                            "textOverflow": "ellipsis",
                            "whiteSpace": "nowrap",
                        },
                    ),
                    html.Td(
                        str(item["count"]),
                        style={**_td, "color": _BLUE, "textAlign": "center", "fontWeight": "bold"},
                    ),
                    html.Td(
                        "нет цены — запусти cs2 backfill"
                        if item["in_scope"] and item["price"] == 0
                        else (f"{int(item['price']):,} {_settings.currency_symbol}" if item["price"] > 0 else "—"),
                        style={
                            **_td,
                            "color": _RED
                            if item["in_scope"] and item["price"] == 0
                            else (_TEXT if item["price"] > 0 else _MUTED),
                            "textAlign": "right",
                            "fontSize": "11px" if item["price"] == 0 else "13px",
                        },
                    ),
                    html.Td(
                        "—" if item["net_per_unit"] == 0 else f"{int(item['net_per_unit']):,} {_settings.currency_symbol}",
                        style={**_td, "color": _MUTED, "textAlign": "right"},
                    ),
                    html.Td(
                        "—" if item["total_net"] == 0 else f"{int(item['total_net']):,} {_settings.currency_symbol}",
                        style={**_td, "color": _GREEN, "textAlign": "right", "fontWeight": "bold"},
                    ),
                    roi_cell,
                ],
                style=row_style,
            )
        )

    # Grand total row (in-scope items only)
    rows.append(
        html.Tr(
            [
                html.Td(
                    "ИТОГО (целевые)",
                    colSpan=4,
                    style={
                        **_td,
                        "textAlign": "right",
                        "color": _MUTED,
                        "fontSize": "11px",
                        "letterSpacing": "1px",
                        "fontWeight": "bold",
                        "borderTop": f"1px solid {_BORDER}",
                    },
                ),
                html.Td(
                    f"{int(total_net_all):,} {_settings.currency_symbol}",
                    style={
                        **_td,
                        "color": _GREEN,
                        "textAlign": "right",
                        "fontWeight": "bold",
                        "fontSize": "14px",
                        "borderTop": f"1px solid {_BORDER}",
                    },
                ),
                html.Td("", style={**_td, "borderTop": f"1px solid {_BORDER}"}),
            ]
        )
    )

    table = dbc.Table(
        [
            html.Thead(
                html.Tr(
                    [
                        html.Th("Предмет", style=_th),
                        html.Th("Кол-во", style={**_th, "textAlign": "center"}),
                        html.Th("Цена Steam", style={**_th, "textAlign": "right"}),
                        html.Th("Получишь / шт", style={**_th, "textAlign": "right"}),
                        html.Th("Итого", style={**_th, "textAlign": "right"}),
                        html.Th("ROI", style={**_th, "textAlign": "right"}),
                    ]
                )
            ),
            html.Tbody(rows),
        ],
        bordered=False,
        hover=True,
        responsive=True,
        style={"backgroundColor": _BG, "fontSize": "12px"},
    )

    # ── Footer note ───────────────────────────────────────────────────────────
    no_price_count = sum(1 for x in visible if x["in_scope"] and x["price"] == 0)
    note_parts = [f"Получишь / шт = цена / 1.15 − 5{_settings.currency_symbol}  (Steam берёт ~15% комиссии со сделки)."]
    if no_price_count:
        note_parts.append(
            f"  {no_price_count} предм. без цены — запусти cs2 backfill чтобы загрузить цены."
        )
    if hidden_count and not show_all:
        note_parts.append(
            f"  {hidden_count} предм. скрыто (немаркетируемые / не контейнеры). "
            "Включи «Показать всё» чтобы увидеть их."
        )

    note = html.Div(
        "  ".join(note_parts),
        style={"color": _MUTED, "fontSize": "10px", "fontStyle": "italic", "marginTop": "6px"},
    )

    return html.Div([stats_bar, kpi_cards, table, note])
