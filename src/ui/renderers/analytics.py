"""
Analytics tab renderer — correlation heatmap + event calendar + event impact chart.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import dcc, html

from config import settings as _settings
from domain.correlation import compute_correlation_matrix
from domain.event_calendar import (
    get_event_impact,
    get_event_signals,
    get_upcoming_events,
    is_calendar_stale,
)
from ui.helpers import (
    _BG,
    _BG2,
    _BG_WARN,
    _BLUE,
    _BORDER,
    _GOLD,
    _GREEN,
    _HEATMAP_NEU,
    _MUTED,
    _ORANGE,
    _RED,
    _TEXT,
    _YELLOW,
    _get_all_price_histories,
    _get_containers,
    _get_price_history,
)


def _render_event_impact_card(
    selected_container_id: Any,
    containers: list,
) -> dbc.Card:
    """Render the per-container event impact chart card."""
    _card_title_style = {
        "color": _MUTED,
        "fontSize": "10px",
        "letterSpacing": "1.5px",
        "marginBottom": "12px",
    }

    # Empty state — no container selected
    if not selected_container_id:
        return dbc.Card(
            dbc.CardBody(
                [
                    html.Div("ВЛИЯНИЕ СОБЫТИЙ НА ЦЕНУ", style=_card_title_style),
                    html.P(
                        "Выбери контейнер на вкладке Анализ чтобы увидеть историю цены "
                        "с наложением ивентов.",
                        style={
                            "color": _MUTED,
                            "fontSize": "12px",
                            "textAlign": "center",
                            "padding": "24px 0",
                        },
                    ),
                ]
            ),
            style={
                "backgroundColor": _BG2,
                "border": f"1px solid {_BORDER}",
                "marginTop": "16px",
            },
        )

    # Find container name from id
    cid_str = str(selected_container_id)
    container_name: str | None = None
    for c in containers:
        if str(c.container_id) == cid_str:
            container_name = str(c.container_name)
            break

    if not container_name:
        return dbc.Card(
            dbc.CardBody(html.P("Контейнер не найден.", style={"color": _MUTED})),
            style={"backgroundColor": _BG2, "border": f"1px solid {_BORDER}", "marginTop": "16px"},
        )

    # Fetch price history for this container
    raw_history = _get_price_history(cid_str)
    # Convert to list of (datetime, float) for get_event_impact
    price_tuples: list[tuple[datetime, float]] = []
    for row in raw_history:
        try:
            ts = datetime.strptime(row["timestamp"], "%Y-%m-%d %H:%M")
            price = float(row["price"])
            price_tuples.append((ts, price))
        except (ValueError, KeyError, TypeError):
            continue

    # Get event impact records
    impact_records = get_event_impact(container_name, price_tuples)

    card_title = html.Div(
        f"ВЛИЯНИЕ СОБЫТИЙ — {container_name}",
        style=_card_title_style,
    )

    # No matching events
    if not impact_records and not price_tuples:
        return dbc.Card(
            dbc.CardBody(
                [
                    card_title,
                    html.P(
                        "Нет данных о ценах для этого контейнера.",
                        style={"color": _MUTED, "fontSize": "12px"},
                    ),
                ]
            ),
            style={"backgroundColor": _BG2, "border": f"1px solid {_BORDER}", "marginTop": "16px"},
        )

    # Build price chart
    fig_children: list[Any] = []

    if price_tuples:
        timestamps = [ts.strftime("%Y-%m-%d") for ts, _ in price_tuples]
        prices = [p for _, p in price_tuples]

        fig = go.Figure(
            go.Scatter(
                x=timestamps,
                y=prices,
                mode="lines",
                line=dict(color=_BLUE, width=1.5),
                hovertemplate=f"%{{x}}<br>%{{y:,.0f}} {_settings.currency_symbol}<extra></extra>",
            )
        )

        # Add event overlay shapes and annotations
        shapes = []
        annotations = []
        for rec in impact_records:
            start_str = rec["start_date"]
            end_str = rec["end_date"]
            short_name = " ".join(rec["event_name"].split()[:3])

            shapes.append(
                dict(
                    type="line",
                    x0=start_str,
                    x1=start_str,
                    y0=0,
                    y1=1,
                    yref="paper",
                    line=dict(color=_GREEN, dash="dash", width=1),
                )
            )
            shapes.append(
                dict(
                    type="line",
                    x0=end_str,
                    x1=end_str,
                    y0=0,
                    y1=1,
                    yref="paper",
                    line=dict(color=_RED, dash="dash", width=1),
                )
            )
            annotations.append(
                dict(
                    x=start_str,
                    y=1.02,
                    xref="x",
                    yref="paper",
                    text=short_name,
                    showarrow=False,
                    xanchor="center",
                    font=dict(size=8, color=_MUTED),
                )
            )

        fig.update_layout(
            paper_bgcolor=_BG,
            plot_bgcolor=_BG2,
            font=dict(color=_TEXT, size=9),
            height=280,
            margin=dict(l=60, r=20, t=30, b=40),
            xaxis=dict(
                color=_MUTED,
                tickformat="%d %b %Y",
                gridcolor=_BORDER,
                showgrid=True,
            ),
            yaxis=dict(
                color=_MUTED,
                title=_settings.currency_symbol,
                gridcolor=_BORDER,
                showgrid=True,
            ),
            shapes=shapes,
            annotations=annotations,
        )

        fig_children.append(dcc.Graph(figure=fig, config={"displayModeBar": False}))

    # Impact table
    _th_style = {
        "color": _MUTED,
        "fontSize": "11px",
        "backgroundColor": _BG2,
        "border": "none",
        "paddingBottom": "6px",
    }
    _EV_BADGE_COLOR = {"major": "danger", "premier": "warning", "iem": "info"}

    def _fmt_pct(val: float | None) -> tuple[str, str]:
        """Return (text, color) for a pct value."""
        if val is None:
            return "—", _MUTED
        color = _GREEN if val > 0 else _RED if val < 0 else _MUTED
        return f"{val:+.1f}%", color

    impact_rows = []
    for rec in impact_records:
        pre_text, pre_color = _fmt_pct(rec.get("pct_change_pre"))
        post_text, post_color = _fmt_pct(rec.get("pct_change_post"))
        win_text, win_color = _fmt_pct(rec.get("pct_change_window"))
        ev_type = rec.get("event_type", "")
        impact_rows.append(
            html.Tr(
                [
                    html.Td(
                        rec["event_name"],
                        style={"color": _TEXT, "fontSize": "12px"},
                    ),
                    html.Td(
                        dbc.Badge(
                            ev_type.upper(),
                            color=_EV_BADGE_COLOR.get(ev_type, "secondary"),
                            style={"fontSize": "9px"},
                        ),
                        style={"textAlign": "center"},
                    ),
                    html.Td(
                        pre_text,
                        style={"color": pre_color, "textAlign": "right", "fontSize": "12px"},
                    ),
                    html.Td(
                        post_text,
                        style={"color": post_color, "textAlign": "right", "fontSize": "12px"},
                    ),
                    html.Td(
                        win_text,
                        style={
                            "color": win_color,
                            "textAlign": "right",
                            "fontSize": "12px",
                            "fontWeight": "bold",
                        },
                    ),
                ],
                style={"borderBottom": f"1px solid {_BG2}"},
            )
        )

    if not impact_rows:
        impact_rows = [
            html.Tr(
                html.Td(
                    "Нет прошлых ивентов для этого контейнера.",
                    colSpan=5,
                    style={"color": _MUTED, "textAlign": "center", "padding": "12px"},
                )
            )
        ]

    impact_table = dbc.Table(
        [
            html.Thead(
                html.Tr(
                    [
                        html.Th("Событие", style=_th_style),
                        html.Th("Тип", style={**_th_style, "textAlign": "center"}),
                        html.Th("Δ до старта", style={**_th_style, "textAlign": "right"}),
                        html.Th("Δ после финала", style={**_th_style, "textAlign": "right"}),
                        html.Th("Δ окно ±30д", style={**_th_style, "textAlign": "right"}),
                    ]
                )
            ),
            html.Tbody(impact_rows),
        ],
        bordered=False,
        hover=True,
        responsive=True,
        style={"backgroundColor": _BG, "fontSize": "12px", "marginTop": "12px"},
    )

    table_title = html.Div(
        "ИСТОРИЯ ИВЕНТОВ",
        style={
            "color": _MUTED,
            "fontSize": "10px",
            "letterSpacing": "1.5px",
            "marginTop": "12px",
            "marginBottom": "6px",
        },
    )

    return dbc.Card(
        dbc.CardBody([card_title, *fig_children, table_title, impact_table]),
        style={"backgroundColor": _BG2, "border": f"1px solid {_BORDER}", "marginTop": "16px"},
    )


def _render_analytics(selected_container_id: Any = None) -> html.Div:
    """Analytics tab — correlation heatmap for all tracked containers."""
    containers = _get_containers()
    id_to_name = {str(c.container_id): str(c.container_name) for c in containers}

    # Bulk-fetch steam_market history for all containers (1 query instead of N)
    cids = [str(c.container_id) for c in containers]
    price_history = _get_all_price_histories(cids, source="steam_market")

    result = compute_correlation_matrix(price_history, id_to_name)
    names = result["names"]
    matrix = result["matrix"]
    pairs = result["pairs"]

    if not names:
        return html.Div(
            [
                html.P(
                    "Нет данных для корреляций. Запусти cs2 backfill чтобы загрузить "
                    "историю Steam Market (нужно минимум 10 дней на контейнер).",
                    style={"color": _MUTED, "textAlign": "center", "paddingTop": "40px"},
                )
            ]
        )

    # Count pairs by risk level for summary line
    high_corr_count = sum(1 for _, _, r in pairs if abs(r) >= 0.70)
    medium_corr_count = sum(1 for _, _, r in pairs if 0.40 <= abs(r) < 0.70)

    # Top correlated pairs — primary view
    _th = {
        "color": _MUTED,
        "fontSize": "11px",
        "backgroundColor": _BG2,
        "border": "none",
        "paddingBottom": "6px",
    }
    pair_rows = []
    for n1, n2, r in pairs[:10]:
        corr_color = _RED if abs(r) >= 0.70 else _YELLOW if abs(r) >= 0.40 else _GREEN
        pair_rows.append(
            html.Tr(
                [
                    html.Td(n1, style={"color": _TEXT, "fontSize": "12px"}),
                    html.Td(n2, style={"color": _TEXT, "fontSize": "12px"}),
                    html.Td(
                        f"{r:+.3f}",
                        style={
                            "color": corr_color,
                            "fontWeight": "bold",
                            "textAlign": "right",
                            "fontSize": "13px",
                        },
                    ),
                    html.Td(
                        "Риск — коррелированы"
                        if abs(r) >= 0.70
                        else "Умеренная связь"
                        if abs(r) >= 0.40
                        else "Низкая (диверсиф.)",
                        style={"color": corr_color, "fontSize": "11px", "textAlign": "center"},
                    ),
                ],
                style={"borderBottom": f"1px solid {_BG2}"},
            )
        )

    summary_text = (
        f"{high_corr_count} пар с высокой корреляцией (r > 0.70) — концентрированный риск."
        if high_corr_count
        else f"Нет высококоррелированных пар. {medium_corr_count} пар со средней корреляцией."
    )

    pairs_card = dbc.Card(
        dbc.CardBody(
            [
                html.Div(
                    "КОРРЕЛЯЦИЯ МЕЖДУ КОНТЕЙНЕРАМИ",
                    style={
                        "color": _MUTED,
                        "fontSize": "10px",
                        "letterSpacing": "1.5px",
                        "marginBottom": "8px",
                    },
                ),
                dbc.Alert(
                    summary_text,
                    color="danger" if high_corr_count else "secondary",
                    style={"fontSize": "12px", "marginBottom": "10px", "padding": "8px 12px"},
                ),
                html.Div(
                    "ТОП-10 НАИБОЛЕЕ КОРРЕЛИРОВАННЫХ ПАР",
                    style={"color": _MUTED, "fontSize": "10px", "marginBottom": "6px"},
                ),
                dbc.Table(
                    [
                        html.Thead(
                            html.Tr(
                                [
                                    html.Th("Контейнер A", style=_th),
                                    html.Th("Контейнер B", style=_th),
                                    html.Th("r", style={**_th, "textAlign": "right"}),
                                    html.Th("Интерпретация", style={**_th, "textAlign": "center"}),
                                ]
                            )
                        ),
                        html.Tbody(
                            pair_rows
                            or [
                                html.Tr(
                                    [
                                        html.Td(
                                            "Нет значимых корреляций.",
                                            colSpan=4,
                                            style={
                                                "color": _MUTED,
                                                "textAlign": "center",
                                                "padding": "16px",
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
                ),
                dbc.Alert(
                    "r > 0.70 — сильная корреляция: при падении одного актива второй тоже падает. "
                    "r 0.40–0.70 — умеренная. r < 0.40 — диверсификация работает. "
                    "Portfolio Advisor предупреждает если флип и инвестиция из одной корреляционной группы.",
                    color="secondary",
                    style={"fontSize": "11px", "marginTop": "10px"},
                ),
            ]
        ),
        style={"backgroundColor": _BG2, "border": f"1px solid {_BORDER}", "marginBottom": "16px"},
    )

    # Full correlation matrix — secondary, shown after pairs table
    z = [[v if v is not None else 0.0 for v in row] for row in matrix]

    fig = go.Figure(
        go.Heatmap(
            z=z,
            x=names,
            y=names,
            colorscale=[
                [0.0, _RED],  # -1  red
                [0.5, _HEATMAP_NEU],  # 0   neutral (yellow-tinted, distinct from bg)
                [1.0, _GREEN],  # +1  green
            ],
            zmin=-1,
            zmax=1,
            colorbar=dict(
                title="r",
                tickvals=[-1, -0.5, 0, 0.5, 1],
                ticktext=["-1", "-0.5", "0", "+0.5", "+1"],
                tickfont=dict(color=_MUTED),
            ),
            hoverongaps=False,
            hovertemplate="%{y} × %{x}<br>r = %{z:.3f}<extra></extra>",
        )
    )
    fig.update_layout(
        paper_bgcolor=_BG,
        plot_bgcolor=_BG2,
        font=dict(color=_TEXT, size=9),
        xaxis=dict(tickangle=-45, tickfont=dict(size=8), color=_MUTED),
        yaxis=dict(tickfont=dict(size=8), color=_MUTED),
        margin=dict(l=140, r=40, t=20, b=140),
        height=max(480, len(names) * 14),
    )

    heatmap_card = dbc.Card(
        dbc.CardBody(
            [
                html.Details(
                    [
                        html.Summary(
                            f"Показать полную матрицу ({len(names)} × {len(names)})",
                            style={
                                "color": _MUTED,
                                "fontSize": "11px",
                                "cursor": "pointer",
                                "padding": "4px 0",
                                "userSelect": "none",
                            },
                        ),
                        html.Div(
                            [
                                html.Div(
                                    f"{len(names)} контейнеров · Pearson r · log-returns",
                                    style={
                                        "color": _MUTED,
                                        "fontSize": "11px",
                                        "marginBottom": "10px",
                                        "marginTop": "8px",
                                    },
                                ),
                                dcc.Graph(figure=fig, config={"displayModeBar": False}),
                            ]
                        ),
                    ]
                ),
            ]
        ),
        style={"backgroundColor": _BG2, "border": f"1px solid {_BORDER}", "marginBottom": "16px"},
    )

    # ── T-07: Stale calendar warning ─────────────────────────────────────────
    calendar_stale_alert = None
    if is_calendar_stale():
        calendar_stale_alert = html.Div(
            "Календарь ивентов устарел — обновите даты турниров в `engine/event_calendar.py`",
            style={
                "backgroundColor": _BG_WARN,
                "border": f"1px solid {_ORANGE}",
                "color": _ORANGE,
                "borderRadius": "4px",
                "padding": "8px 14px",
                "fontSize": "12px",
                "marginBottom": "12px",
            },
        )

    # ── Event Calendar ────────────────────────────────────────────────────────
    all_names = [str(c.container_name) for c in containers]
    ev_signals = get_event_signals(all_names)
    ev_upcoming = get_upcoming_events(lookahead_days=60)

    _EV_BADGE = {"major": "danger", "premier": "warning", "iem": "info", "esl": "primary"}

    ev_upcoming_els = []
    for ev in ev_upcoming:
        badge_color = _EV_BADGE.get(ev["type"], "secondary")
        days_label = f"через {ev['days_to_start']} дн." if ev["days_to_start"] > 0 else "▶ LIVE"
        ev_upcoming_els.append(
            html.Div(
                [
                    dbc.Badge(
                        ev["type"].upper(),
                        color=badge_color,
                        style={"fontSize": "9px", "marginRight": "8px"},
                    ),
                    html.Span(
                        ev["name"],
                        style={
                            "color": _TEXT,
                            "fontSize": "12px",
                            "fontWeight": "bold",
                            "marginRight": "8px",
                        },
                    ),
                    html.Span(
                        f"{ev['start'].strftime('%d %b')} – {ev['end'].strftime('%d %b')}",
                        style={"color": _MUTED, "fontSize": "11px", "marginRight": "8px"},
                    ),
                    html.Span(
                        days_label,
                        style={
                            "color": _GOLD if ev["days_to_start"] == 0 else _BLUE,
                            "fontSize": "11px",
                            "fontWeight": "bold",
                        },
                    ),
                ],
                style={"marginBottom": "6px"},
            )
        )

    ev_signal_rows = []
    for name, info in sorted(ev_signals.items(), key=lambda x: x[1]["signal"]):
        sig = info["signal"]
        ev_signal_rows.append(
            html.Tr(
                [
                    html.Td(
                        dbc.Badge(
                            sig,
                            color={"BUY": "success", "SELL": "danger", "HOLD": "warning"}.get(
                                sig, "secondary"
                            ),
                            className="px-2",
                        )
                    ),
                    html.Td(
                        html.Span(name, style={"color": _TEXT, "fontSize": "12px"}),
                        style={
                            "maxWidth": "220px",
                            "overflow": "hidden",
                            "textOverflow": "ellipsis",
                            "whiteSpace": "nowrap",
                        },
                    ),
                    html.Td(
                        html.Span(info["message"], style={"color": _MUTED, "fontSize": "10px"})
                    ),
                ],
                style={"borderBottom": f"1px solid {_BG2}"},
            )
        )

    _ev_th = {
        "color": _MUTED,
        "fontSize": "11px",
        "backgroundColor": _BG2,
        "border": "none",
        "paddingBottom": "6px",
    }

    calendar_card = dbc.Card(
        dbc.CardBody(
            [
                html.Div(
                    "СОБЫТИЙНЫЙ ТРЕЙДИНГ — CALENDAR",
                    style={
                        "color": _MUTED,
                        "fontSize": "10px",
                        "letterSpacing": "1.5px",
                        "marginBottom": "12px",
                    },
                ),
                html.Div(
                    "БЛИЖАЙШИЕ СОБЫТИЯ (60 дней):",
                    style={"color": _MUTED, "fontSize": "10px", "marginBottom": "8px"},
                ),
                html.Div(
                    ev_upcoming_els
                    or [
                        html.Span(
                            "Нет мероприятий в ближайшие 60 дней.",
                            style={"color": _MUTED, "fontSize": "12px"},
                        )
                    ],
                    style={"marginBottom": "16px"},
                ),
                html.Div(
                    "АКТИВНЫЕ ТОРГОВЫЕ СИГНАЛЫ:",
                    style={"color": _MUTED, "fontSize": "10px", "marginBottom": "8px"},
                ),
                dbc.Table(
                    [
                        html.Thead(
                            html.Tr(
                                [
                                    html.Th("Сигнал", style=_ev_th),
                                    html.Th("Контейнер", style=_ev_th),
                                    html.Th("Причина", style=_ev_th),
                                ]
                            )
                        ),
                        html.Tbody(
                            ev_signal_rows
                            or [
                                html.Tr(
                                    [
                                        html.Td(
                                            "Нет активных сигналов — нет мероприятий в активном окне.",
                                            colSpan=3,
                                            style={
                                                "color": _MUTED,
                                                "textAlign": "center",
                                                "padding": "12px",
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
                ),
                dbc.Alert(
                    "BUY за 30 дней до начала мажора. "
                    "HOLD пока идёт (пиковый спрос — не покупать). "
                    "SELL в течение 7 дней после финала если цена выросла. "
                    "Добавь новые турниры в engine/event_calendar.py → EVENTS.",
                    color="secondary",
                    style={"fontSize": "11px", "marginTop": "10px"},
                ),
            ]
        ),
        style={"backgroundColor": _BG2, "border": f"1px solid {_BORDER}", "marginTop": "16px"},
    )

    # ── F-06: Event impact chart ─────────────────────────────────────────────
    event_impact_card = _render_event_impact_card(selected_container_id, containers)

    analytics_children = [pairs_card, heatmap_card]
    if calendar_stale_alert is not None:
        analytics_children.append(calendar_stale_alert)
    analytics_children.append(calendar_card)
    analytics_children.append(event_impact_card)
    return html.Div(analytics_children)
