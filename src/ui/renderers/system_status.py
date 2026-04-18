"""
System Status tab renderer.

Displays live health metrics:
  - Liveness: cookie status, worker state
  - Blacklisted containers with unblock button
  - Action buttons (trigger syncs via API)
"""

from __future__ import annotations

import dash_bootstrap_components as dbc
from dash import dcc, html

from ui.helpers import _BG2, _BORDER, _GREEN, _MUTED, _RED, _TEXT

_GOLD = "#e8c84a"
_ORANGE = "#e07b39"

_CARD = {"backgroundColor": _BG2, "border": f"1px solid {_BORDER}", "borderRadius": "6px", "padding": "16px"}
_LABEL = {"color": _MUTED, "fontSize": "11px", "textTransform": "uppercase", "letterSpacing": "0.05em", "marginBottom": "4px"}
_VALUE = {"color": _TEXT, "fontSize": "20px", "fontWeight": "600"}


def _badge(text: str, color: str) -> html.Span:
    return html.Span(
        text,
        style={
            "backgroundColor": color,
            "color": "#000",
            "borderRadius": "4px",
            "padding": "2px 8px",
            "fontSize": "11px",
            "fontWeight": "700",
        },
    )


def _render_progress(worker: dict) -> list:
    """Inline progress indicator — spinner + label when busy, empty when idle."""
    if not worker.get("busy") and worker.get("queue_size", 0) == 0:
        return []
    job_label = worker.get("current_type") or "ожидание..."
    queue_size = worker.get("queue_size", 0)
    extra = f" (+{queue_size} в очереди)" if queue_size > 0 else ""
    return [
        html.Div(
            [
                dbc.Spinner(size="sm", color="primary", style={"marginRight": "8px"}),
                html.Span(
                    f"{job_label}{extra}",
                    style={"color": _TEXT, "fontSize": "12px"},
                ),
            ],
            style={
                "display": "flex",
                "alignItems": "center",
                "padding": "8px 12px",
                "backgroundColor": _BG2,
                "border": f"1px solid {_BORDER}",
                "borderRadius": "6px",
            },
        )
    ]


def render_system_status(health=None) -> html.Div:
    if health is None:
        return html.Div(
            dbc.Spinner(color="primary", size="lg"),
            style={"textAlign": "center", "paddingTop": "80px"},
        )

    # ── Liveness block ────────────────────────────────────────────────────────
    cookie_color = _GREEN if health.cookie_set else _RED
    cookie_text = "Valid" if health.cookie_set else "Not set"

    worker = getattr(health, "worker", {})
    if worker.get("busy"):
        wk_text = f"BUSY — {worker.get('current_type', '?')}"
        wk_color = _GOLD
    elif worker.get("restarts", 0) > 0 and worker.get("last_error"):
        wk_text = f"RESTARTED ({worker['restarts']}x)"
        wk_color = _ORANGE
    else:
        wk_text = f"IDLE  (queue: {worker.get('queue_size', 0)})"
        wk_color = _GREEN

    liveness = dbc.Row(
        [
            dbc.Col(
                html.Div([
                    html.Div("Steam Cookie", style=_LABEL),
                    _badge(cookie_text, cookie_color),
                ], style=_CARD),
                width=6,
            ),
            dbc.Col(
                html.Div([
                    html.Div("Worker", style=_LABEL),
                    html.Span(wk_text, style={**_VALUE, "color": wk_color, "fontSize": "14px"}),
                ], style=_CARD),
                width=6,
            ),
        ],
        className="g-2",
        style={"marginBottom": "20px"},
    )

    if worker.get("last_error"):
        liveness = html.Div([
            liveness,
            html.Div(
                f"Last error: {worker['last_error']}",
                style={"color": _RED, "fontSize": "11px", "marginBottom": "12px"},
            ),
        ])

    # ── Action buttons ────────────────────────────────────────────────────────
    btn_row = html.Div([
        dbc.Row([
            dbc.Col(dbc.Button(
                [html.I(className="fa fa-refresh me-1"), "Force Global Sync"],
                id="btn-force-sync",
                color="primary", size="sm", n_clicks=0,
            ), width="auto"),
            dbc.Col(dbc.Button(
                "Обновить контейнеры",
                id="btn-update-containers",
                color="success", outline=True, size="sm", n_clicks=0,
            ), width="auto"),
            dbc.Col(dbc.Button(
                "Sync Inventory",
                id="btn-sync-inventory",
                color="info", outline=True, size="sm", n_clicks=0,
            ), width="auto"),
            dbc.Col(dbc.Button(
                "Sync Catalog",
                id="btn-sync-catalog",
                color="info", outline=True, size="sm", n_clicks=0,
            ), width="auto"),
            dbc.Col(dbc.Button(
                "Sync Prices",
                id="btn-sync-prices",
                color="info", outline=True, size="sm", n_clicks=0,
            ), width="auto"),
            dbc.Col(
                html.Span(
                    id="health-action-msg",
                    style={"color": _MUTED, "fontSize": "12px", "paddingTop": "4px"},
                ),
                width="auto",
            ),
        ], className="g-2 align-items-center", style={"marginBottom": "20px"}),
        dbc.Tooltip("Синхронизирует инвентарь и цены", target="btn-force-sync", placement="bottom"),
        dbc.Tooltip("Сканирует БД и ставит в очередь обновление цен для всех активных контейнеров", target="btn-update-containers", placement="bottom"),
        dbc.Tooltip("Загружает актуальный инвентарь Steam", target="btn-sync-inventory", placement="bottom"),
        dbc.Tooltip("Обновляет список контейнеров с рынка Steam", target="btn-sync-catalog", placement="bottom"),
        dbc.Tooltip("Получает текущие цены на все контейнеры", target="btn-sync-prices", placement="bottom"),
    ])

    # ── Blacklisted containers ─────────────────────────────────────────────────
    def _blacklist_row(item: dict) -> html.Tr:
        return html.Tr([
            html.Td(item["container_name"], style={"color": _TEXT, "fontSize": "12px"}),
            html.Td(
                dbc.Button(
                    "Разблокировать",
                    id={"type": "btn-unblacklist", "index": item["container_id"]},
                    color="success", outline=True, size="sm", n_clicks=0,
                    style={"fontSize": "11px"},
                ),
            ),
        ])

    blacklist_section = html.Div([
        html.H6("Скрытые контейнеры", style={"color": _MUTED, "marginBottom": "8px"}),
        html.Div(
            dbc.Table(
                [
                    html.Thead(html.Tr([
                        html.Th("Контейнер", style={"color": _MUTED}),
                        html.Th("Действие", style={"color": _MUTED}),
                    ])),
                    html.Tbody(
                        [_blacklist_row(item) for item in health.blacklisted_containers]
                        if health.blacklisted_containers else [
                            html.Tr(html.Td(
                                "Нет скрытых контейнеров",
                                colSpan=2,
                                style={"color": _MUTED, "textAlign": "center", "fontSize": "12px"},
                            ))
                        ]
                    ),
                ],
                bordered=False, size="sm",
            ),
            style={**_CARD, "marginBottom": "20px"},
        ),
        html.Div(id="blacklist-action-msg", style={"color": _MUTED, "fontSize": "12px", "marginBottom": "16px"}),
    ])

    # ── Live progress bar (shown while worker is busy) ────────────────────────
    progress_section = html.Div(
        id="worker-progress-section",
        children=_render_progress(worker),
        style={"marginBottom": "16px"},
    )

    # ── Refresh button ─────────────────────────────────────────────────────────
    refresh_btn = html.Div([
        dbc.Button(
            [html.I(className="fa fa-refresh me-1"), "Обновить"],
            id="btn-refresh-system",
            size="sm", color="secondary", outline=True,
            n_clicks=0,
            style={"fontSize": "11px", "padding": "1px 8px"},
        ),
        # Polls worker state every 3 s while visible; disabled when idle
        dcc.Interval(
            id="worker-progress-interval",
            interval=3_000,
            n_intervals=0,
            disabled=not worker.get("busy") and worker.get("queue_size", 0) == 0,
        ),
    ], style={"display": "flex", "gap": "10px", "alignItems": "center", "marginBottom": "16px"})

    footer = html.Div(
        f"Last refresh: {health.timestamp}",
        style={"color": _MUTED, "fontSize": "11px", "marginTop": "8px"},
    )

    return html.Div([
        html.H6("Liveness", style={"color": _MUTED, "marginBottom": "12px"}),
        liveness,
        progress_section,
        refresh_btn,
        html.H6("Actions", style={"color": _MUTED, "marginBottom": "8px"}),
        btn_row,
        blacklist_section,
        footer,
    ])
