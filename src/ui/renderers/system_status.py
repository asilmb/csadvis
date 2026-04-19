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


def _ping_label(last_ping: dict | None) -> list:
    """Format last ping result as inline Dash children."""
    if not last_ping or not last_ping.get("status"):
        return []
    status = last_ping["status"]
    pinged_at = last_ping.get("pinged_at", "")
    if status == "ok":
        return [html.Span(f"✓ {pinged_at}", style={"color": _GREEN, "fontSize": "11px"})]
    if status == "blocked":
        blocked_until = last_ping.get("blocked_until", "?")
        remaining_s = last_ping.get("remaining_s", 0)
        if remaining_s >= 3600:
            r_str = f"{remaining_s // 3600}ч {(remaining_s % 3600) // 60}м"
        elif remaining_s >= 60:
            r_str = f"{remaining_s // 60} мин"
        else:
            r_str = f"{remaining_s} сек"
        return [
            html.Span(f"⛔ до {blocked_until}", style={"color": _RED, "fontSize": "11px"}),
            html.Span(f"  ({r_str})", style={"color": _MUTED, "fontSize": "10px"}),
            html.Br(),
            html.Span(pinged_at, style={"color": _MUTED, "fontSize": "10px"}),
        ]
    if status == "no_credentials":
        return [html.Span(f"— нет токена  {pinged_at}", style={"color": _MUTED, "fontSize": "11px"})]
    return [html.Span(f"⚠ {pinged_at}", style={"color": _ORANGE, "fontSize": "11px"})]


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
    """Inline progress indicator — spinner + label + queue list when busy."""
    if not worker.get("busy") and worker.get("queue_size", 0) == 0:
        return []
    job_type = worker.get("current_type") or "ожидание..."
    prog_cur = worker.get("progress_current", 0)
    prog_tot = worker.get("progress_total", 0)
    eta_s = worker.get("eta_seconds")
    if prog_tot > 0:
        if eta_s and eta_s > 0:
            if eta_s >= 3600:
                eta_str = f"{eta_s // 3600}ч {(eta_s % 3600) // 60}мин"
            elif eta_s >= 60:
                eta_str = f"~{eta_s // 60} мин"
            else:
                eta_str = f"~{eta_s} сек"
            job_label = f"{job_type}  {prog_cur} / {prog_tot}  ({eta_str})"
        else:
            job_label = f"{job_type}  {prog_cur} / {prog_tot}"
    else:
        job_label = job_type
    last_name = worker.get("last_item_name", "")
    last_price = worker.get("last_item_price", 0.0)
    last_volume = worker.get("last_item_volume", 0)
    queue_items = worker.get("queue_items", [])

    queue_rows = []
    if queue_items:
        queue_rows = [
            html.Div(
                [
                    html.Span(f"{i + 1}.", style={"color": _MUTED, "fontSize": "11px", "width": "18px", "flexShrink": "0"}),
                    html.Span(jt, style={"color": _TEXT, "fontSize": "11px"}),
                ],
                style={"display": "flex", "gap": "4px"},
            )
            for i, jt in enumerate(queue_items)
        ]

    last_row = []
    if last_name:
        last_row = [html.Div(
            [
                html.Span("Последний: ", style={"color": _MUTED, "fontSize": "11px"}),
                html.Span(last_name, style={"color": _TEXT, "fontSize": "11px", "fontWeight": "600"}),
                html.Span(f"  {last_price:,.0f}₸", style={"color": _GOLD, "fontSize": "11px", "marginLeft": "6px"}),
                html.Span(f"  vol {last_volume:,}", style={"color": _MUTED, "fontSize": "11px", "marginLeft": "6px"}),
            ],
            style={"marginTop": "4px"},
        )]

    return [
        html.Div(
            [
                html.Div(
                    [
                        dbc.Spinner(size="sm", color="primary", spinner_style={"marginRight": "8px"}),
                        html.Span(job_label, style={"color": _TEXT, "fontSize": "12px"}),
                    ],
                    style={"display": "flex", "alignItems": "center", "marginBottom": "4px"},
                ),
                *last_row,
                *([html.Div(
                    [html.Div("В очереди:", style={**_LABEL, "marginBottom": "4px"})] + queue_rows,
                    style={"borderTop": f"1px solid {_BORDER}", "paddingTop": "6px", "marginTop": "6px"},
                )] if queue_rows else []),
            ],
            style={
                "padding": "10px 12px",
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

    def _group_label(text: str) -> html.Div:
        return html.Div(text, style={**_LABEL, "marginBottom": "4px"})

    def _btn_group(*buttons) -> html.Div:
        return html.Div(list(buttons), style={"display": "flex", "gap": "6px", "flexWrap": "wrap"})

    last_ping = getattr(health, "last_ping", None)

    # ── Action buttons ────────────────────────────────────────────────────────
    btn_row = html.Div([
        dbc.Row([
            dbc.Col(html.Div([
                _group_label("Steam"),
                _btn_group(
                    dbc.Button("Инвентарь", id="btn-sync-inventory", color="info", outline=True, size="sm", n_clicks=0),
                    dbc.Button("Каталог", id="btn-sync-catalog", color="info", outline=True, size="sm", n_clicks=0),
                ),
            ]), width="auto"),
            dbc.Col(html.Div([
                _group_label("Текущие цены"),
                _btn_group(
                    dbc.Button("Sync Prices", id="btn-sync-prices", color="info", outline=True, size="sm", n_clicks=0),
                ),
            ]), width="auto"),
            dbc.Col(html.Div([
                _group_label("История цен"),
                _btn_group(
                    dbc.Button("Backfill Active", id="btn-backfill-active", color="warning", outline=True, size="sm", n_clicks=0),
                    dbc.Button("Backfill All", id="btn-backfill-all", color="warning", outline=True, size="sm", n_clicks=0),
                ),
            ]), width="auto"),
            dbc.Col(html.Div([
                _group_label("Очередь"),
                _btn_group(
                    dbc.Button([html.I(className="fa fa-trash me-1"), "Очистить"], id="btn-clear-queue", color="danger", outline=True, size="sm", n_clicks=0),
                ),
            ]), width="auto"),
            dbc.Col(html.Div([
                _group_label("Диагностика"),
                _btn_group(
                    dbc.Button("Ping Steam", id="btn-ping-steam", color="secondary", outline=True, size="sm", n_clicks=0),
                ),
                html.Div(id="ping-steam-last", children=_ping_label(last_ping), style={"marginTop": "4px"}),
            ]), width="auto"),
            dbc.Col(html.Span(id="health-action-msg", style={"color": _MUTED, "fontSize": "12px", "paddingTop": "18px", "display": "block"}), width="auto"),
        ], className="g-3 align-items-start", style={"marginBottom": "20px"}),
        dbc.Tooltip("Загружает актуальный инвентарь Steam", target="btn-sync-inventory", placement="bottom"),
        dbc.Tooltip("Обновляет список контейнеров с рынка Steam", target="btn-sync-catalog", placement="bottom"),
        dbc.Tooltip("Получает текущие цены на все контейнеры", target="btn-sync-prices", placement="bottom"),
        dbc.Tooltip("Загружает историю цен только для контейнеров с открытыми позициями", target="btn-backfill-active", placement="bottom"),
        dbc.Tooltip("Загружает историю цен для всех контейнеров (~60–110 мин)", target="btn-backfill-all", placement="bottom"),
        dbc.Tooltip("Очищает очередь задач воркера", target="btn-clear-queue", placement="bottom"),
        dbc.Tooltip("Проверяет токен и наличие блокировки Steam", target="btn-ping-steam", placement="bottom"),
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
        html.Div([
            _btn_group(
                dbc.Button("История скрытых", id="btn-bl-backfill", color="warning", outline=True, size="sm", n_clicks=0),
                dbc.Button("Цены скрытых", id="btn-bl-prices", color="info", outline=True, size="sm", n_clicks=0),
                html.Span(id="bl-scan-msg", style={"color": _MUTED, "fontSize": "11px", "alignSelf": "center"}),
            ),
        ], style={"marginBottom": "10px"}),
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

    # ── Cooldown banner ───────────────────────────────────────────────────────
    cooldown_banner = html.Div()
    if health.cooldown_until:
        cooldown_banner = html.Div(
            [html.I(className="fa fa-ban me-2"), f"Steam заблокирован до {health.cooldown_until} UTC"],
            style={
                "backgroundColor": "#3a1a1a",
                "border": f"1px solid {_RED}",
                "borderRadius": "6px",
                "color": _RED,
                "fontSize": "12px",
                "padding": "10px 14px",
                "marginBottom": "16px",
            },
        )

    # ── Saved scrape sessions ─────────────────────────────────────────────────
    def _session_row(s: dict) -> html.Tr:
        pct = int(s["processed_count"] / s["total_count"] * 100) if s["total_count"] else 0
        label = "Цены" if s["job_type"] == "price_poll" else "История"
        return html.Tr([
            html.Td(label, style={"color": _TEXT, "fontSize": "12px"}),
            html.Td(
                f"{s['processed_count']} / {s['total_count']} ({pct}%)",
                style={"color": _MUTED, "fontSize": "12px"},
            ),
            html.Td(s["updated_at"], style={"color": _MUTED, "fontSize": "11px"}),
            html.Td(html.Div([
                dbc.Button("Продолжить", id={"type": "btn-session-resume", "index": s["id"]},
                           color="success", outline=True, size="sm", n_clicks=0,
                           style={"fontSize": "11px", "marginRight": "4px"}),
                dbc.Button("Удалить", id={"type": "btn-session-delete", "index": s["id"]},
                           color="danger", outline=True, size="sm", n_clicks=0,
                           style={"fontSize": "11px"}),
            ])),
        ])

    sessions_section = html.Div([
        html.H6("Сохранённые сессии", style={"color": _MUTED, "marginBottom": "8px"}),
        html.Div(
            dbc.Table(
                [
                    html.Thead(html.Tr([
                        html.Th("Тип", style={"color": _MUTED}),
                        html.Th("Прогресс", style={"color": _MUTED}),
                        html.Th("Обновлено", style={"color": _MUTED}),
                        html.Th("", style={"color": _MUTED}),
                    ])),
                    html.Tbody(
                        [_session_row(s) for s in health.scrape_sessions]
                        if health.scrape_sessions else [
                            html.Tr(html.Td(
                                "Нет сохранённых сессий",
                                colSpan=4,
                                style={"color": _MUTED, "textAlign": "center", "fontSize": "12px"},
                            ))
                        ]
                    ),
                ],
                bordered=False, size="sm",
            ),
            style={**_CARD, "marginBottom": "8px"},
        ),
        html.Div(id="session-action-msg", style={"color": _MUTED, "fontSize": "12px", "marginBottom": "16px"}),
    ])

    return html.Div([
        html.H6("Liveness", style={"color": _MUTED, "marginBottom": "12px"}),
        liveness,
        cooldown_banner,
        progress_section,
        refresh_btn,
        html.H6("Actions", style={"color": _MUTED, "marginBottom": "8px"}),
        btn_row,
        sessions_section,
        blacklist_section,
        footer,
    ])
