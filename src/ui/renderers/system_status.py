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


_PHASE_META: dict[str, tuple[str, str, int]] = {
    # phase → (label, color, warn_after_seconds)
    "requesting":     ("⟳ Запрос Steam",      _ORANGE, 40),
    "received":       ("✓ Ответ получен",      _GREEN,  10),
    "delay":          ("⏳ Ожидание",           _MUTED,  60),
    "session_break":  ("☕ Перерыв сессии",     _MUTED,  90),
    "saving":         ("💾 Запись в БД",        _GREEN,  15),
}


def _render_progress(worker: dict) -> list:
    """Rich phase-aware progress block — shows phase, staleness, progress bar, queue."""
    if not worker.get("busy") and worker.get("queue_size", 0) == 0:
        return []

    job_type = worker.get("current_type") or "ожидание..."
    prog_cur = worker.get("progress_current", 0)
    prog_tot = worker.get("progress_total", 0)
    eta_s = worker.get("eta_seconds")
    phase = worker.get("phase", "")
    secs = worker.get("seconds_in_phase", 0)
    current_item = worker.get("current_item_name", "")
    last_name = worker.get("last_item_name", "")
    last_price = worker.get("last_item_price", 0.0)
    last_volume = worker.get("last_item_volume", 0)
    last_job_detail = worker.get("last_job_detail", "")
    queue_items = worker.get("queue_items", [])

    # ── Phase badge ───────────────────────────────────────────────────────────
    phase_label, phase_color, warn_after = _PHASE_META.get(phase, ("● Работает", _MUTED, 120))
    if secs >= warn_after and warn_after > 0:
        phase_color = _RED
        phase_label += " ⚠"
    phase_badge = html.Span(
        [
            html.Span(phase_label, style={"fontWeight": "600"}),
            html.Span(f" {secs}с", style={"opacity": "0.7", "fontSize": "10px"}),
        ],
        style={
            "backgroundColor": f"{phase_color}22",
            "border": f"1px solid {phase_color}",
            "borderRadius": "4px",
            "color": phase_color,
            "fontSize": "11px",
            "padding": "1px 8px",
            "display": "inline-block",
        },
    )

    # ── ETA string ────────────────────────────────────────────────────────────
    if eta_s and eta_s > 0:
        if eta_s >= 3600:
            eta_str = f"{eta_s // 3600}ч {(eta_s % 3600) // 60}мин"
        elif eta_s >= 60:
            eta_str = f"~{eta_s // 60} мин"
        else:
            eta_str = f"~{eta_s} сек"
    else:
        eta_str = ""

    # ── Header row ────────────────────────────────────────────────────────────
    header_parts: list = [
        dbc.Spinner(size="sm", color="primary", spinner_style={"marginRight": "8px", "flexShrink": "0"}),
        html.Span(job_type, style={"color": _TEXT, "fontSize": "12px", "fontWeight": "600", "marginRight": "8px"}),
        phase_badge,
    ]
    if prog_tot > 0:
        header_parts.append(
            html.Span(
                f"{prog_cur} / {prog_tot}",
                style={"color": _MUTED, "fontSize": "11px", "marginLeft": "10px"},
            )
        )
    if eta_str:
        header_parts.append(
            html.Span(eta_str, style={"color": _GOLD, "fontSize": "11px", "marginLeft": "6px"})
        )

    # ── Progress bar ──────────────────────────────────────────────────────────
    progress_bar = []
    if prog_tot > 0:
        pct = min(100, int(prog_cur / prog_tot * 100))
        progress_bar = [
            html.Div(
                html.Div(
                    style={
                        "width": f"{pct}%",
                        "height": "100%",
                        "backgroundColor": _ORANGE,
                        "borderRadius": "2px",
                        "transition": "width 0.4s ease",
                    }
                ),
                style={
                    "width": "100%",
                    "height": "4px",
                    "backgroundColor": f"{_BORDER}",
                    "borderRadius": "2px",
                    "marginTop": "6px",
                },
            )
        ]

    # ── Current item (being fetched right now) ────────────────────────────────
    current_row = []
    if current_item:
        current_row = [html.Div(
            [
                html.Span("Сейчас: ", style={"color": _MUTED, "fontSize": "11px"}),
                html.Span(
                    current_item,
                    style={"color": _TEXT, "fontSize": "11px", "fontStyle": "italic"},
                ),
            ],
            style={"marginTop": "5px"},
        )]

    # ── Last completed item / job result ─────────────────────────────────────
    last_row = []
    if job_type in ("market_catalog", "sync_inventory"):
        # Single-shot jobs: show result summary instead of per-item detail
        if last_job_detail:
            last_row = [html.Div(
                [
                    html.Span("Результат: ", style={"color": _MUTED, "fontSize": "11px"}),
                    html.Span(last_job_detail, style={"color": _TEXT, "fontSize": "11px"}),
                ],
                style={"marginTop": "4px"},
            )]
    elif last_name:
        if job_type == "backfill_history":
            detail_spans = [
                html.Span(
                    f"  +{last_volume} записей" if last_volume else "  нет новых",
                    style={"color": _GOLD if last_volume else _MUTED, "fontSize": "11px", "marginLeft": "6px"},
                ),
            ]
        else:  # price_poll
            detail_spans = [
                html.Span(f"  {last_price:,.0f}₸", style={"color": _GOLD, "fontSize": "11px", "marginLeft": "6px"}),
                html.Span(f"  vol {last_volume:,}", style={"color": _MUTED, "fontSize": "11px", "marginLeft": "6px"}),
            ]
        last_row = [html.Div(
            [
                html.Span("Последний: ", style={"color": _MUTED, "fontSize": "11px"}),
                html.Span(last_name, style={"color": _TEXT, "fontSize": "11px", "fontWeight": "600"}),
                *detail_spans,
            ],
            style={"marginTop": "4px"},
        )]

    # ── Queue ─────────────────────────────────────────────────────────────────
    queue_section = []
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
        queue_section = [html.Div(
            [html.Div("В очереди:", style={**_LABEL, "marginBottom": "4px"})] + queue_rows,
            style={"borderTop": f"1px solid {_BORDER}", "paddingTop": "6px", "marginTop": "6px"},
        )]

    return [
        html.Div(
            [
                html.Div(header_parts, style={"display": "flex", "alignItems": "center", "flexWrap": "wrap", "gap": "2px"}),
                *progress_bar,
                *current_row,
                *last_row,
                *queue_section,
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
    _cs = getattr(health, "cookie_status", "VALID" if health.cookie_set else "NOT_SET")
    cookie_color = _GREEN if _cs == "VALID" else (_RED if _cs in ("EXPIRED", "NOT_SET") else _ORANGE)
    cookie_text = {"VALID": "Valid", "EXPIRED": "Expired", "NOT_SET": "Not set"}.get(_cs, _cs)

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
                    dbc.Button("Missing Volume", id="btn-missing-volume", color="info", outline=True, size="sm", n_clicks=0),
                ),
            ]), width="auto"),
            dbc.Col(html.Div([
                _group_label("История цен"),
                _btn_group(
                    dbc.Button("Backfill Позиции", id="btn-backfill-active", color="warning", outline=True, size="sm", n_clicks=0),
                    dbc.Button("Missing History", id="btn-backfill-missing", color="warning", outline=True, size="sm", n_clicks=0),
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
                    dbc.Button("Обновить Cookie", id="btn-open-cookie-modal", color="warning", outline=True, size="sm", n_clicks=0),
                ),
                html.Div(id="ping-steam-last", children=_ping_label(last_ping), style={"marginTop": "4px"}),
            ]), width="auto"),
            dbc.Col(html.Span(id="health-action-msg", style={"color": _MUTED, "fontSize": "12px", "paddingTop": "18px", "display": "block"}), width="auto"),
        ], className="g-3 align-items-start", style={"marginBottom": "20px"}),
        dbc.Tooltip("Загружает актуальный инвентарь Steam", target="btn-sync-inventory", placement="bottom"),
        dbc.Tooltip("Обновляет список контейнеров с рынка Steam", target="btn-sync-catalog", placement="bottom"),
        dbc.Tooltip("Получает текущие цены. Тип определяется фильтром выше (*)", target="btn-sync-prices", placement="bottom"),
        dbc.Tooltip("Запрашивает цены только для контейнеров без данных об объёме (volume_7d = 0)", target="btn-missing-volume", placement="bottom"),
        dbc.Tooltip("История цен только для контейнеров с открытыми позициями. Фильтр типа не применяется.", target="btn-backfill-active", placement="bottom"),
        dbc.Tooltip("Загружает историю только для контейнеров с менее чем 5 записями за последние 90 дней", target="btn-backfill-missing", placement="bottom"),
        dbc.Tooltip("Очищает очередь задач воркера", target="btn-clear-queue", placement="bottom"),
        dbc.Tooltip("Проверяет токен и наличие блокировки Steam", target="btn-ping-steam", placement="bottom"),
        dbc.Tooltip("Открыть форму ввода Steam cookie вручную", target="btn-open-cookie-modal", placement="bottom"),
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

    # ── Task history ──────────────────────────────────────────────────────────
    history = getattr(health, "task_history", [])

    def _fmt_duration(s: int) -> str:
        if s < 60:
            return f"{s}s"
        return f"{s // 60}m {s % 60}s"

    def _history_row(t: dict) -> html.Tr:
        status = t.get("status", "?")
        color = {
            "ok": _GREEN,
            "error": _RED,
            "cancelled": _ORANGE,
        }.get(status, _MUTED)
        started = (t.get("started_at") or "")[:16].replace("T", " ")
        detail = t.get("detail") or t.get("error") or ""
        detail_color = _RED if t.get("error") else _MUTED
        task_id = t.get("id")
        summary_btn = (
            dbc.Button(
                "Summary",
                id={"type": "btn-task-summary", "index": task_id},
                color="secondary", outline=True, size="sm", n_clicks=0,
                style={"fontSize": "10px", "padding": "1px 6px"},
            )
            if task_id and t.get("has_summary") else html.Span()
        )
        return html.Tr([
            html.Td(t.get("type", "?"), style={"color": _TEXT, "fontSize": "12px"}),
            html.Td(status.upper(), style={"color": color, "fontSize": "11px", "fontWeight": "bold"}),
            html.Td(started, style={"color": _MUTED, "fontSize": "11px"}),
            html.Td(_fmt_duration(t.get("duration_s", 0)), style={"color": _MUTED, "fontSize": "11px"}),
            html.Td(
                detail,
                style={"color": detail_color, "fontSize": "10px", "maxWidth": "200px",
                       "overflow": "hidden", "textOverflow": "ellipsis", "whiteSpace": "nowrap"},
            ),
            html.Td(summary_btn),
        ])

    history_section = html.Div([
        html.Div(
            [
                html.H6("История задач", style={"color": _MUTED, "marginBottom": "0", "display": "inline"}),
                dbc.Button(
                    [html.I(className="fa fa-trash me-1"), "Очистить"],
                    id="btn-clear-task-history",
                    color="danger", outline=True, size="sm", n_clicks=0,
                    style={"fontSize": "11px", "float": "right"},
                ),
            ],
            style={"marginBottom": "8px", "overflow": "hidden"},
        ),
        html.Div(
            dbc.Table(
                [
                    html.Thead(html.Tr([
                        html.Th("Тип", style={"color": _MUTED}),
                        html.Th("Статус", style={"color": _MUTED}),
                        html.Th("Запуск", style={"color": _MUTED}),
                        html.Th("Время", style={"color": _MUTED}),
                        html.Th("Детали", style={"color": _MUTED}),
                        html.Th("", style={"color": _MUTED}),
                    ])),
                    html.Tbody(
                        [_history_row(t) for t in history]
                        if history else [
                            html.Tr(html.Td(
                                "Нет истории",
                                colSpan=6,
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

    return html.Div([
        html.H6("Liveness", style={"color": _MUTED, "marginBottom": "12px"}),
        liveness,
        cooldown_banner,
        html.H6("Actions", style={"color": _MUTED, "marginBottom": "8px"}),
        btn_row,
        history_section,
        blacklist_section,
        footer,
    ])
