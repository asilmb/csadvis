"""
System Status tab renderer (PV-37).

Displays live health metrics:
  - Liveness: cookie status, token bucket level, active worker count
  - Watchdog: WorkerRegistry table with stale-heartbeat highlighting (>90s = red)
  - Task Control: last 10 PROCESSING tasks + action buttons
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

_STALE_THRESHOLD_S = 90  # seconds — matches WORKER_STUCK_THRESHOLD_S


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


def render_system_status(health=None) -> html.Div:
    """
    Build the full System Status tab layout.

    Parameters
    ----------
    health : SystemHealth | None
        When None (initial render), show skeleton / loading state.
        Called again by the interval callback with a real SystemHealth object.
    """
    if health is None:
        return html.Div(
            dbc.Spinner(color="primary", size="lg"),
            style={"textAlign": "center", "paddingTop": "80px"},
        )

    # ── Liveness block ────────────────────────────────────────────────────────
    cookie_color = _GREEN if health.cookie_set else _RED
    cookie_text = "Valid" if health.cookie_set else "Not set"

    if health.tokens is None:
        bucket_text, bucket_color = "N/A", _MUTED
    elif health.token_level == "HIGH":
        bucket_text, bucket_color = f"HIGH ({health.tokens:.1f}/15)", _GREEN
    elif health.token_level == "MED":
        bucket_text, bucket_color = f"MED ({health.tokens:.1f}/15)", _GOLD
    else:
        bucket_text, bucket_color = f"LOW ({health.tokens:.1f}/15)", _RED

    if health.circuit_open:
        bucket_text = "FROZEN (circuit open)"
        bucket_color = _RED

    liveness = dbc.Row(
        [
            dbc.Col(
                html.Div([
                    html.Div("Steam Cookie", style=_LABEL),
                    _badge(cookie_text, cookie_color),
                ], style=_CARD),
                width=4,
            ),
            dbc.Col(
                html.Div([
                    html.Div("Token Bucket", style=_LABEL),
                    html.Span(bucket_text, style={**_VALUE, "color": bucket_color}),
                ], style=_CARD),
                width=4,
            ),
            dbc.Col(
                html.Div([
                    html.Div("Active Workers", style=_LABEL),
                    html.Span(str(health.active_workers), style=_VALUE),
                ], style=_CARD),
                width=4,
            ),
        ],
        className="g-2",
        style={"marginBottom": "20px"},
    )

    # ── Worker Registry table ─────────────────────────────────────────────────
    def _worker_row(w: dict) -> html.Tr:
        age = w["heartbeat_age_s"]
        if age is None:
            age_text, row_style = "—", {}
        elif age > _STALE_THRESHOLD_S:
            age_text = f"{age}s ⚠"
            row_style = {"backgroundColor": "#3d0000", "color": _RED}
        else:
            age_text, row_style = f"{age}s", {}

        status_color = _GREEN if w["status"] in ("IDLE", "BUSY") else _RED
        return html.Tr(
            [
                html.Td(w["name"], style={"color": _TEXT}),
                html.Td(
                    html.Span(w["status"], style={"color": status_color, "fontWeight": "600"}),
                ),
                html.Td(age_text),
                html.Td(w["current_task_id"], style={"fontFamily": "monospace", "fontSize": "11px"}),
            ],
            style=row_style,
        )

    registry_table = dbc.Table(
        [
            html.Thead(html.Tr([
                html.Th("Worker", style={"color": _MUTED}),
                html.Th("Status", style={"color": _MUTED}),
                html.Th("Heartbeat Age", style={"color": _MUTED}),
                html.Th("Task ID", style={"color": _MUTED}),
            ])),
            html.Tbody([_worker_row(w) for w in health.workers] or [
                html.Tr(html.Td("No workers registered", colSpan=4, style={"color": _MUTED, "textAlign": "center"}))
            ]),
        ],
        bordered=False, size="sm",
        style={"fontSize": "13px"},
    )

    # ── Recent task history ────────────────────────────────────────────────────
    _STATUS_COLOR = {
        "COMPLETED": _GREEN,
        "PROCESSING": _GOLD,
        "FAILED": _RED,
        "RETRY": _ORANGE,
        "PENDING": _MUTED,
        "PAUSED_AUTH": _RED,
    }

    def _task_row(t: dict) -> html.Tr:
        status = t.get("status", "PENDING")
        status_color = _STATUS_COLOR.get(status, _MUTED)
        pri_color = _GREEN if t["priority"] == 1 else (_GOLD if t["priority"] == 2 else _MUTED)
        # Show live progress from payload when available (e.g. backfill_history)
        display_type = t.get("progress") or t["type"]

        # Time column: show completed_at if finished, else age
        if t.get("completed_at"):
            time_cell = html.Td(t["completed_at"], style={"color": _MUTED, "fontSize": "11px"})
        else:
            time_cell = html.Td(f"{t['age_s']}s", style={"color": _MUTED, "fontSize": "11px"})

        # Error tooltip on FAILED rows
        err = t.get("error_message", "")
        err_cell = html.Td(
            html.Span(
                err[:60] + ("…" if len(err) > 60 else ""),
                title=err,
                style={"color": _RED, "fontSize": "10px", "cursor": "help"},
            ) if err else "—",
            style={"maxWidth": "200px", "overflow": "hidden"},
        )

        return html.Tr([
            html.Td(t["id"], style={"fontFamily": "monospace", "fontSize": "11px", "color": _MUTED}),
            html.Td(display_type, style={"fontSize": "12px"}),
            html.Td(
                html.Span(status, style={"color": status_color, "fontWeight": "700", "fontSize": "11px"}),
            ),
            html.Td(html.Span(["P", str(t["priority"])], style={"color": pri_color, "fontWeight": "600", "fontSize": "11px"})),
            time_cell,
            err_cell,
        ])

    tasks_table = dbc.Table(
        [
            html.Thead(html.Tr([
                html.Th("Task ID", style={"color": _MUTED}),
                html.Th("Type", style={"color": _MUTED}),
                html.Th("Status", style={"color": _MUTED}),
                html.Th("Pri", style={"color": _MUTED}),
                html.Th("Time", style={"color": _MUTED}),
                html.Th("Error", style={"color": _MUTED}),
            ])),
            html.Tbody([_task_row(t) for t in health.recent_tasks] or [
                html.Tr(html.Td("No task history yet", colSpan=6, style={"color": _MUTED, "textAlign": "center"}))
            ]),
        ],
        bordered=False, size="sm",
        style={"fontSize": "13px"},
    )

    # ── Status counters ───────────────────────────────────────────────────────
    failed_color = _RED if health.failed_count > 0 else _GREEN

    counters = dbc.Row([
        dbc.Col(html.Div([
            html.Div("Pending", style=_LABEL),
            html.Span(str(health.pending_count), style=_VALUE),
        ], style=_CARD), width=2),
        dbc.Col(html.Div([
            html.Div("Processing", style=_LABEL),
            html.Span(str(sum(1 for t in health.recent_tasks if t.get("status") == "PROCESSING")), style=_VALUE),
        ], style=_CARD), width=2),
        dbc.Col(html.Div([
            html.Div("Failed", style=_LABEL),
            html.Span(str(health.failed_count), style={**_VALUE, "color": failed_color}),
        ], style=_CARD), width=2),
    ], className="g-2", style={"marginBottom": "20px"})

    # ── Action buttons ────────────────────────────────────────────────────────
    btn_row = html.Div([
        dbc.Row([
            dbc.Col(dbc.Button(
                [html.I(className="fa fa-refresh me-1"), "Force Global Sync"],
                id="btn-force-sync",
                color="primary", size="sm", n_clicks=0,
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
            dbc.Col(dbc.Button(
                "Reclaim Stuck",
                id="btn-reclaim-stuck",
                color="warning", outline=True, size="sm", n_clicks=0,
            ), width="auto"),
            dbc.Col(dbc.Button(
                "Flush Failed",
                id="btn-flush-failed",
                color="danger", outline=True, size="sm", n_clicks=0,
            ), width="auto"),
            dbc.Col(dbc.Button(
                "Reset Workers",
                id="btn-reset-workers",
                color="secondary", outline=True, size="sm", n_clicks=0,
            ), width="auto"),
            dbc.Col(
                html.Span(
                    id="health-action-msg",
                    style={"color": _MUTED, "fontSize": "12px", "paddingTop": "4px"},
                ),
                width="auto",
            ),
        ], className="g-2 align-items-center", style={"marginBottom": "20px"}),
        dbc.Tooltip("Запускает полный цикл: цены + инвентарь + каталог + пересчёт портфеля", target="btn-force-sync", placement="bottom"),
        dbc.Tooltip("Загружает актуальный инвентарь Steam (предметы на аккаунте)", target="btn-sync-inventory", placement="bottom"),
        dbc.Tooltip("Обновляет список контейнеров с рынка Steam", target="btn-sync-catalog", placement="bottom"),
        dbc.Tooltip("Получает текущие цены на все контейнеры из Steam Market", target="btn-sync-prices", placement="bottom"),
        dbc.Tooltip("Освобождает задачи зависшие в статусе PROCESSING дольше допустимого", target="btn-reclaim-stuck", placement="bottom"),
        dbc.Tooltip("Удаляет все задачи в статусе FAILED из очереди (необратимо)", target="btn-flush-failed", placement="bottom"),
        dbc.Tooltip("Сбрасывает зависших/мёртвых воркеров в статус IDLE и снимает блокировки", target="btn-reset-workers", placement="bottom"),
    ])

    footer = html.Div(
        f"Last refresh: {health.timestamp}",
        style={"color": _MUTED, "fontSize": "11px", "marginTop": "8px"},
    )

    return html.Div([
        html.H6("Liveness", style={"color": _MUTED, "marginBottom": "12px"}),
        liveness,
        html.H6("Watchdog — Worker Registry", style={"color": _MUTED, "marginBottom": "8px"}),
        html.Div(registry_table, style={**_CARD, "marginBottom": "20px"}),
        html.H6("Task Control", style={"color": _MUTED, "marginBottom": "8px"}),
        counters,
        html.Div([
            html.Div("Recent Task History (last 15)", style={**_LABEL, "marginBottom": "8px"}),
            tasks_table,
        ], style={**_CARD, "marginBottom": "16px"}),
        btn_row,
        dcc.ConfirmDialog(
            id="confirm-flush",
            message="Delete ALL FAILED tasks from the queue? This cannot be undone.",
        ),
        dcc.ConfirmDialog(
            id="confirm-reset-workers",
            message="Mark all STUCK/DEAD workers as IDLE and clear their task locks?",
        ),
        footer,
    ])
