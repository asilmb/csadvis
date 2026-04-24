"""
CS2 Market Analytics — Investment Dashboard.

Layout factory only — callbacks live in frontend/callbacks.py,
renderers in frontend/renderers/, shared helpers in frontend/helpers.py.
"""

from __future__ import annotations

import logging

import dash
import dash_bootstrap_components as dbc
from dash import dcc, html

from ui.cache import init_cache
from ui.callbacks import register_callbacks
from ui.helpers import (
    _BG,
    _BG2,
    _BLUE,
    _BORDER,
    _GOLD,
    _MUTED,
    _TEXT,
    _get_containers,
)

logger = logging.getLogger(__name__)

_VERSION = "2.4.2"  # bump this to confirm new code is running

# ─── Design token aliases (kept here for layout code) ──────────────────────────
_BG_WARN = "#3d2b00"  # stale calendar warning background (not in theme)


# ─── Dash app factory ──────────────────────────────────────────────────────────


def create_dash_app() -> dash.Dash:
    logger.info("CS2 Dashboard v%s starting", _VERSION)

    # PV-50: suppress per-request noise from Flask/Werkzeug in the unified log
    logging.getLogger("werkzeug").setLevel(logging.ERROR)

    app = dash.Dash(
        __name__,
        external_stylesheets=[dbc.themes.CYBORG, dbc.icons.FONT_AWESOME],
        title=f"CS2 Analytics v{_VERSION}",
        suppress_callback_exceptions=True,
    )

    # PV-10: Redis-backed cache for market overview (300 s TTL)
    init_cache(app.server)

    containers = _get_containers()
    default_id = containers[0].container_id if containers else None

    app.layout = dbc.Container(
        fluid=True,
        style={"backgroundColor": _BG, "minHeight": "100vh", "padding": "0"},
        children=[
            # ── Navbar ──────────────────────────────────────────────────────
            dbc.Navbar(
                dbc.Container(
                    [
                        html.Span(
                            "◈", style={"color": _GOLD, "fontSize": "22px", "marginRight": "8px"}
                        ),
                        dbc.NavbarBrand(
                            "CS2 Market Analytics",
                            style={"color": _TEXT, "fontWeight": "bold", "fontSize": "18px"},
                        ),
                        dbc.Nav(
                            [
                                dbc.NavItem(
                                    html.Span(
                                        id="scheduler-badge",
                                        style={
                                            "fontSize": "11px",
                                            "paddingTop": "8px",
                                            "paddingRight": "12px",
                                        },
                                    )
                                ),
                                # Emergency block badge (visible when STEALTH_BLOCK_EXPIRES set)
                                dbc.NavItem(
                                    html.Span(
                                        id="emergency-block-badge",
                                        style={"display": "none"},
                                    )
                                ),
                                # Cookie expiry badge (hidden until EXPIRED)
                                dbc.NavItem(
                                    html.Span(
                                        id="cookie-status-badge",
                                        style={"display": "none"},
                                    )
                                ),
                            ],
                            navbar=True,
                            className="ms-auto",
                        ),
                    ],
                    fluid=True,
                ),
                color=_BG2,
                dark=True,
                style={"borderBottom": f"1px solid {_BORDER}"},
            ),
            dbc.Row(
                [
                    # ── Sidebar ─────────────────────────────────────────────────
                    dbc.Col(
                        width=3,
                        style={
                            "backgroundColor": _BG2,
                            "minHeight": "calc(100vh - 56px)",
                            "borderRight": f"1px solid {_BORDER}",
                            "padding": "16px 14px",
                            "display": "flex",
                            "flexDirection": "column",
                        },
                        children=[
                            html.Div(
                                [
                                    html.Span("CONTAINERS", style={
                                        "color": _MUTED,
                                        "letterSpacing": "2px",
                                        "fontSize": "10px",
                                        "fontWeight": "bold",
                                    }),
                                    dbc.Button(
                                        "Скрытые",
                                        id="btn-toggle-blacklist-view",
                                        size="sm",
                                        color="danger",
                                        outline=True,
                                        n_clicks=0,
                                        style={"fontSize": "10px", "padding": "1px 6px", "lineHeight": "1.4"},
                                    ),
                                ],
                                style={"display": "flex", "justifyContent": "space-between", "alignItems": "center", "marginBottom": "8px"},
                            ),
                            dcc.Store(id="blacklist-view-store", data=False),
                            dbc.Input(
                                id="sidebar-search",
                                placeholder="Search...",
                                type="text",
                                size="sm",
                                style={
                                    "backgroundColor": _BG,
                                    "color": _TEXT,
                                    "borderColor": _BORDER,
                                    "marginBottom": "4px",
                                    "fontSize": "12px",
                                },
                            ),
                            dcc.Dropdown(
                                id="sidebar-sort",
                                options=[
                                    {"label": "Недавно обновлённые", "value": "recently_updated"},
                                    {"label": "Новые сначала", "value": "newest"},
                                    {"label": "Старые сначала", "value": "oldest"},
                                    {"label": "Дешевые сначала", "value": "price_asc"},
                                    {"label": "Дорогие сначала", "value": "price_desc"},
                                    {"label": "Большой объём", "value": "volume_desc"},
                                    {"label": "Малый объём", "value": "volume_asc"},
                                ],
                                value="recently_updated",
                                clearable=False,
                                style={
                                    "backgroundColor": _BG,
                                    "color": _TEXT,
                                    "borderColor": _BORDER,
                                    "fontSize": "11px",
                                    "marginBottom": "4px",
                                },
                                className="sidebar-sort-dropdown",
                            ),
                            dcc.Dropdown(
                                id="sidebar-type-filter",
                                options=[
                                    {"label": "Все типы", "value": ""},
                                    {"label": "Кейсы", "value": "Weapon Case"},
                                    {"label": "Сувенирные пакеты", "value": "Souvenir Package"},
                                    {"label": "Стикер-капсулы", "value": "Sticker Capsule"},
                                    {"label": "Автограф-капсулы", "value": "Autograph Capsule"},
                                    {"label": "Event-капсулы", "value": "Event Capsule"},
                                    {"label": "Терминалы", "value": "Sealed Terminal"},
                                ],
                                value="",
                                clearable=False,
                                style={
                                    "backgroundColor": _BG,
                                    "color": _TEXT,
                                    "borderColor": _BORDER,
                                    "fontSize": "11px",
                                    "marginBottom": "6px",
                                },
                                className="sidebar-sort-dropdown",
                            ),
                            html.Div(
                                id="container-list",
                                style={
                                    "flex": "1",
                                    "overflowY": "auto",
                                    "overflowX": "hidden",
                                    "marginBottom": "8px",
                                    "maxHeight": "calc(100vh - 180px)",
                                },
                            ),
                            # Stores
                            dcc.Store(id="selected-cid", data=default_id),
                            dcc.Store(id="invest-store", data={}),
                            dcc.Store(id="raw-items-store", data=[]),
                            dcc.Store(id="inventory-store", data=None),
                            dcc.Store(id="portfolio-balance", data=None),
                            dcc.Store(id="balance-refresh-store", data=0),
                            dcc.Store(id="price-count-store", data=None),
                            dcc.Store(id="task-done-ts", data=None),
                            dcc.Interval(
                                id="startup-interval",
                                interval=1_000,
                                n_intervals=0,
                                max_intervals=1,
                            ),
                            # Lightweight Redis poller — only reads one key, no DB
                            dcc.Interval(id="task-poll-interval", interval=2_000, n_intervals=0),
                            # Hidden stubs — callbacks write to these; UI no longer shows them
                            dbc.Button(id="btn-sync-all", style={"display": "none"}),
                            html.Div(id="last-sync-indicator", style={"display": "none"}),
                            # UX-13: sync state store (idle/loading/done-ok/done-partial/done-error)
                            dcc.Store(id="sync-state", data={"ts": None, "status": "idle"}),
                            dcc.Interval(
                                id="sync-reset-interval",
                                interval=2_000,
                                n_intervals=0,
                                disabled=True,
                            ),
                        ],
                    ),
                    # ── Main content ─────────────────────────────────────────────
                    dbc.Col(
                        width=9,
                        style={"padding": "20px"},
                        children=[
                            dcc.Tabs(
                                id="main-tabs",
                                value="system",  # PV-46: open on System tab for auth/health check
                                style={"backgroundColor": _BG2},
                                colors={
                                    "border": _BORDER,
                                    "primary": _BLUE,
                                    "background": _BG2,
                                },
                                children=[
                                    dcc.Tab(label="Анализ", value="market", className="custom-tab"),
                                    dcc.Tab(
                                        label="Inventory", value="inventory", className="custom-tab"
                                    ),
                                    dcc.Tab(
                                        label="Portfolio", value="portfolio", className="custom-tab"
                                    ),
                                    dcc.Tab(
                                        label="Balance", value="balance", className="custom-tab"
                                    ),
                                    dcc.Tab(
                                        label="Analytics", value="analytics", className="custom-tab"
                                    ),
                                    dcc.Tab(
                                        label="System", value="system", className="custom-tab"
                                    ),
                                ],
                            ),
                            # Global worker status bar — always visible regardless of active tab
                            html.Div(id="global-worker-status", style={"marginTop": "6px"}),
                            # Controls below tabs — avoids layout shift when shown/hidden (UX-11)
                            html.Div(
                                id="portfolio-controls-panel",
                                style={"display": "none"},
                                children=[
                                    html.Div(id="portfolio-balance-status", style={"display": "none"}),
                                    html.Div(id="wallet-fetch-btn", style={"display": "none"}),
                                ],
                            ),
                            html.Div(
                                id="inventory-controls-panel",
                                style={"display": "none", "marginTop": "24px"},
                                children=[
                                    html.Div(id="inventory-load-btn", style={"display": "none"}),
                                    html.Div(id="inventory-load-status", style={"display": "none"}),
                                    dbc.Row(
                                        dbc.Col(
                                            dbc.Switch(
                                                id="inventory-show-all",
                                                label="Показать всё (скины/наклейки полупрозрачно)",
                                                value=False,
                                                style={"fontSize": "12px", "marginTop": "8px"},
                                            ),
                                        ),
                                    ),
                                ],
                            ),
                            html.Div(
                                id="system-controls-panel",
                                style={"display": "none"},
                                children=[
                                    html.Div(id="worker-progress-section", style={"marginBottom": "16px"}),
                                    html.Div([
                                        dbc.Button(
                                            [html.I(className="fa fa-refresh me-1"), "Обновить"],
                                            id="btn-refresh-system",
                                            size="sm", color="secondary", outline=True,
                                            n_clicks=0,
                                            style={"fontSize": "11px", "padding": "1px 8px"},
                                        ),
                                        dcc.Dropdown(
                                            id="system-type-filter",
                                            options=[
                                                {"label": "Все типы", "value": ""},
                                                {"label": "Кейсы", "value": "Weapon Case"},
                                                {"label": "Сувенирные пакеты", "value": "Souvenir Package"},
                                                {"label": "Стикер-капсулы", "value": "Sticker Capsule"},
                                                {"label": "Автограф-капсулы", "value": "Autograph Capsule"},
                                                {"label": "Event-капсулы", "value": "Event Capsule"},
                                                {"label": "Терминалы", "value": "Sealed Terminal"},
                                            ],
                                            value="",
                                            clearable=False,
                                            style={
                                                "backgroundColor": _BG,
                                                "color": _TEXT,
                                                "borderColor": _BORDER,
                                                "fontSize": "11px",
                                                "width": "180px",
                                            },
                                            className="sidebar-sort-dropdown",
                                        ),
                                        dcc.Interval(
                                            id="worker-progress-interval",
                                            interval=3_000,
                                            n_intervals=0,
                                            disabled=False,
                                        ),
                                    ], style={"display": "flex", "gap": "10px", "alignItems": "center", "marginBottom": "16px"}),
                                ],
                            ),
                            dcc.Loading(
                                id="tab-content-loading",
                                type="circle",
                                color=_BLUE,
                                style={"minHeight": "60px"},
                                children=html.Div(
                                    id="tab-content", style={"marginTop": "12px"}
                                ),
                            ),
                        ],
                    ),
                ],
                style={"margin": "0"},
            ),
            # ── Notification bus (hidden) — callbacks still write here; ────────
            # ── aggregator picks it up and pushes to toast-store.       ────────
            dbc.Toast(
                id="app-toast",
                header="",
                is_open=False,
                dismissable=False,
                duration=4000,
                style={"display": "none"},
            ),
            # ── Notification store — max 5 entries, oldest dropped ──────────────
            dcc.Store(id="toast-store", data=[]),
            # ── Visible stacked toasts — rendered from toast-store ──────────────
            html.Div(
                id="toast-stack-container",
                style={
                    "position": "fixed",
                    "top": "20px",
                    "right": "20px",
                    "zIndex": 9999,
                    "display": "flex",
                    "flexDirection": "column",
                    "gap": "8px",
                    "width": "320px",
                },
            ),
            # Cookie status polling — 30s interval
            dcc.Interval(id="cookie-status-interval", interval=30_000, n_intervals=0),
            # Auth-pause polling — enabled only when cs2:worker:auth_paused Redis key exists
            dcc.Interval(id="auth-check-interval", interval=3_000, n_intervals=0, disabled=True),
            # Auth-Pause Modal — opens when worker enters PAUSED_AUTH (no credentials)
            dbc.Modal(
                id="auth-modal",
                is_open=False,
                backdrop="static",
                keyboard=False,
                children=[
                    dbc.ModalHeader(
                        dbc.ModalTitle([
                            html.I(className="fa fa-lock me-2"),
                            "Требуется авторизация Steam",
                        ]),
                        close_button=False,
                    ),
                    dbc.ModalBody([
                        dbc.Alert(
                            [
                                html.I(className="fa fa-exclamation-triangle me-2"),
                                "Воркер приостановлен: Steam вернул ошибку авторизации. "
                                "Введи актуальные cookies чтобы продолжить парсинг.",
                            ],
                            color="warning",
                            className="mb-3",
                            style={"fontSize": "13px"},
                        ),
                        dbc.Label("steamLoginSecure (оставь пустым если не изменился)", html_for="auth-login-secure-input"),
                        dbc.Input(
                            id="auth-login-secure-input",
                            type="password",
                            placeholder="76561198…%7C%7C…",
                            debounce=False,
                            className="mb-3",
                        ),
                        dbc.Label("Session ID (оставь пустым если не изменился)", html_for="auth-session-id-input"),
                        dbc.Input(
                            id="auth-session-id-input",
                            type="text",
                            placeholder="a1b2c3d4e5f6…",
                            debounce=False,
                            className="mb-2",
                        ),
                        html.Small(
                            "DevTools → Application → Cookies → steamcommunity.com",
                            className="text-muted d-block mb-2",
                        ),
                        html.Div(id="auth-modal-status", className="text-danger mt-1", style={"fontSize": "12px"}),
                    ]),
                    dbc.ModalFooter(
                        dbc.Button(
                            [html.I(className="fa fa-unlock me-2"), "Отправить и разблокировать воркер"],
                            id="auth-submit-btn",
                            color="primary",
                            n_clicks=0,
                        ),
                    ),
                ],
            ),
            # Cookie Hot-Swap Modal (PV-43) — opens automatically when cookie is EXPIRED
            dbc.Modal(
                id="cookie-modal",
                is_open=False,
                backdrop="static",  # prevent closing by clicking outside
                children=[
                    dbc.ModalHeader(dbc.ModalTitle("Сессия Steam истекла")),
                    dbc.ModalBody([
                        html.P(
                            "Введи значения куки из DevTools → Application → Cookies → steamcommunity.com."
                        ),
                        dbc.Label("steamLoginSecure (оставь пустым если не изменился)", html_for="cookie-input", className="mt-2"),
                        dbc.Input(
                            id="cookie-input",
                            type="password",
                            placeholder="76561198…%7C%7C…",
                            debounce=False,
                        ),
                        dbc.Label("sessionid (оставь пустым если не изменился)", html_for="sessionid-input", className="mt-3"),
                        dbc.Input(
                            id="sessionid-input",
                            type="text",
                            placeholder="a1b2c3d4e5f6…",
                            debounce=False,
                        ),
                        dbc.Label("Описание сессии (необязательно)", html_for="session-note-input", className="mt-3"),
                        dbc.Input(
                            id="session-note-input",
                            type="text",
                            placeholder="Напр.: Chrome, Windows, апрель 2026",
                            debounce=False,
                        ),
                        dbc.Label("Steam ID (оставь пустым если не изменился)", html_for="steam-id-input", className="mt-3"),
                        dbc.Input(
                            id="steam-id-input",
                            type="text",
                            placeholder="76561198XXXXXXXXX",
                            debounce=False,
                        ),
                        html.Div(id="cookie-update-status", className="mt-2 text-danger"),
                    ]),
                    dbc.ModalFooter([
                        dbc.Button(
                            "Обновить и запустить синхронизацию",
                            id="cookie-submit-btn",
                            color="primary",
                            n_clicks=0,
                        ),
                        dbc.Button("Закрыть", id="cookie-close-btn", color="secondary", className="ms-2", n_clicks=0),
                    ]),
                ],
            ),
        ],
    )

    register_callbacks(app)

    return app
