"""
Dash callback registration for the CS2 Analytics dashboard.

All @app.callback functions are registered here via register_callbacks(app).
Import this module in create_dash_app() and call register_callbacks(app)
after the layout is set.
"""

from __future__ import annotations

import json as _json
import logging
import traceback
from datetime import UTC, datetime
from typing import Any

import dash
import dash_bootstrap_components as dbc
from dash import ALL, Input, Output, State, callback_context, html, no_update

from config import settings as _settings
from src.domain.event_calendar import get_event_signals as _get_ev_signals
from src.domain.value_objects import Amount
from ui.helpers import (
    _BG,
    _BG3,
    _BG_SEL,
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
    _get_containers,
    _get_current_steam_prices,
    _no_data,
)
from ui.renderers.analytics import _render_analytics
from ui.renderers.inventory import _render_inventory
from ui.renderers.market import _render_market
from ui.renderers.portfolio import _render_portfolio
from ui.theme import verdict_color

logger = logging.getLogger(__name__)

_latest_ts_cache: dict = {}
_latest_ts_cache_at: float = 0.0


def _get_latest_ts_map() -> dict:
    import time
    global _latest_ts_cache, _latest_ts_cache_at
    if time.monotonic() - _latest_ts_cache_at < 60:
        return _latest_ts_cache
    from sqlalchemy import func as _func
    from src.domain.connection import SessionLocal as _SL
    from src.domain.models import FactContainerPrice as _FCP
    try:
        with _SL() as _db:
            rows = _db.query(
                _FCP.container_id,
                _func.max(_FCP.timestamp).label("max_ts"),
            ).group_by(_FCP.container_id).all()
            _latest_ts_cache = {str(cid): ts for cid, ts in rows if ts}
            _latest_ts_cache_at = time.monotonic()
    except Exception:
        pass
    return _latest_ts_cache


def _render_balance_tab(wallet_balance: Any, inventory_data: Any) -> html.Div:
    from ui.balance import render_balance

    return render_balance(wallet_balance, inventory_data)


def _get_system_health():
    """Fetch system health snapshot for the Status tab (calls API + DB)."""
    import requests as _req

    from config import settings as _s
    from infra.steam_credentials import get_login_secure

    worker_state: dict = {}
    try:
        r = _req.get(
            f"http://{_s.api_internal_host}:{_s.api_port}/api/v1/system/queue-status",
            timeout=3,
        )
        worker_state = r.json() if r.ok else {}
    except Exception as _e:
        logger.debug("health: queue-status unavailable: %s", _e)

    from src.domain.connection import SessionLocal
    from src.domain.models import DimContainer
    blacklisted = []
    try:
        with SessionLocal() as db:
            bl = db.query(DimContainer).filter(DimContainer.is_blacklisted == 1).all()
            blacklisted = [{"container_id": str(c.container_id), "container_name": c.container_name} for c in bl]
    except Exception as _e:
        logger.debug("health: blacklist query failed: %s", _e)

    cooldown_until = None
    scrape_sessions = []
    try:
        r = _req.get(
            f"http://{_s.api_internal_host}:{_s.api_port}/api/v1/scrape-sessions/cooldown",
            timeout=3,
        )
        if r.ok:
            cd = r.json()
            cooldown_until = cd.get("cooldown_until_fmt") if cd.get("active") else None
    except Exception as _e:
        logger.debug("health: cooldown unavailable: %s", _e)
    try:
        r = _req.get(
            f"http://{_s.api_internal_host}:{_s.api_port}/api/v1/scrape-sessions/",
            timeout=3,
        )
        if r.ok:
            scrape_sessions = r.json()
    except Exception as _e:
        logger.debug("health: scrape-sessions unavailable: %s", _e)

    last_ping = None
    try:
        r = _req.get(
            f"http://{_s.api_internal_host}:{_s.api_port}/api/v1/system/last-ping",
            timeout=3,
        )
        if r.ok:
            last_ping = r.json()
    except Exception as _e:
        logger.debug("health: last-ping unavailable: %s", _e)

    task_history = []
    try:
        r = _req.get(
            f"http://{_s.api_internal_host}:{_s.api_port}/api/v1/system/task-history",
            timeout=3,
        )
        if r.ok:
            task_history = r.json()
    except Exception as _e:
        logger.debug("health: task-history unavailable: %s", _e)

    from datetime import UTC, datetime

    class _Health:
        cookie_set = bool(get_login_secure())
        tokens = None
        token_level = "N/A"
        circuit_open = False
        blacklisted_containers = blacklisted
        timestamp = datetime.now(UTC).strftime("%H:%M:%S")
        worker = worker_state

    _Health.cooldown_until = cooldown_until
    _Health.scrape_sessions = scrape_sessions
    _Health.last_ping = last_ping
    _Health.task_history = task_history

    return _Health()


def register_callbacks(app: Any) -> None:
    """Register all Dash callbacks on the given app instance."""

    # ── Global worker status bar — visible on all tabs ───────────────────────
    @app.callback(
        Output("global-worker-status", "children"),
        Input("task-poll-interval", "n_intervals"),
        prevent_initial_call=False,
    )
    def update_global_worker_status(_n: Any) -> Any:
        import requests as _req
        try:
            r = _req.get(
                f"http://{_settings.api_internal_host}:{_settings.api_port}/api/v1/system/queue-status",
                timeout=2,
            )
            state = r.json() if r.ok else {}
        except Exception:
            return None
        if not state.get("busy"):
            queue = state.get("queue_items", [])
            if not queue:
                return None
            return html.Div(
                f"В очереди: {', '.join(queue)}",
                style={"fontSize": "11px", "color": _MUTED, "padding": "4px 0"},
            )
        job = state.get("current_type", "")
        cur = state.get("progress_current", 0)
        tot = state.get("progress_total", 0)
        eta = state.get("eta_seconds")
        name = state.get("last_item_name", "")
        parts = [f"⟳ {job}"]
        if tot:
            parts.append(f"{cur}/{tot}")
        if eta:
            parts.append(f"~{eta // 60}m {eta % 60}s")
        if name:
            parts.append(name)
        queue = state.get("queue_items", [])
        if queue:
            parts.append(f"| далее: {', '.join(queue)}")
        return html.Div(
            "  ".join(parts),
            style={"fontSize": "11px", "color": _YELLOW, "padding": "4px 0", "fontFamily": "monospace"},
        )

    # ── Task-completion poller: reads Redis key, updates store on change ──────
    @app.callback(
        Output("task-done-ts", "data"),
        Input("task-poll-interval", "n_intervals"),
        State("task-done-ts", "data"),
        prevent_initial_call=False,
    )
    def poll_task_done(n: Any, current_ts: Any) -> Any:
        try:
            from infra.redis_client import get_redis
            ts = get_redis().get("cs2:ui:last_task_done")
        except Exception:
            ts = None
        if ts and ts != current_ts:
            global _latest_ts_cache_at
            _latest_ts_cache_at = 0.0  # force refresh on next sort render
            return ts
        raise dash.exceptions.PreventUpdate

    @app.callback(
        Output("auth-check-interval", "disabled"),
        Input("task-poll-interval", "n_intervals"),
        prevent_initial_call=False,
    )
    def toggle_auth_interval(_n: Any) -> bool:
        try:
            from infra.redis_client import get_redis
            return not bool(get_redis().exists("cs2:worker:auth_paused"))
        except Exception:
            return True

    @app.callback(
        Output("raw-items-store", "data"),
        Output("invest-store", "data"),
        Input("task-done-ts", "data"),
        Input("startup-interval", "n_intervals"),
        prevent_initial_call=False,
    )
    def refresh_prices(_ts: Any, _startup: Any) -> Any:
        # PV-05: all market data flows through ItemService (never raw repo/DB in callbacks)
        from src.domain.item_service import ItemService

        svc = ItemService.open()
        try:
            invest = svc.get_signals()
        finally:
            svc.close()

        return [], invest

    @app.callback(
        Output("blacklist-view-store", "data"),
        Output("btn-toggle-blacklist-view", "outline"),
        Output("btn-toggle-blacklist-view", "children"),
        Input("btn-toggle-blacklist-view", "n_clicks"),
        State("blacklist-view-store", "data"),
        prevent_initial_call=True,
    )
    def toggle_blacklist_view(n: Any, is_bl: Any) -> Any:
        new_val = not bool(is_bl)
        return new_val, not new_val, ("Активные" if new_val else "Скрытые")

    @app.callback(
        Output("bl-scan-msg", "children"),
        Input("btn-bl-backfill", "n_clicks"),
        Input("btn-bl-prices", "n_clicks"),
        prevent_initial_call=True,
    )
    def do_bl_scan(n_backfill: Any, n_prices: Any) -> str:
        if not n_backfill and not n_prices:
            raise dash.exceptions.PreventUpdate
        ctx = callback_context
        btn_id = ctx.triggered[0]["prop_id"].split(".")[0]
        import requests as _req
        try:
            if btn_id == "btn-bl-backfill":
                r = _req.post("http://api:8000/sync/backfill/blacklisted", timeout=5)
                return r.json().get("message", "OK")
            else:
                r = _req.post("http://api:8000/sync/market/prices/blacklisted", timeout=5)
                return r.json().get("message", "OK")
        except Exception as exc:
            return str(exc)

    @app.callback(
        Output("container-list", "children"),
        Input("invest-store", "data"),
        Input("selected-cid", "data"),
        Input("sidebar-search", "value"),
        Input("inventory-store", "data"),
        Input("sidebar-sort", "value"),
        Input("blacklist-view-store", "data"),
        Input("sidebar-type-filter", "value"),
    )
    def render_container_list(
        invest: Any, selected_cid: Any, search: Any, inventory_data: Any, sort: Any, show_blacklisted: Any, type_filter: Any
    ) -> Any:
        invest = invest or {}
        search = (search or "").lower().strip()
        sort = sort or "recently_updated"
        type_filter = (type_filter or "").strip()
        show_blacklisted = bool(show_blacklisted)
        all_containers = _get_containers(blacklisted=show_blacklisted)

        if type_filter:
            all_containers = [c for c in all_containers if str(c.container_type) == type_filter]

        latest_ts_map: dict = _get_latest_ts_map() if sort == "recently_updated" else {}

        def _main_key(c):
            from datetime import datetime as _dt2
            cid = str(c.container_id)
            sig = invest.get(cid, {})
            price = sig.get("current_price") or 0.0
            volume = sig.get("quantity") or 0
            name = str(c.container_name)
            if sort == "recently_updated":
                return latest_ts_map.get(cid) or _dt2.min
            if sort in ("newest", "oldest"):
                return name
            if sort == "price_asc":
                return price if price > 0 else float("inf")
            if sort == "price_desc":
                return -(price or 0.0)
            if sort == "volume_desc":
                return -(volume or 0)
            if sort == "volume_asc":
                return volume if volume > 0 else float("inf")
            return name

        reverse_main = sort in ("oldest",)
        if sort == "recently_updated":
            reverse_main = True
        all_containers = sorted(all_containers, key=_main_key, reverse=reverse_main)

        # Build owned count map: container_name -> total count
        owned_map: dict = {}
        if inventory_data:
            for item in inventory_data:
                n = item.get("market_hash_name", "")
                if n:
                    owned_map[n] = owned_map.get(n, 0) + item.get("count", 1)

        # NO DATA uses _MUTED border (not a signal color) to avoid visual noise in sidebar.
        def _vstyle(v: str) -> tuple[str, str, str]:
            label = v if v != "NO DATA" else ""
            c = _MUTED if v == "NO DATA" else verdict_color(v)
            return c, c, label

        # Compute event signals once for all container names (Batch A — M-05)
        all_names = [str(c.container_name) for c in all_containers]
        ev_signals_map = _get_ev_signals(all_names)

        _EV_SIG_COLOR = {"BUY": _GREEN, "HOLD": _YELLOW, "SELL": _RED}
        _EV_SIG_BADGE = {
            "BUY": "success",
            "HOLD": "warning",
            "SELL": "danger",
        }

        sections = []
        for c in all_containers:
            name_str = str(c.container_name)
            if search and search not in name_str.lower():
                continue

            cid = str(c.container_id)
            sig = invest.get(cid, {})
            verdict = sig.get("verdict", "NO DATA")
            border_c, text_c, label = _vstyle(verdict)
            is_sel = cid == str(selected_cid or "")
            owned = owned_map.get(name_str, 0)

            # Event signal badge
            ev_info = ev_signals_map.get(name_str, {})
            ev_sig = ev_info.get("signal")
            ev_badge = None
            if ev_sig and ev_sig != "HOLD":
                ev_badge = dbc.Badge(
                    ev_sig,
                    color=_EV_SIG_BADGE.get(ev_sig, "secondary"),
                    style={"fontSize": "8px", "marginLeft": "4px"},
                )

            badge_els = []
            if owned > 0:
                badge_els.append(
                    html.Span(
                        f"x{owned}",
                        style={
                            "fontSize": "9px",
                            "color": _BLUE,
                            "backgroundColor": _BG3,
                            "borderRadius": "3px",
                            "padding": "0px 4px",
                            "marginLeft": "5px",
                            "fontWeight": "bold",
                        },
                    )
                )
            if ev_badge:
                badge_els.append(ev_badge)

            is_blacklisted = getattr(c, "is_blacklisted", False)
            _btn_base = {
                "background": "none", "border": "none", "cursor": "pointer",
                "padding": "2px 4px", "lineHeight": "1", "flexShrink": "0",
            }
            sections.append(
                html.Div(
                    style={
                        "borderLeft": f"3px solid {border_c}",
                        "backgroundColor": _BG_SEL if is_sel else _BG,
                        "marginBottom": "3px",
                        "borderRadius": "2px",
                        "display": "flex",
                        "alignItems": "center",
                        "opacity": "1",
                    },
                    children=[
                        # Clickable name area
                        html.Div(
                            id={"type": "ccard", "index": c.container_id},
                            n_clicks=0,
                            style={"flex": "1", "padding": "7px 6px 7px 10px", "cursor": "pointer", "minWidth": "0"},
                            children=[
                                html.Div(
                                    [
                                        html.Span(
                                            name_str,
                                            style={
                                                "color": _TEXT,
                                                "fontSize": "11px",
                                                "fontWeight": "bold" if is_sel else "normal",
                                                "overflow": "hidden",
                                                "textOverflow": "ellipsis",
                                                "whiteSpace": "nowrap",
                                                "display": "block",
                                            },
                                        ),
                                        *badge_els,
                                    ]
                                ),
                                html.Div(
                                    label,
                                    style={"color": text_c, "fontSize": "10px", "marginTop": "2px"},
                                ),
                            ],
                        ),
                        # Refresh button
                        html.Button(
                            html.I(className="fa fa-refresh", style={"fontSize": "11px", "color": _MUTED}),
                            id={"type": "btn-refresh-price", "index": cid},
                            n_clicks=0,
                            title="Обновить цену",
                            style=_btn_base,
                        ),
                        # Hide / unblock button
                        html.Button(
                            html.I(
                                className="fa fa-trash" if not is_blacklisted else "fa fa-eye",
                                style={"fontSize": "11px", "color": _MUTED if not is_blacklisted else _GREEN},
                            ),
                            id={"type": "btn-toggle-blacklist", "index": cid},
                            n_clicks=0,
                            title="Скрыть контейнер" if not is_blacklisted else "Показать контейнер",
                            style={**_btn_base, "paddingRight": "8px"},
                        ),
                    ],
                )
            )

        if not sections:
            return html.Div(
                "No containers found.",
                style={
                    "color": _MUTED,
                    "fontSize": "12px",
                    "textAlign": "center",
                    "padding": "12px",
                },
            )
        return sections

    @app.callback(
        Output("selected-cid", "data"),
        Input({"type": "ccard", "index": ALL}, "n_clicks"),
        prevent_initial_call=True,
    )
    def select_container(n_clicks_list: Any) -> Any:
        ctx = callback_context
        if not ctx.triggered:
            return no_update
        triggered = [t for t in ctx.triggered if t.get("value") and t["value"] > 0]
        if not triggered:
            return no_update
        prop_id = triggered[0]["prop_id"]
        try:
            return _json.loads(prop_id.rsplit(".", 1)[0])["index"]
        except Exception:
            return no_update

    @app.callback(
        Output("main-tabs", "value"),
        Input("selected-cid", "data"),
        prevent_initial_call=True,
    )
    def navigate_to_analysis_on_select(_cid: Any) -> Any:
        """Switch to the Analysis tab whenever the user clicks a container in the sidebar."""
        ctx = callback_context
        if not ctx.triggered:
            return no_update
        return "market"

    @app.callback(
        Output("portfolio-controls-panel", "style"),
        Input("main-tabs", "value"),
    )
    def toggle_portfolio_controls(tab: Any) -> Any:
        base = {"marginTop": "24px", "marginBottom": "12px"}
        return {**base, "display": "block" if tab == "portfolio" else "none"}

    @app.callback(
        Output("inventory-controls-panel", "style"),
        Input("main-tabs", "value"),
    )
    def toggle_inventory_controls(tab: Any) -> Any:
        base = {"marginTop": "24px", "marginBottom": "12px"}
        return {**base, "display": "block" if tab == "inventory" else "none"}

    @app.callback(
        Output("system-controls-panel", "style"),
        Input("main-tabs", "value"),
    )
    def toggle_system_controls(tab: Any) -> Any:
        return {"display": "block" if tab == "system" else "none"}

    @app.callback(
        Output("price-count-store", "data"),
        Input("startup-interval", "n_intervals"),
        Input("task-done-ts", "data"),
    )
    def load_price_count(_startup: Any, _ts: Any) -> Any:
        """Return number of items with prices so render_tab can show cold-start banner."""
        try:
            from src.domain.item_service import ItemService

            svc = ItemService.open()
            try:
                return len(svc.get_market_overview())
            finally:
                svc.close()
        except Exception:
            return 0

    @app.callback(
        Output("portfolio-balance", "data"),
        Output("portfolio-balance-status", "children"),
        Output("app-toast", "children", allow_duplicate=True),
        Output("app-toast", "header", allow_duplicate=True),
        Output("app-toast", "is_open", allow_duplicate=True),
        Output("app-toast", "icon", allow_duplicate=True),
        Input("startup-interval", "n_intervals"),
        Input("wallet-fetch-btn", "n_clicks"),
        prevent_initial_call="initial_duplicate",
    )
    def load_wallet_balance(n_intervals: Any, n_clicks: Any) -> Any:
        from scrapper.steam_wallet import (
            fetch_wallet_balance,
            get_saved_balance,
            save_balance,
        )

        is_btn = callback_context.triggered_id == "wallet-fetch-btn"
        balance, msg = fetch_wallet_balance()

        if balance is None:
            cached = get_saved_balance()
            if msg == "NO_COOKIE":
                hint = "Cookie не найден — запусти: cs2 cookie"
                toast_hdr = "Нет Steam cookie"
                toast_msg = "Запусти в терминале: cs2 cookie"
            else:
                hint = f"{msg} — обнови: cs2 cookie"
                toast_hdr = "Cookie устарел"
                toast_msg = hint
            if cached:
                status_text = html.Span(
                    [f"{int(cached):,} {_settings.currency_symbol} (устарел)  — ", hint],
                    style={"color": _MUTED, "fontSize": "12px"},
                )
            else:
                status_text = html.Span(hint, style={"color": _MUTED, "fontSize": "12px"})
            show_toast = is_btn
            return cached, status_text, toast_msg, toast_hdr, show_toast, "warning"

        save_balance(balance)
        status_text = html.Span(f"{int(balance):,} {_settings.currency_symbol}", style={"color": _GREEN, "fontSize": "13px"})
        toast_msg = f"Баланс: {int(balance):,} {_settings.currency_symbol}"
        show_toast = is_btn
        return balance, status_text, toast_msg, "Баланс обновлён", show_toast, "success"

    @app.callback(
        Output("inventory-store", "data"),
        Output("inventory-load-status", "children"),
        Output("app-toast", "children", allow_duplicate=True),
        Output("app-toast", "header", allow_duplicate=True),
        Output("app-toast", "is_open", allow_duplicate=True),
        Output("app-toast", "icon", allow_duplicate=True),
        Input("startup-interval", "n_intervals"),
        Input("inventory-load-btn", "n_clicks"),
        prevent_initial_call="initial_duplicate",
    )
    def load_inventory(n_intervals: Any, n_clicks: Any) -> Any:
        from ui.inventory import fetch_inventory

        is_btn = callback_context.triggered_id == "inventory-load-btn"
        steam_id = _settings.steam_id.strip()
        if not steam_id:
            status = html.Span(
                "Добавь STEAM_ID= в .env", style={"color": _MUTED, "fontSize": "12px"}
            )
            return None, status, "Добавь STEAM_ID= в .env", "Нет Steam ID", is_btn, "warning"

        try:
            items = fetch_inventory(steam_id)
        except Exception as exc:
            status = html.Span(f"Ошибка: {exc}", style={"color": _RED, "fontSize": "12px"})
            return (
                None,
                status,
                f"Ошибка загрузки инвентаря: {exc}",
                "Ошибка инвентаря",
                is_btn,
                "danger",
            )

        count = len(items) if items else 0
        status = html.Span(
            f"Загружено {count} предм." if count else "Инвентарь пуст",
            style={"color": _GREEN if count else _MUTED, "fontSize": "12px"},
        )
        toast_msg = f"Инвентарь: {count} предметов"
        return items, status, toast_msg, "Инвентарь обновлён", is_btn, "success"

    @app.callback(
        Output("tab-content", "children"),
        Input("main-tabs", "value"),
        Input("selected-cid", "data"),
        Input("invest-store", "data"),
        Input("raw-items-store", "data"),
        Input("inventory-store", "data"),
        Input("portfolio-balance", "data"),
        Input("balance-refresh-store", "data"),
        Input("price-count-store", "data"),
        Input("inventory-show-all", "value"),
    )
    def render_tab(
        tab: Any,
        container_id: Any,
        invest: Any,
        raw_items: Any,
        inventory_data: Any,
        portfolio_balance: Any,
        _balance_refresh: Any,
        price_count: Any,
        show_all_inventory: Any,
    ) -> Any:
        invest = invest or {}
        raw_items = raw_items or []

        # ── F-02: cold-start banner ───────────────────────────────────────────
        cold_start_banner = None
        if price_count is not None and price_count == 0:
            cold_start_banner = dbc.Alert(
                "Данные ещё не загружены. Запустите cs2 backfill чтобы загрузить историю цен.",
                color="info",
                style={"fontSize": "13px", "marginBottom": "12px"},
                dismissable=True,
            )

        def _wrap(content: Any) -> Any:
            if cold_start_banner is not None:
                return html.Div([cold_start_banner, content])
            return content

        def _safe(fn, *args, **kwargs):
            """Call fn(*args, **kwargs); return an error card on any exception."""
            try:
                return fn(*args, **kwargs)
            except Exception:
                tb = traceback.format_exc()
                logger.exception("render_tab: unhandled error in %s", getattr(fn, "__name__", fn))
                return html.Div(
                    [
                        html.H3(
                            "Ошибка рендеринга страницы",
                            style={"color": "#ff4444", "fontSize": "14px", "marginBottom": "8px"},
                        ),
                        html.Pre(
                            tb,
                            style={
                                "backgroundColor": "#2a1010",
                                "border": "1px solid #ff4444",
                                "borderRadius": "4px",
                                "padding": "12px",
                                "fontSize": "11px",
                                "color": "#ffaaaa",
                                "whiteSpace": "pre-wrap",
                                "overflowX": "auto",
                            },
                        ),
                    ]
                )

        if tab == "market":
            return _wrap(_safe(_render_market, container_id, invest, raw_items, inventory_data))
        if tab == "inventory":
            return _wrap(_safe(_render_inventory, inventory_data, invest, show_all=bool(show_all_inventory)))
        if tab == "portfolio":
            return _wrap(_safe(_render_portfolio, portfolio_balance, inventory_data, invest))
        if tab == "balance":
            return _wrap(_safe(_render_balance_tab, portfolio_balance, inventory_data))
        if tab == "analytics":
            return _wrap(_safe(_render_analytics, selected_container_id=container_id))
        if tab == "system":
            from ui.renderers.system_status import render_system_status
            return render_system_status(health=_get_system_health())
        return _no_data()

    # ── System Status: refresh on task completion or manual button ───────────
    @app.callback(
        Output("tab-content", "children", allow_duplicate=True),
        Input("task-done-ts", "data"),
        Input("btn-refresh-system", "n_clicks"),
        State("main-tabs", "value"),
        prevent_initial_call=True,
    )
    def refresh_health_tab(_ts: Any, _n: Any, tab: Any) -> Any:
        if tab != "system":
            raise dash.exceptions.PreventUpdate
        from ui.renderers.system_status import render_system_status
        return render_system_status(health=_get_system_health())

    # ── Worker progress: live poll while worker is busy ───────────────────────
    @app.callback(
        Output("worker-progress-section", "children"),
        Output("worker-progress-interval", "disabled"),
        Input("worker-progress-interval", "n_intervals"),
        prevent_initial_call=False,
    )
    def update_worker_progress(_n: Any) -> Any:
        import requests as _req

        from config import settings as _s
        from ui.renderers.system_status import _render_progress
        try:
            r = _req.get(
                f"http://{_s.api_internal_host}:{_s.api_port}/api/v1/system/queue-status",
                timeout=2,
            )
            state = r.json() if r.ok else {}
        except Exception:
            state = {}
        idle = not state.get("busy") and state.get("queue_size", 0) == 0
        return _render_progress(state), idle  # disable interval when idle

    # ── Sync Inventory (API endpoint) ────────────────────────────────────────
    @app.callback(
        Output("app-toast", "children", allow_duplicate=True),
        Output("app-toast", "header", allow_duplicate=True),
        Output("app-toast", "is_open", allow_duplicate=True),
        Output("app-toast", "icon", allow_duplicate=True),
        Input("btn-sync-inventory", "n_clicks"),
        running=[
            (Output("btn-sync-inventory", "disabled"), True, False),
            (Output("btn-sync-inventory", "children"), [dbc.Spinner(size="sm"), " Запуск…"], "Sync Inventory"),
        ],
        prevent_initial_call=True,
    )
    def do_sync_inventory(n: Any) -> tuple:
        if not n:
            raise dash.exceptions.PreventUpdate
        import requests as _req
        try:
            r = _req.post(f"http://{_settings.api_internal_host}:{_settings.api_port}/api/v1/sync/inventory", timeout=5)
            data = r.json()
            if data.get("already_running"):
                return "Уже выполняется — дождись завершения.", "Инвентарь", True, "warning"
            if data.get("ok"):
                tid = (data.get("task_id") or "")[:8]
                return f"Задача запущена (ID: {tid}). Следи за прогрессом в Task History.", "Синхронизация инвентаря", True, "success"
            return data.get("message", "Неизвестная ошибка"), "Ошибка инвентаря", True, "danger"
        except Exception as exc:
            return str(exc), "Ошибка подключения", True, "danger"

    # ── Sync Market Catalog (Celery dispatch via API endpoint) ────────────────
    @app.callback(
        Output("app-toast", "children", allow_duplicate=True),
        Output("app-toast", "header", allow_duplicate=True),
        Output("app-toast", "is_open", allow_duplicate=True),
        Output("app-toast", "icon", allow_duplicate=True),
        Input("btn-sync-catalog", "n_clicks"),
        running=[
            (Output("btn-sync-catalog", "disabled"), True, False),
            (Output("btn-sync-catalog", "children"), [dbc.Spinner(size="sm"), " Запуск…"], "Sync Catalog"),
        ],
        prevent_initial_call=True,
    )
    def do_sync_catalog(n: Any) -> tuple:
        if not n:
            raise dash.exceptions.PreventUpdate
        import requests as _req
        try:
            r = _req.post(f"http://{_settings.api_internal_host}:{_settings.api_port}/api/v1/sync/market/catalog", timeout=5)
            data = r.json()
            if data.get("already_running"):
                return "Уже выполняется — дождись завершения.", "Каталог", True, "warning"
            if data.get("ok"):
                tid = (data.get("task_id") or "")[:8]
                return f"Задача запущена (ID: {tid}). Следи за прогрессом в Task History.", "Sync Catalog", True, "success"
            return data.get("message", "Неизвестная ошибка"), "Ошибка каталога", True, "danger"
        except Exception as exc:
            return str(exc), "Ошибка подключения", True, "danger"

    # ── Sync Market Prices (Celery dispatch via API endpoint) ─────────────────
    @app.callback(
        Output("app-toast", "children", allow_duplicate=True),
        Output("app-toast", "header", allow_duplicate=True),
        Output("app-toast", "is_open", allow_duplicate=True),
        Output("app-toast", "icon", allow_duplicate=True),
        Input("btn-sync-prices", "n_clicks"),
        State("system-type-filter", "value"),
        running=[
            (Output("btn-sync-prices", "disabled"), True, False),
            (Output("btn-sync-prices", "children"), [dbc.Spinner(size="sm"), " Запуск…"], "Sync Prices"),
        ],
        prevent_initial_call=True,
    )
    def do_sync_prices(n: Any, type_filter: Any) -> tuple:
        if not n:
            raise dash.exceptions.PreventUpdate
        import requests as _req
        try:
            body = {"container_type": type_filter or ""}
            r = _req.post(f"http://{_settings.api_internal_host}:{_settings.api_port}/api/v1/sync/market/prices", json=body, timeout=5)
            data = r.json()
            if data.get("already_running"):
                return "Уже выполняется — дождись завершения.", "Цены", True, "warning"
            if data.get("ok"):
                tid = (data.get("task_id") or "")[:8]
                return f"Задача запущена (ID: {tid}). Следи за прогрессом в Task History.", "Sync Prices", True, "success"
            return data.get("message", "Неизвестная ошибка"), "Ошибка цен", True, "danger"
        except Exception as exc:
            return str(exc), "Ошибка подключения", True, "danger"

    @app.callback(
        Output("app-toast", "children", allow_duplicate=True),
        Output("app-toast", "header", allow_duplicate=True),
        Output("app-toast", "is_open", allow_duplicate=True),
        Output("app-toast", "icon", allow_duplicate=True),
        Input("btn-backfill-active", "n_clicks"),
        running=[
            (Output("btn-backfill-active", "disabled"), True, False),
            (Output("btn-backfill-active", "children"), [dbc.Spinner(size="sm"), " Запуск…"], "Backfill Active"),
        ],
        prevent_initial_call=True,
    )
    def do_backfill_active(n: Any) -> tuple:
        if not n:
            raise dash.exceptions.PreventUpdate
        import requests as _req
        try:
            r = _req.post(f"http://{_settings.api_internal_host}:{_settings.api_port}/api/v1/sync/backfill/active", timeout=5)
            data = r.json()
            if data.get("ok"):
                return data.get("message", "Запущено"), "Backfill Active", True, "success"
            return data.get("message", "Нет открытых позиций"), "Backfill Active", True, "warning"
        except Exception as exc:
            return str(exc), "Ошибка подключения", True, "danger"

    @app.callback(
        Output("app-toast", "children", allow_duplicate=True),
        Output("app-toast", "header", allow_duplicate=True),
        Output("app-toast", "is_open", allow_duplicate=True),
        Output("app-toast", "icon", allow_duplicate=True),
        Input("btn-backfill-all", "n_clicks"),
        State("system-type-filter", "value"),
        running=[
            (Output("btn-backfill-all", "disabled"), True, False),
            (Output("btn-backfill-all", "children"), [dbc.Spinner(size="sm"), " Запуск…"], "Backfill All"),
        ],
        prevent_initial_call=True,
    )
    def do_backfill_all(n: Any, type_filter: Any) -> tuple:
        if not n:
            raise dash.exceptions.PreventUpdate
        import requests as _req
        try:
            body = {"container_type": type_filter or ""}
            r = _req.post(f"http://{_settings.api_internal_host}:{_settings.api_port}/api/v1/sync/backfill", json=body, timeout=5)
            data = r.json()
            if data.get("ok"):
                label = f"Backfill — {type_filter}" if type_filter else "Backfill All (~60–110 мин)"
                return data.get("message", "Запущено"), label, True, "success"
            return data.get("message", "Неизвестная ошибка"), "Ошибка", True, "warning"
        except Exception as exc:
            return str(exc), "Ошибка подключения", True, "danger"

    @app.callback(
        Output("health-action-msg", "children", allow_duplicate=True),
        Input("btn-clear-queue", "n_clicks"),
        running=[(Output("btn-clear-queue", "disabled"), True, False)],
        prevent_initial_call=True,
    )
    def do_clear_queue(n: Any) -> str:
        import requests as _req
        try:
            r = _req.post(
                f"http://{_settings.api_internal_host}:{_settings.api_port}/api/v1/system/cancel-task",
                json={"job_type": None},
                timeout=5,
            )
            data = r.json()
            removed = data.get("removed", 0)
            if data.get("ok"):
                return f"Очередь очищена — удалено {removed} задач."
            return f"Ошибка: {data.get('error', '?')}"
        except Exception as exc:
            return str(exc)

    @app.callback(
        Output("ping-steam-last", "children"),
        Output("app-toast", "children", allow_duplicate=True),
        Output("app-toast", "header", allow_duplicate=True),
        Output("app-toast", "is_open", allow_duplicate=True),
        Output("app-toast", "icon", allow_duplicate=True),
        Input("btn-ping-steam", "n_clicks"),
        running=[
            (Output("btn-ping-steam", "disabled"), True, False),
            (Output("btn-ping-steam", "children"), [dbc.Spinner(size="sm"), " Пинг…"], "Ping Steam"),
        ],
        prevent_initial_call=True,
    )
    def do_ping_steam(n: Any) -> tuple:
        if not n:
            raise dash.exceptions.PreventUpdate
        import requests as _req
        from ui.renderers.system_status import _ping_label
        try:
            r = _req.post(
                f"http://{_settings.api_internal_host}:{_settings.api_port}/api/v1/system/ping-steam",
                timeout=35,
            )
            data = r.json()
        except Exception as exc:
            label = [html.Span(f"⚠ {str(exc)[:80]}", style={"color": "#e07b39", "fontSize": "11px"})]
            return label, str(exc)[:100], "Ping Steam", True, "danger"

        status = data.get("status")
        label = _ping_label(data)
        if status == "ok":
            return label, "Steam доступен ✓", "Ping Steam", True, "success"
        if status == "blocked":
            blocked_until = data.get("blocked_until", "?")
            remaining_s = data.get("remaining_s", 0)
            if remaining_s >= 3600:
                r_str = f"{remaining_s // 3600}ч {(remaining_s % 3600) // 60}м"
            elif remaining_s >= 60:
                r_str = f"{remaining_s // 60} мин"
            else:
                r_str = f"{remaining_s} сек"
            return label, f"Blocked до {blocked_until} ({r_str})", "Steam заблокирован", True, "warning"
        if status == "no_credentials":
            return label, "Токен не настроен", "Ping Steam", True, "danger"
        return label, data.get("detail", "Ошибка запроса"), "Ping Steam", True, "danger"

    @app.callback(
        Output("steam-history-status", "children"),
        Output("balance-refresh-store", "data"),
        Output("app-toast", "children", allow_duplicate=True),
        Output("app-toast", "header", allow_duplicate=True),
        Output("app-toast", "is_open", allow_duplicate=True),
        Output("app-toast", "icon", allow_duplicate=True),
        Input("steam-history-load-btn", "n_clicks"),
        State("balance-refresh-store", "data"),
        prevent_initial_call=True,
    )
    def load_steam_history(n_clicks: Any, refresh_counter: Any) -> Any:
        import uuid as _uuid

        from scrapper.steam_transactions import compute_annual_pnl, fetch_market_history
        from src.domain.portfolio import upsert_annual

        transactions, msg = fetch_market_history(max_pages=10)

        if not transactions:
            if msg == "NO_COOKIE":
                display_msg = "Cookie не найден — запусти: cs2 cookie"
            elif "403" in msg:
                display_msg = msg
            elif "0 транзакций" in msg:
                display_msg = "Steam вернул 0 транзакций — проверь cookie или попробуй позже"
            else:
                display_msg = msg
            status = html.Span(display_msg, style={"color": _YELLOW, "fontSize": "11px"})
            return status, no_update, display_msg, "История Steam", True, "warning"

        from src.domain.connection import SessionLocal as _SL_hist
        from src.domain.models import FactTransaction

        db = _SL_hist()
        try:
            db.query(FactTransaction).filter(FactTransaction.notes.like("steam:%")).delete(
                synchronize_session=False
            )
            for tx in transactions:
                db.add(
                    FactTransaction(
                        id=str(_uuid.uuid4()),
                        trade_date=tx["date"],
                        action=tx["action"],
                        item_name=tx["item_name"],
                        quantity=1,
                        price=tx["price"],
                        total=tx["total"],
                        pnl=None,
                        listing_id=tx["listing_id"],
                        notes=f"steam:{tx['listing_id']}",
                    )
                )
            db.commit()
        except Exception as exc:
            db.rollback()
            logger.error("load_steam_history: DB write failed — %s", exc)
            status = html.Span(f"Ошибка БД: {exc}", style={"color": _RED, "fontSize": "11px"})
            return status, no_update, str(exc), "Ошибка сохранения", True, "danger"
        finally:
            db.close()

        annual = compute_annual_pnl(transactions)
        for year, pnl in annual.items():
            upsert_annual(year, pnl, notes="авто из Steam")

        sell_count = sum(1 for t in transactions if t["action"] == "SELL")
        buy_count = sum(1 for t in transactions if t["action"] == "BUY")
        status = html.Span(
            f"✓ {len(transactions)} сделок ({buy_count} купл. / {sell_count} прод.)",
            style={"color": _GREEN, "fontSize": "11px"},
        )
        toast_msg = (
            f"{len(transactions)} транзакций: {buy_count} покупок, {sell_count} продаж. "
            f"История лет обновлена ({len(annual)} год(а))."
        )
        return (
            status,
            (refresh_counter or 0) + 1,
            toast_msg,
            "История Steam загружена",
            True,
            "success",
        )

    # ── UX-13: Unified Sync button ─────────────────────────────────────────────

    @app.callback(
        Output("btn-sync-all", "color"),
        Output("btn-sync-all", "disabled"),
        Output("btn-sync-all", "children"),
        Output("app-toast", "children", allow_duplicate=True),
        Output("app-toast", "header", allow_duplicate=True),
        Output("app-toast", "is_open", allow_duplicate=True),
        Output("app-toast", "icon", allow_duplicate=True),
        Output("sync-state", "data"),
        Output("sync-reset-interval", "disabled"),
        Output("balance-refresh-store", "data", allow_duplicate=True),
        Output("cookie-status-badge", "children"),
        Output("cookie-status-badge", "style"),
        Input("btn-sync-all", "n_clicks"),
        prevent_initial_call=True,
    )
    def sync_all(n_clicks: Any) -> Any:
        """Trigger wallet + inventory + transactions sync simultaneously."""
        import uuid as _uuid

        from dash import html as _html

        from scrapper.steam_sync import sync_inventory as _sync_inv
        from scrapper.steam_sync import sync_transactions as _sync_tx
        from scrapper.steam_sync import sync_wallet as _sync_wal
        from src.domain.portfolio import upsert_annual

        _err_return = lambda msg: (
            "danger", False,
            [_html.I(className="fa fa-refresh me-1"), "Синхронизировать"],
            msg, "Критическая ошибка", True, "danger",
            no_update, True, no_update, no_update, no_update,
        )

        try:
            wallet = _sync_wal()
            inventory = _sync_inv(_settings.steam_id)
            transactions = _sync_tx()

            # Persist transactions to DB (same logic as load_steam_history)
            if transactions.ok and transactions.transactions:
                from src.domain.connection import SessionLocal as _SL_tx
                from src.domain.models import FactTransaction

                db = _SL_tx()
                try:
                    db.query(FactTransaction).filter(FactTransaction.notes.like("steam:%")).delete(
                        synchronize_session=False
                    )
                    for tx in transactions.transactions:
                        db.add(
                            FactTransaction(
                                id=str(_uuid.uuid4()),
                                trade_date=tx["date"],
                                action=tx["action"],
                                item_name=tx["item_name"],
                                quantity=1,
                                price=tx["price"],
                                total=tx["total"],
                                pnl=None,
                                listing_id=tx["listing_id"],
                                notes=f"steam:{tx['listing_id']}",
                            )
                        )
                    db.commit()
                except Exception as exc:
                    db.rollback()
                    logger.error("sync_all: DB write failed — %s", exc)
                finally:
                    db.close()

                for year, pnl in transactions.annual_pnl.items():
                    upsert_annual(year, pnl, notes="авто из Steam")

            # CACHE-1: recompute and persist portfolio advice + investment signals
            try:
                from infra.cache_writer import refresh_cache as _refresh_cache
                from src.domain.connection import SessionLocal as _SL

                _cache_db = _SL()
                try:
                    _refresh_cache(_cache_db)
                    _cache_db.commit()
                    logger.info("sync_all: cache refreshed.")
                except Exception as _cache_exc:
                    _cache_db.rollback()
                    logger.warning("sync_all: cache refresh failed — %s", _cache_exc)
                finally:
                    _cache_db.close()
            except Exception as _outer_exc:
                logger.warning("sync_all: cache refresh import failed — %s", _outer_exc)

            ok_count = sum([wallet.ok, inventory.ok, transactions.ok])
            all_ok = ok_count == 3
            all_fail = ok_count == 0

            # Build toast parts
            parts = []
            if wallet.ok:
                parts.append(
                    f"✓ Баланс: {int(wallet.balance or 0):,} {_settings.currency_symbol}"
                )
            else:
                parts.append(f"✗ Баланс: {wallet.message}")
            if inventory.ok:
                parts.append(f"✓ Инвентарь: {inventory.count} пред.")
            else:
                parts.append(f"✗ Инвентарь: {inventory.message}")
            if transactions.ok:
                tx_total = transactions.buy_count + transactions.sell_count
                parts.append(f"✓ Транзакции: {tx_total} записей")
            else:
                parts.append(f"✗ Транзакции: {transactions.message}")

            toast_body = "  |  ".join(parts)

            if all_ok:
                toast_hdr = "Синхронизация завершена"
                toast_icon = "success"
                btn_color = "success"
            elif all_fail:
                toast_hdr = "Ошибка синхронизации"
                toast_icon = "danger"
                btn_color = "danger"
            else:
                toast_hdr = "Синхронизация частично завершена"
                toast_icon = "warning"
                btn_color = "warning"

            btn_label = [_html.I(className="fa fa-refresh me-1"), "Синхронизировать"]
            sync_data = {"ts": datetime.now(UTC).isoformat(), "status": "done"}
            balance_refresh = datetime.now(UTC).isoformat() if transactions.ok else no_update

            # PV-07/PV-13: cookie expiry badge + DB status sync (PV-43)
            _AUTH_ERRORS = {"NO_COOKIE", "STALE_COOKIE"}
            _cookie_expired = (
                wallet.error_code in _AUTH_ERRORS
                or transactions.error_code in _AUTH_ERRORS
            )
            if _cookie_expired:
                _badge_children = [
                    _html.I(className="fa fa-exclamation-triangle me-1"),
                    "Steam Session Expired. Update auth cookie.",
                ]
                _badge_style = {
                    "color": _RED,
                    "fontSize": "11px",
                    "fontWeight": "bold",
                    "paddingTop": "8px",
                    "paddingRight": "12px",
                    "display": "inline",
                }
                # Write EXPIRED to DB so the cookie modal (PV-43) can open
                try:
                    from src.domain.connection import SessionLocal as _SL2
                    from src.domain.sql_repositories import set_cookie_status as _set_cs
                    with _SL2() as _csdb:
                        _set_cs(_csdb, "EXPIRED")
                        _csdb.commit()
                except Exception as _cs_exc:
                    logger.warning("sync_all: could not write cookie status to DB: %s", _cs_exc)
            else:
                _badge_children = ""
                _badge_style = {"display": "none"}

            return (
                btn_color,
                False,
                btn_label,
                toast_body,
                toast_hdr,
                True,
                toast_icon,
                sync_data,
                False,
                balance_refresh,
                _badge_children,
                _badge_style,
            )

        except Exception as _exc:  # global guard — any unhandled crash → Toast
            logger.exception("sync_all: unhandled error — %s", _exc)
            return _err_return(f"Ошибка: {type(_exc).__name__}: {_exc}")

    @app.callback(
        Output("btn-sync-all", "color", allow_duplicate=True),
        Output("sync-reset-interval", "disabled", allow_duplicate=True),
        Input("sync-reset-interval", "n_intervals"),
        State("sync-state", "data"),
        prevent_initial_call=True,
    )
    def reset_sync_button_color(n_intervals: Any, sync_data: Any) -> Any:
        """Reset the Sync button to secondary color 2s after completion."""
        return "secondary", True

    @app.callback(
        Output("scheduler-badge", "children"),
        Output("scheduler-badge", "style"),
        Output("emergency-block-badge", "children"),
        Output("emergency-block-badge", "style"),
        Input("task-done-ts", "data"),
        Input("startup-interval", "n_intervals"),
        prevent_initial_call=False,
    )
    def update_header_badges(_ts: Any, _startup: Any) -> Any:
        import os

        import redis as _redis_lib

        _badge_base = {"fontSize": "11px", "paddingTop": "8px", "paddingRight": "12px"}
        _hidden = {**_badge_base, "display": "none"}

        sched_running = False
        is_blocked = False

        try:
            _url = os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0")
            r = _redis_lib.from_url(_url, socket_connect_timeout=1, decode_responses=True)
            r.ping()
            sched_running = True

            # Emergency block: STEALTH_BLOCK_EXPIRES key present → workers paused
            is_blocked = r.exists("STEALTH_BLOCK_EXPIRES") > 0
        except Exception:
            pass

        sched_style = {**_badge_base, "color": _GREEN if sched_running else _MUTED}
        sched_label = "Beat: Active" if sched_running else "Beat: Offline"

        block_label = "Emergency Block" if is_blocked else ""
        block_style = {**_badge_base, "color": _RED, "fontWeight": "bold"} if is_blocked else _hidden

        return sched_label, sched_style, block_label, block_style

    # ── F-09: Armory Pass calculator callback ─────────────────────────────────
    @app.callback(
        Output("ap-result-output", "children"),
        Input("ap-container-dropdown", "value"),
        Input("ap-pass-cost-input", "value"),
        Input("ap-stars-in-pass-input", "value"),
        Input("ap-stars-per-case-input", "value"),
        prevent_initial_call=True,
    )
    def update_armory_pass_result(
        container_name: str | None,
        pass_cost: float | None,
        stars_in_pass: int | None,
        stars_per_case: int | None,
    ) -> Any:
        from src.domain.armory_pass import compare_armory_pass

        if not container_name or pass_cost is None:
            return html.Span(
                "Введи цену Armory Pass и выбери контейнер для расчёта.",
                style={"color": _MUTED, "fontSize": "12px"},
            )

        price_now = _get_current_steam_prices()
        pd_entry = price_now.get(container_name, {})
        market_price = pd_entry.get("current_price")

        if not market_price:
            return html.Span(
                f"Нет рыночной цены для «{container_name}» — обнови данные.",
                style={"color": _YELLOW, "fontSize": "12px"},
            )

        sip = int(stars_in_pass) if stars_in_pass else 5
        spc = int(stars_per_case) if stars_per_case else 1
        if spc > sip:
            return html.Span(
                "Звёзд за контейнер не может превышать звёзд в пассе.",
                style={"color": _RED, "fontSize": "12px"},
            )

        try:
            result = compare_armory_pass(
                container_name=container_name,
                market_price=float(market_price),
                pass_cost=float(pass_cost),
                stars_in_pass=sip,
                stars_per_case=spc,
                steam_fee_divisor=_FEE_DIV,
                steam_fee_fixed=_FEE_FIXED,
            )
        except ValueError as exc:
            return html.Span(str(exc), style={"color": _RED, "fontSize": "12px"})

        rec_color = _GREEN if result.recommendation == "MARKET" else _ORANGE
        signal_color = {"SELL": _GREEN, "WAIT": _YELLOW, "AVOID": _RED}.get(result.sell_signal, _MUTED)
        return html.Div(
            [
                html.Span(
                    result.recommendation,
                    style={
                        "color": rec_color,
                        "fontWeight": "bold",
                        "fontSize": "14px",
                        "marginRight": "12px",
                    },
                ),
                html.Span(
                    f"Сигнал: {result.sell_signal}",
                    style={"color": signal_color, "fontWeight": "bold", "fontSize": "13px", "marginRight": "12px"},
                ),
                html.Span(
                    f"Листингуй от: {int(result.breakeven_listing_price):,}{_settings.currency_symbol}",
                    style={"color": _TEXT, "fontSize": "12px", "marginRight": "12px"},
                ),
                html.Span(result.message, style={"color": _TEXT, "fontSize": "12px"}),
                html.Div(
                    [
                        html.Span(
                            f"Цена рынка: {int(result.market_price):,}{_settings.currency_symbol}  |  "
                            f"Нетто с рынка: {int(result.net_market_proceeds.amount if isinstance(result.net_market_proceeds, Amount) else result.net_market_proceeds):,}{_settings.currency_symbol}  |  "
                            f"Стоимость через пасс: {int(result.effective_pass_cost.amount if isinstance(result.effective_pass_cost, Amount) else result.effective_pass_cost):,}{_settings.currency_symbol}",
                            style={"color": _MUTED, "fontSize": "11px"},
                        )
                    ],
                    style={"marginTop": "4px"},
                ),
            ]
        )

    @app.callback(
        Output("last-sync-indicator", "children"),
        Output("btn-sync-all", "disabled", allow_duplicate=True),
        Output("btn-sync-all", "children", allow_duplicate=True),
        Input("task-done-ts", "data"),
        Input("sync-state", "data"),
        prevent_initial_call="initial_duplicate",
    )
    def update_sync_status(_ts: Any, sync_data: Any) -> Any:
        """Show last sync age; check worker busy state via API."""
        import requests as _req
        from dash import html as _html

        from config import settings as _s
        from src.domain.connection import SessionLocal
        from src.domain.models import SystemSettings

        # Check if worker is busy
        worker_busy = False
        try:
            r = _req.get(
                f"http://{_s.api_internal_host}:{_s.api_port}/api/v1/system/queue-status",
                timeout=2,
            )
            state = r.json() if r.ok else {}
            worker_busy = bool(state.get("busy")) or state.get("queue_size", 0) > 0
        except Exception:
            pass

        last_times = []
        try:
            with SessionLocal() as db:
                for task_type in ["sync_inventory", "sync_transactions"]:
                    row = db.get(SystemSettings, f"last_sync_{task_type}")
                    if row and row.value:
                        try:
                            last_times.append(datetime.fromisoformat(row.value))
                        except ValueError:
                            pass
        except Exception:
            pass

        if worker_busy:
            btn_label = [dbc.Spinner(size="sm", color="light"), " Sync in progress..."]
            return "Sync in progress...", True, btn_label

        if last_times:
            oldest = min(last_times)
            age_min = int((datetime.now(UTC).replace(tzinfo=None) - oldest).total_seconds() / 60)
            age_str = f"Last sync: {age_min} min ago" if age_min < 60 else f"Last sync: {age_min // 60}h ago"
        else:
            age_str = "Last sync: never"

        btn_label = [_html.I(className="fa fa-refresh me-1"), "Синхронизировать"]
        return age_str, False, btn_label

    # ── Per-container: refresh price ─────────────────────────────────────────────
    @app.callback(
        Output("app-toast", "children", allow_duplicate=True),
        Output("app-toast", "header", allow_duplicate=True),
        Output("app-toast", "is_open", allow_duplicate=True),
        Output("app-toast", "icon", allow_duplicate=True),
        Input({"type": "btn-refresh-price", "index": ALL}, "n_clicks"),
        prevent_initial_call=True,
    )
    def do_refresh_container_price(n_clicks_list: Any) -> Any:
        ctx = callback_context
        if not ctx.triggered:
            raise dash.exceptions.PreventUpdate
        triggered = [t for t in ctx.triggered if t.get("value") and t["value"] > 0]
        if not triggered:
            raise dash.exceptions.PreventUpdate
        prop_id = triggered[0]["prop_id"]
        try:
            cid = _json.loads(prop_id.rsplit(".", 1)[0])["index"]
        except Exception:
            raise dash.exceptions.PreventUpdate
        import requests as _req
        try:
            r = _req.post(f"http://{_settings.api_internal_host}:{_settings.api_port}/api/v1/containers/{cid}/sync-price", timeout=5)
            data = r.json()
            if data.get("ok"):
                tid = (data.get("task_id") or "")[:8]
                return f"Задача запущена (ID: {tid}). Результат появится в Task History.", "Обновление цены", True, "success"
            return data.get("message", "Неизвестная ошибка"), "Ошибка", True, "danger"
        except Exception as exc:
            return str(exc), "Ошибка подключения", True, "danger"

    # ── Stop task ────────────────────────────────────────────────────────────────
    @app.callback(
        Output("tab-content", "children", allow_duplicate=True),
        Output("toast-store", "data", allow_duplicate=True),
        Input({"type": "btn-stop-task", "index": ALL}, "n_clicks"),
        State("toast-store", "data"),
        prevent_initial_call=True,
    )
    def do_stop_task(n_clicks_list: Any, store: Any) -> Any:
        ctx = callback_context
        if not ctx.triggered:
            raise dash.exceptions.PreventUpdate
        triggered = [t for t in ctx.triggered if t.get("value") and t["value"] > 0]
        if not triggered:
            raise dash.exceptions.PreventUpdate

        prop_id = triggered[0]["prop_id"]
        try:
            job_type: str | None = _json.loads(prop_id.rsplit(".", 1)[0])["index"]
        except Exception:
            job_type = None

        import requests as _req
        try:
            _req.post(
                f"http://{_settings.api_internal_host}:{_settings.api_port}/api/v1/system/cancel-task",
                json={"job_type": job_type},
                timeout=5,
            )
        except Exception as exc:
            logger.warning("do_stop_task: cancel API error — %s", exc)

        # Keep only already-dismissed toasts (is_open=False); drop all active ones.
        # In a single-worker system any active notification belongs to the cancelled job.
        cleared_store = [e for e in (store or []) if not e.get("is_open", True)]

        from ui.renderers.system_status import render_system_status
        return render_system_status(health=_get_system_health()), cleared_store

    # ── Toast-store: force-clear on page load (prevents phantom toasts after F5) ─
    @app.callback(
        Output("toast-store", "data", allow_duplicate=True),
        Input("startup-interval", "n_intervals"),
        prevent_initial_call=True,
    )
    def _init_toast_store(_n: Any) -> Any:
        return []

    # ── System tab: unblacklist from blacklisted section ────────────────────────
    @app.callback(
        Output("tab-content", "children", allow_duplicate=True),
        Input({"type": "btn-unblacklist", "index": ALL}, "n_clicks"),
        prevent_initial_call=True,
    )
    def do_unblacklist_from_system(n_clicks_list: Any) -> Any:
        ctx = callback_context
        if not ctx.triggered:
            raise dash.exceptions.PreventUpdate
        triggered = [t for t in ctx.triggered if t.get("value") and t["value"] > 0]
        if not triggered:
            raise dash.exceptions.PreventUpdate
        prop_id = triggered[0]["prop_id"]
        try:
            cid = _json.loads(prop_id.rsplit(".", 1)[0])["index"]
        except Exception:
            raise dash.exceptions.PreventUpdate
        try:
            from src.domain.connection import SessionLocal
            from src.domain.models import DimContainer
            with SessionLocal() as db:
                c = db.query(DimContainer).filter(DimContainer.container_id == cid).first()
                if c:
                    c.is_blacklisted = 0
                    db.commit()
        except Exception as exc:
            logger.error("do_unblacklist_from_system: %s", exc)
        from ui.renderers.system_status import render_system_status
        return render_system_status(health=_get_system_health())

    # ── Scrape sessions: resume / delete ─────────────────────────────────────────
    @app.callback(
        Output("tab-content", "children", allow_duplicate=True),
        Output("session-action-msg", "children"),
        Input({"type": "btn-session-resume", "index": ALL}, "n_clicks"),
        Input({"type": "btn-session-delete", "index": ALL}, "n_clicks"),
        prevent_initial_call=True,
    )
    def do_session_action(resume_clicks: Any, delete_clicks: Any) -> Any:
        ctx = callback_context
        triggered = [t for t in ctx.triggered if t.get("value") and t["value"] > 0]
        if not triggered:
            raise dash.exceptions.PreventUpdate
        prop_id = triggered[0]["prop_id"]
        import json as _json2
        try:
            info = _json2.loads(prop_id.rsplit(".", 1)[0])
            session_id = info["index"]
            btn_type = info["type"]
        except Exception:
            raise dash.exceptions.PreventUpdate

        import requests as _req

        from config import settings as _s
        base = f"http://{_s.api_internal_host}:{_s.api_port}/api/v1/scrape-sessions"
        try:
            if btn_type == "btn-session-resume":
                r = _req.post(f"{base}/{session_id}/resume", timeout=5)
                msg = r.json().get("message", "OK")
            else:
                r = _req.delete(f"{base}/{session_id}", timeout=5)
                msg = "Сессия удалена."
        except Exception as exc:
            msg = str(exc)

        from ui.renderers.system_status import render_system_status
        return render_system_status(health=_get_system_health()), msg

    # ── Per-container: toggle blacklist ──────────────────────────────────────────
    @app.callback(
        Output("invest-store", "data", allow_duplicate=True),
        Input({"type": "btn-toggle-blacklist", "index": ALL}, "n_clicks"),
        prevent_initial_call=True,
    )
    def do_toggle_blacklist(n_clicks_list: Any) -> Any:
        ctx = callback_context
        if not ctx.triggered:
            raise dash.exceptions.PreventUpdate
        triggered = [t for t in ctx.triggered if t.get("value") and t["value"] > 0]
        if not triggered:
            raise dash.exceptions.PreventUpdate
        prop_id = triggered[0]["prop_id"]
        try:
            cid = _json.loads(prop_id.rsplit(".", 1)[0])["index"]
        except Exception:
            raise dash.exceptions.PreventUpdate
        try:
            from src.domain.connection import SessionLocal
            from src.domain.models import DimContainer
            with SessionLocal() as db:
                c = db.query(DimContainer).filter(DimContainer.container_id == cid).first()
                if c is None:
                    raise dash.exceptions.PreventUpdate
                c.is_blacklisted = 0 if c.is_blacklisted else 1
                db.commit()
        except dash.exceptions.PreventUpdate:
            raise
        except Exception as exc:
            logger.error("do_toggle_blacklist: %s", exc)
            raise dash.exceptions.PreventUpdate
        # Re-fetch signals to trigger sidebar re-render
        from src.domain.item_service import ItemService
        svc = ItemService.open()
        try:
            return svc.get_signals()
        finally:
            svc.close()

    # ── Toast stack: aggregate → store → render ───────────────────────────────

    def _push_toast(store: list, text: Any, header: str, icon: str) -> list:
        import time as _t
        entry = {
            "id": int(_t.time() * 1000),
            "text": text,
            "header": header or "",
            "icon": icon or "info",
            "is_open": True,
        }
        return (list(store or []) + [entry])[-5:]

    @app.callback(
        Output("toast-store", "data"),
        Input("app-toast", "is_open"),
        State("app-toast", "children"),
        State("app-toast", "header"),
        State("app-toast", "icon"),
        State("toast-store", "data"),
        prevent_initial_call=True,
    )
    def _aggregate_toast(is_open: Any, text: Any, header: Any, icon: Any, store: Any) -> Any:
        if not is_open:
            raise dash.exceptions.PreventUpdate
        return _push_toast(store or [], text, header or "", icon or "info")

    @app.callback(
        Output("toast-store", "data", allow_duplicate=True),
        Input({"type": "notif", "index": ALL}, "is_open"),
        State("toast-store", "data"),
        prevent_initial_call=True,
    )
    def _sync_toast_dismissals(is_open_list: Any, store: Any) -> Any:
        if not any(v is False for v in (is_open_list or [])):
            raise dash.exceptions.PreventUpdate
        ctx = callback_context
        closed_ids: set[str] = set()
        for t in ctx.triggered:
            if t["value"] is False:
                try:
                    closed_ids.add(str(_json.loads(t["prop_id"].rsplit(".", 1)[0])["index"]))
                except Exception:
                    pass
        if not closed_ids:
            raise dash.exceptions.PreventUpdate
        updated = [
            {**e, "is_open": False} if str(e["id"]) in closed_ids else e
            for e in (store or [])
        ]
        if updated == store:
            raise dash.exceptions.PreventUpdate
        return updated

    @app.callback(
        Output("toast-stack-container", "children"),
        Input("toast-store", "data"),
        prevent_initial_call=False,
    )
    def _render_toast_stack(store: Any) -> Any:
        if not store:
            return []
        _ICON_BORDER = {
            "success": "#2a9d5c",
            "danger":  "#dc3545",
            "warning": "#ffc107",
            "info":    _BLUE,
        }
        return [
            dbc.Toast(
                entry.get("text", ""),
                id={"type": "notif", "index": entry["id"]},
                header=entry.get("header", ""),
                is_open=entry.get("is_open", True),
                dismissable=True,
                duration=3000,
                icon=entry.get("icon", "info"),
                style={
                    "backgroundColor": _BG,
                    "border": f"1px solid {_ICON_BORDER.get(entry.get('icon', ''), _BORDER)}",
                    "color": _TEXT,
                    "fontSize": "13px",
                    "width": "320px",
                    "boxShadow": "0 4px 12px rgba(0,0,0,0.4)",
                },
            )
            for entry in store
        ]

    _register_auth_modal_callbacks(app)
    _register_cookie_callbacks(app)


# ─── Auth-Pause Modal ─────────────────────────────────────────────────────────


def _register_auth_modal_callbacks(app: dash.Dash) -> None:

    @app.callback(
        Output("auth-modal", "is_open"),
        Input("auth-check-interval", "n_intervals"),
        State("auth-modal", "is_open"),
        prevent_initial_call=False,
    )
    def check_auth_pause(_n: Any, is_open: Any) -> Any:
        """Open the auth modal when the worker enters PAUSED_AUTH; keep it open until resolved."""
        if is_open:
            # Already open — don't interfere; submit callback closes it
            raise dash.exceptions.PreventUpdate
        import requests as _req
        try:
            r = _req.get(
                f"http://{_settings.api_internal_host}:{_settings.api_port}/api/v1/system/queue-status",
                timeout=2,
            )
            state = r.json() if r.ok else {}
            return bool(state.get("auth_paused", False))
        except Exception:
            return no_update

    @app.callback(
        Output("auth-modal", "is_open", allow_duplicate=True),
        Output("auth-login-secure-input", "value"),
        Output("auth-session-id-input", "value"),
        Output("auth-modal-status", "children"),
        Output("app-toast", "children", allow_duplicate=True),
        Output("app-toast", "header", allow_duplicate=True),
        Output("app-toast", "is_open", allow_duplicate=True),
        Output("app-toast", "icon", allow_duplicate=True),
        Input("auth-submit-btn", "n_clicks"),
        State("auth-login-secure-input", "value"),
        State("auth-session-id-input", "value"),
        prevent_initial_call=True,
    )
    def submit_auth_credentials(n_clicks: Any, login_secure: Any, session_id: Any) -> Any:
        _err = lambda msg: (True, no_update, no_update, msg, no_update, no_update, False, no_update)

        if not n_clicks:
            raise dash.exceptions.PreventUpdate

        login_secure = (login_secure or "").strip()
        session_id   = (session_id   or "").strip()

        if not login_secure:
            return _err("steamLoginSecure не может быть пустым.")
        if not session_id:
            return _err("Session ID не может быть пустым.")

        import requests as _req
        try:
            r = _req.post(
                f"http://{_settings.api_internal_host}:{_settings.api_port}/api/v1/auth/steam",
                json={"steamLoginSecure": login_secure, "session_id": session_id},
                timeout=10,
            )
            data = r.json()
            if not data.get("ok"):
                return _err(f"Ошибка: {data.get('detail', 'неизвестная ошибка')}")
        except Exception as exc:
            return _err(f"Ошибка подключения к API: {exc}")

        return (
            False,   # close modal
            "",      # clear steamLoginSecure input
            "",      # clear session_id input
            "",      # clear status message
            "Учётные данные Steam сохранены. Воркер возобновит работу автоматически.",
            "Авторизация обновлена",
            True,
            "success",
        )


# ─── PV-43: Cookie Hot-Swap Modal ─────────────────────────────────────────────


def _register_cookie_callbacks(app: dash.Dash) -> None:
    @app.callback(
        Output("cookie-modal", "is_open"),
        Input("cookie-status-interval", "n_intervals"),
        Input("cookie-close-btn", "n_clicks"),
        State("cookie-modal", "is_open"),
        prevent_initial_call=False,  # fire on load to show modal immediately if EXPIRED
    )
    def check_cookie_status(n_intervals, close_clicks, is_open):
        """Poll cookie status; open modal when EXPIRED."""
        from dash import ctx
        if ctx.triggered_id == "cookie-close-btn":
            logger.debug("check_cookie_status: user closed modal")
            return False

        cookie_expired = False
        try:
            import requests
            resp = requests.get(
                f"http://{_settings.api_internal_host}:{_settings.api_port}/api/v1/system/cookie-status",
                timeout=3,
            )
            api_status = resp.json().get("status") if resp.ok else "ERROR"
            cookie_expired = api_status == "EXPIRED"
            logger.info("check_cookie_status: api_status=%r cookie_expired=%s", api_status, cookie_expired)
        except Exception as exc:
            logger.warning("check_cookie_status: API unreachable — %s", exc)

        return cookie_expired

    @app.callback(
        Output("cookie-modal", "is_open", allow_duplicate=True),
        Output("cookie-update-status", "children"),
        Input("cookie-submit-btn", "n_clicks"),
        State("cookie-input", "value"),
        State("sessionid-input", "value"),
        State("session-note-input", "value"),
        prevent_initial_call=True,
    )
    def submit_new_cookie(n_clicks, cookie_value, sessionid_value, session_note):
        """POST new cookie + sessionid + session note to API; close modal on success."""
        if not n_clicks or not cookie_value:
            raise dash.exceptions.PreventUpdate
        try:
            import requests
            resp = requests.post(
                f"http://{_settings.api_internal_host}:{_settings.api_port}/api/v1/system/update-cookie",
                json={
                    "value": cookie_value,
                    "sessionid": sessionid_value or "",
                    "session_note": session_note or "",
                },
                timeout=15,
            )
            data = resp.json()
            if data.get("ok"):
                reset = data.get("reset_tasks", 0)
                workers = data.get("workers_released", 0)
                return False, f"Кука обновлена. Задач восстановлено: {reset}, воркеров разблокировано: {workers}"
            else:
                err = data.get("error", "Неизвестная ошибка")
                return True, f"Ошибка: {err}"
        except Exception as exc:
            return True, f"Ошибка подключения: {exc}"
