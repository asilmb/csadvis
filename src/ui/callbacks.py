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

import dash_bootstrap_components as dbc
import dash
from dash import ALL, Input, Output, State, callback_context, html, no_update
from config import settings as _settings
from src.domain.value_objects import Amount
from src.domain.event_calendar import get_event_signals as _get_ev_signals
from ui.helpers import (
    _BG,
    _BG3,
    _BG_SEL,
    _BLUE,
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


def _render_balance_tab(wallet_balance: Any, inventory_data: Any) -> html.Div:
    from ui.balance import render_balance

    return render_balance(wallet_balance, inventory_data)


def register_callbacks(app: Any) -> None:
    """Register all Dash callbacks on the given app instance."""

    @app.callback(
        Output("raw-items-store", "data"),
        Output("invest-store", "data"),
        Input("auto-refresh", "n_intervals"),
        prevent_initial_call=False,
    )
    def refresh_prices(n_intervals: Any) -> Any:
        # PV-05: all market data flows through ItemService (never raw repo/DB in callbacks)
        from src.domain.item_service import ItemService

        svc = ItemService.open()
        try:
            invest = svc.get_signals()
        finally:
            svc.close()

        return [], invest

    @app.callback(
        Output("container-list", "children"),
        Input("invest-store", "data"),
        Input("selected-cid", "data"),
        Input("sidebar-search", "value"),
        Input("inventory-store", "data"),
    )
    def render_container_list(
        invest: Any, selected_cid: Any, search: Any, inventory_data: Any
    ) -> Any:
        invest = invest or {}
        search = (search or "").lower().strip()
        all_containers = _get_containers()

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

            sections.append(
                html.Div(
                    id={"type": "ccard", "index": c.container_id},
                    n_clicks=0,
                    style={
                        "borderLeft": f"3px solid {border_c}",
                        "backgroundColor": _BG_SEL if is_sel else _BG,
                        "padding": "7px 10px",
                        "marginBottom": "3px",
                        "borderRadius": "2px",
                        "cursor": "pointer",
                    },
                    children=[
                        html.Div(
                            [
                                html.Span(
                                    name_str,
                                    style={
                                        "color": _TEXT,
                                        "fontSize": "11px",
                                        "fontWeight": "bold" if is_sel else "normal",
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
        Output("price-count-store", "data"),
        Input("startup-interval", "n_intervals"),
        Input("auto-refresh", "n_intervals"),
    )
    def load_price_count(_startup: Any, _refresh: Any) -> Any:
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
            from infra.task_manager import TaskQueueService
            try:
                _health = TaskQueueService().get_system_health()
            except Exception:
                _health = None
            return render_system_status(health=_health)  # PV-46: instant load on default tab
        return _no_data()

    # ── System Status: enable/disable interval based on active tab ────────────
    @app.callback(
        Output("health-interval", "disabled"),
        Input("main-tabs", "value"),
    )
    def toggle_health_interval(tab: Any) -> bool:
        return tab != "system"

    # ── System Status: refresh health data on interval ────────────────────────
    @app.callback(
        Output("tab-content", "children", allow_duplicate=True),
        Input("health-interval", "n_intervals"),
        State("main-tabs", "value"),
        prevent_initial_call=True,
    )
    def refresh_health_tab(n: Any, tab: Any) -> Any:
        if tab != "system":
            raise dash.exceptions.PreventUpdate
        from ui.renderers.system_status import render_system_status
        from infra.task_manager import TaskQueueService
        try:
            health = TaskQueueService().get_system_health()
        except Exception:
            health = None
        return render_system_status(health=health)

    # ── Flush Failed: show confirm dialog ─────────────────────────────────────
    @app.callback(
        Output("confirm-flush", "displayed"),
        Input("btn-flush-failed", "n_clicks"),
        prevent_initial_call=True,
    )
    def show_confirm_flush(n: Any) -> bool:
        return True

    # ── Flush Failed: execute on confirmation ─────────────────────────────────
    @app.callback(
        Output("health-action-msg", "children"),
        Input("confirm-flush", "submit_n_clicks"),
        prevent_initial_call=True,
    )
    def do_flush_failed(submit_n: Any) -> str:
        from infra.task_manager import TaskQueueService
        count = TaskQueueService().flush_failed()
        return f"Flushed {count} FAILED task(s)."

    # ── Reset Workers: show confirm dialog ────────────────────────────────────
    @app.callback(
        Output("confirm-reset-workers", "displayed"),
        Input("btn-reset-workers", "n_clicks"),
        prevent_initial_call=True,
    )
    def show_confirm_reset_workers(n: Any) -> bool:
        return True

    # ── Reset Workers: execute on confirmation ────────────────────────────────
    @app.callback(
        Output("health-action-msg", "children", allow_duplicate=True),
        Input("confirm-reset-workers", "submit_n_clicks"),
        prevent_initial_call=True,
    )
    def do_reset_workers(submit_n: Any) -> str:
        from infra.task_manager import TaskQueueService
        count = TaskQueueService().reset_stuck_workers()
        return f"Reset {count} stuck worker(s) to IDLE."

    # ── Reclaim Stuck: immediate action (no confirm needed) ───────────────────
    @app.callback(
        Output("health-action-msg", "children", allow_duplicate=True),
        Input("btn-reclaim-stuck", "n_clicks"),
        prevent_initial_call=True,
    )
    def do_reclaim_stuck(n: Any) -> str:
        from infra.task_manager import TaskQueueService
        count = TaskQueueService().reclaim_stuck_tasks()
        return f"Reclaimed {count} stuck task(s)."

    # ── Force Global Sync: enqueue all sync tasks at HIGH priority ────────────
    @app.callback(
        Output("health-action-msg", "children", allow_duplicate=True),
        Input("btn-force-sync", "n_clicks"),
        prevent_initial_call=True,
    )
    def do_force_sync(n: Any) -> str:
        from config import settings as _s
        from infra.task_manager import TaskQueueService
        svc = TaskQueueService()
        enqueued = []
        if _s.steam_id:
            t = svc.enqueue("sync_inventory", priority=1, payload={"steam_id": _s.steam_id})
            if t:
                enqueued.append("sync_inventory")
        t = svc.enqueue("sync_transactions", priority=1, payload={})
        if t:
            enqueued.append("sync_transactions")
        if enqueued:
            return f"Enqueued: {', '.join(enqueued)}"
        return "All sync tasks already queued or active."

    # ── Sync Inventory (Celery dispatch via API endpoint) ─────────────────────
    @app.callback(
        Output("health-action-msg", "children", allow_duplicate=True),
        Input("btn-sync-inventory", "n_clicks"),
        running=[
            (Output("btn-sync-inventory", "disabled"), True, False),
            (Output("btn-sync-inventory", "children"), [dbc.Spinner(size="sm"), " Sending…"], "Sync Inventory"),
        ],
        prevent_initial_call=True,
    )
    def do_sync_inventory(n: Any) -> str:
        import requests as _req
        try:
            r = _req.post("http://localhost:8000/api/v1/sync/inventory", timeout=5)
            data = r.json()
            if data.get("already_running"):
                return "Inventory sync already running."
            if data.get("ok"):
                return f"Inventory sync dispatched (task {(data.get('task_id') or '')[:8]})."
            return f"Inventory sync error: {data.get('message', 'unknown')}"
        except Exception as exc:
            return f"Inventory sync failed: {exc}"

    # ── Sync Market Catalog (Celery dispatch via API endpoint) ────────────────
    @app.callback(
        Output("health-action-msg", "children", allow_duplicate=True),
        Input("btn-sync-catalog", "n_clicks"),
        running=[
            (Output("btn-sync-catalog", "disabled"), True, False),
            (Output("btn-sync-catalog", "children"), [dbc.Spinner(size="sm"), " Sending…"], "Sync Catalog"),
        ],
        prevent_initial_call=True,
    )
    def do_sync_catalog(n: Any) -> str:
        import requests as _req
        try:
            r = _req.post("http://localhost:8000/api/v1/sync/market/catalog", timeout=5)
            data = r.json()
            if data.get("already_running"):
                return "Market catalog sync already running."
            if data.get("ok"):
                return f"Catalog sync dispatched (task {(data.get('task_id') or '')[:8]})."
            return f"Catalog sync error: {data.get('message', 'unknown')}"
        except Exception as exc:
            return f"Catalog sync failed: {exc}"

    # ── Sync Market Prices (Celery dispatch via API endpoint) ─────────────────
    @app.callback(
        Output("health-action-msg", "children", allow_duplicate=True),
        Input("btn-sync-prices", "n_clicks"),
        running=[
            (Output("btn-sync-prices", "disabled"), True, False),
            (Output("btn-sync-prices", "children"), [dbc.Spinner(size="sm"), " Sending…"], "Sync Prices"),
        ],
        prevent_initial_call=True,
    )
    def do_sync_prices(n: Any) -> str:
        import requests as _req
        try:
            r = _req.post("http://localhost:8000/api/v1/sync/market/prices", timeout=5)
            data = r.json()
            if data.get("already_running"):
                return "Price sync already running."
            if data.get("ok"):
                return f"Price sync dispatched (task {(data.get('task_id') or '')[:8]})."
            return f"Price sync error: {data.get('message', 'unknown')}"
        except Exception as exc:
            return f"Price sync failed: {exc}"

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

        from src.domain.models import FactTransaction

        db = SessionLocal()
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

        from src.domain.portfolio import upsert_annual
        from scrapper.steam_sync import sync_inventory as _sync_inv
        from scrapper.steam_sync import sync_transactions as _sync_tx
        from scrapper.steam_sync import sync_wallet as _sync_wal

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
                from src.domain.models import FactTransaction

                db = SessionLocal()
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
                from src.domain.connection import SessionLocal as _SL
                from infra.cache_writer import refresh_cache as _refresh_cache

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
        Input("header-interval", "n_intervals"),
        prevent_initial_call=False,
    )
    def update_header_badges(_n: Any) -> Any:
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
        Input("sync-age-interval", "n_intervals"),
        Input("sync-state", "data"),
        prevent_initial_call="initial_duplicate",
    )
    def update_sync_status(n_intervals: Any, sync_data: Any) -> Any:
        """Show last sync age and disable button while sync tasks are in queue."""
        from src.domain.connection import SessionLocal
        from src.domain.models import SystemSettings, TaskQueue, TaskStatus
        from dash import html as _html

        # Check if sync tasks are currently running
        try:
            with SessionLocal() as db:
                in_flight = (
                    db.query(TaskQueue)
                    .filter(
                        TaskQueue.type.in_(["sync_inventory", "sync_transactions"]),
                        TaskQueue.status.in_([TaskStatus.PENDING, TaskStatus.PROCESSING]),
                    )
                    .count()
                )

                # Get last successful sync time (use min of both task types)
                last_times = []
                for task_type in ["sync_inventory", "sync_transactions"]:
                    row = db.get(SystemSettings, f"last_sync_{task_type}")
                    if row and row.value:
                        try:
                            last_times.append(datetime.fromisoformat(row.value))
                        except ValueError:
                            pass
        except Exception:
            return "", False, [_html.I(className="fa fa-refresh me-1"), "Синхронизировать"]

        if in_flight > 0:
            btn_label = [dbc.Spinner(size="sm", color="light"), " Sync in progress..."]
            return "Sync in progress...", True, btn_label

        if last_times:
            oldest = min(last_times)
            age_min = int((datetime.now(UTC).replace(tzinfo=None) - oldest).total_seconds() / 60)
            if age_min < 60:
                age_str = f"Last sync: {age_min} min ago"
            else:
                age_h = age_min // 60
                age_str = f"Last sync: {age_h}h ago"
        else:
            age_str = "Last sync: never"

        btn_label = [_html.I(className="fa fa-refresh me-1"), "Синхронизировать"]
        return age_str, False, btn_label

    _register_cookie_callbacks(app)


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
        """Poll cookie status; open modal when EXPIRED or PAUSED_AUTH task exists."""
        from dash import ctx
        if ctx.triggered_id == "cookie-close-btn":
            logger.debug("check_cookie_status: user closed modal")
            return False

        cookie_expired = False
        has_paused = False

        try:
            import requests
            resp = requests.get(
                f"http://127.0.0.1:{_settings.api_port}/api/v1/system/cookie-status",
                timeout=3,
            )
            api_status = resp.json().get("status") if resp.ok else "ERROR"
            cookie_expired = api_status == "EXPIRED"
            logger.info("check_cookie_status: api_status=%r cookie_expired=%s", api_status, cookie_expired)
        except Exception as exc:
            logger.warning("check_cookie_status: API unreachable — %s", exc)

        try:
            from src.domain.connection import SessionLocal as _SL
            from src.domain.sql_repositories import SqlAlchemyTaskQueueRepository as _Repo
            with _SL() as _db:
                has_paused = _Repo(_db).has_paused_auth_tasks()
            logger.info("check_cookie_status: has_paused_auth=%s", has_paused)
        except Exception as exc:
            logger.warning("check_cookie_status: DB check failed — %s", exc)

        should_open = cookie_expired or has_paused
        logger.info("check_cookie_status: should_open=%s (was %s)", should_open, is_open)
        return should_open

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
                f"http://127.0.0.1:{_settings.api_port}/api/v1/system/update-cookie",
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
