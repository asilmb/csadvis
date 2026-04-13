"""
Signal handler — processes domain events and performs side-effects
(logging, DB persistence, webhook notifications).

Engines call the module-level functions directly:

    from services import signal_handler
    signal_handler.notify_super_deal(event)
    signal_handler.notify_liquidity_warning(event)
    signal_handler.notify_auth_error(event)
"""

from __future__ import annotations

import logging

from config import settings
from domain.events import AuthError, LiquidityWarning, PriceAlert, SuperDealDetected
from infra.event_logger import log_event
from infra.webhook_dispatcher import dispatch

logger = logging.getLogger(__name__)

_MODULE = "signal_handler"


# ── Module-level functions (called directly by publishers) ─────────────────────

def notify_super_deal(event: SuperDealDetected) -> None:
    """Handle SuperDealDetected: log + EventLog + webhook."""
    p = event.payload
    msg = (
        f"SUPER DEAL: {event.item_name} | "
        f"buy={p.get('buy_price', 0):.0f}{settings.currency_symbol} "
        f"target={p.get('target_exit_price', 0):.0f}{settings.currency_symbol} "
        f"stop={p.get('stop_loss_price', 0):.0f}{settings.currency_symbol} "
        f"margin={p.get('expected_margin_pct', 0):.1f}%"
    )
    logger.info(msg)
    log_event("INFO", _MODULE, msg)
    dispatch(
        {
            "event": "super_deal",
            "item": event.item_name,
            "buy_price": p.get("buy_price", 0),
            "target_exit_price": p.get("target_exit_price", 0),
            "stop_loss_price": p.get("stop_loss_price", 0),
            "expected_margin_pct": p.get("expected_margin_pct", 0),
        }
    )


def notify_liquidity_warning(event: LiquidityWarning) -> None:
    """Handle LiquidityWarning: log + EventLog (no webhook)."""
    msg = f"LIQUIDITY WARNING: {event.item_name} — BUY suppressed to HOLD | {event.payload}"
    logger.warning(msg)
    log_event("WARNING", _MODULE, msg)


def notify_price_alert(event: PriceAlert) -> None:
    """Handle PriceAlert: log + EventLog (no webhook)."""
    p = event.payload
    msg = (
        f"PRICE ALERT: {event.item_name} | "
        f"current={p.get('current_price', 0):.0f}{settings.currency_symbol} "
        f"threshold={p.get('threshold', 0):.0f}{settings.currency_symbol} "
        f"direction={p.get('direction', '?')}"
    )
    logger.info(msg)
    log_event("INFO", _MODULE, msg)


def notify_auth_error(event: AuthError) -> None:
    """Handle AuthError: log + EventLog + webhook."""
    msg = (
        f"AUTH ERROR: {event.item_name} — "
        f"Steam returned HTTP {event.status_code}. "
        f"Cookie may be stale — refresh STEAM_LOGIN_SECURE."
    )
    logger.error(msg)
    log_event("ERROR", _MODULE, msg)
    dispatch(
        {
            "event": "auth_error",
            "item": event.item_name,
            "status_code": event.status_code,
            "message": msg,
        }
    )


