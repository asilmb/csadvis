"""
Webhook dispatcher (PV-17).

Fire-and-forget HTTP POST to the configured webhook URL.  Compatible with
Discord, Telegram (via bot API), and generic Slack-style webhooks.

Usage:
    from services.webhook_dispatcher import dispatch

    dispatch({"content": "SUPER DEAL: Recoil Case — margin 25%"})

When WEBHOOK_URL is not configured (empty string), dispatch() is a no-op
and emits a WARNING-level log so operators know notifications are silent.

Threading model:
    dispatch() spawns a daemon thread for the HTTP call — never blocks the
    calling thread (signal_handler, worker, CLI).  If the POST fails the
    error is logged at WARNING level and silently dropped.
"""

from __future__ import annotations

import logging
import threading

logger = logging.getLogger(__name__)

_HTTP_TIMEOUT: int = 10  # seconds before webhook POST is abandoned


def dispatch(payload: dict) -> None:
    """
    Fire-and-forget POST of payload to the configured WEBHOOK_URL.

    No-op (with a WARNING log) when settings.webhook_url is empty.
    Never raises — all errors are logged and swallowed.
    """
    from config import settings

    url = settings.webhook_url.strip()
    if not url:
        logger.warning(
            "webhook_dispatcher: WEBHOOK_URL not configured — alert not sent (%r)",
            payload.get("event", "?"),
        )
        return

    threading.Thread(target=_post, args=(url, payload), daemon=True).start()


def _post(url: str, payload: dict) -> None:
    """Blocking HTTP POST — runs in a daemon thread spawned by dispatch()."""
    try:
        import httpx

        with httpx.Client(timeout=_HTTP_TIMEOUT) as client:
            resp = client.post(url, json=payload)
            logger.debug(
                "webhook_dispatcher: POST %s → HTTP %d", url, resp.status_code
            )
    except Exception as exc:
        logger.warning("webhook_dispatcher: POST to %s failed — %s", url, exc)
