"""
Browser launch and startup auth check utilities (PV-46/PV-47).
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def startup_auth_check(settings) -> bool:
    """
    Check Steam cookie validity before browser opens.
    Returns True if cookie is OK, False if EXPIRED.
    Writes EXPIRED status to DB if check fails.
    Never raises — failures are logged and treated as non-fatal.
    """
    if not settings.steam_login_secure:
        print("[UP] WARNING: STEAM_LOGIN_SECURE not set — add it via the dashboard modal.")
        return False
    try:
        from ingestion.steam_wallet import fetch_wallet_balance

        _bal, _msg = fetch_wallet_balance()
        if _bal is None and any(k in _msg.lower() for k in ("403", "устарел", "expired", "cookie")):
            print("[UP] WARNING: Steam cookie expired — dashboard will prompt for update.")
            try:
                from database.connection import SessionLocal, init_db

                init_db()
                from database.repositories import set_cookie_status

                with SessionLocal() as _db:
                    set_cookie_status(_db, "EXPIRED")
                    _db.commit()
            except Exception as _dbe:
                print(f"[UP] Could not mark cookie EXPIRED in DB: {_dbe}")
            return False
        return True
    except Exception as _exc:
        print(f"[UP] Auth pre-check failed (non-fatal): {_exc}")
        return True  # assume ok on error — don't block startup


def _open_url(url: str) -> None:
    """
    Open a URL in the default browser, robust on Windows Python 3.14+.

    webbrowser.open() delegates to os.startfile() on some Windows builds which
    raises OSError for http:// URLs.  We try three methods in order:
      1. subprocess with 'start' (Windows shell verb — most reliable)
      2. webbrowser.open_new_tab()
      3. webbrowser.open()
    """
    import subprocess
    import sys

    if sys.platform == "win32":
        try:
            subprocess.Popen(["cmd", "/c", "start", "", url], shell=False)
            return
        except Exception as exc:
            logger.debug("_open_url: cmd/start failed (%s) — trying webbrowser", exc)

    import webbrowser
    try:
        webbrowser.open_new_tab(url)
    except Exception:
        webbrowser.open(url)


def open_browser_tabs(dashboard_url: str, cookie_expired: bool) -> None:
    """
    Open browser tabs exactly once at startup.
    Always opens the dashboard. Opens Steam Market only if cookie is expired.
    """
    import time

    time.sleep(2)  # give services a moment to bind ports
    _open_url(dashboard_url)
    if cookie_expired:
        _open_url("https://steamcommunity.com/market/")
        print("[UP] Opened Steam Market — copy steamLoginSecure from DevTools → Cookies.")
