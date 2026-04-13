"""
Steam wallet balance — Redis persistence + auto-fetch via Steam cookie.

The user can either enter the balance manually or load it automatically from
Steam using the STEAM_LOGIN_SECURE cookie (same one used for backfill).

Balances are stored and used in the account's native currency — no conversion is performed.
"""

from __future__ import annotations

import contextlib
import json
import logging
import re
import httpx

from config import settings
from infra.redis_client import get_redis

logger = logging.getLogger(__name__)


class AuthError(Exception):
    """Raised when Steam page indicates the session cookie is expired."""

_KEY = "cs2:wallet:balance"


def get_saved_balance() -> float | None:
    """Load the last saved wallet balance from Redis."""
    try:
        val = get_redis().get(_KEY)
        return float(val) if val else None
    except Exception:
        return None


def save_balance(balance: float) -> None:
    """Persist wallet balance to Redis."""
    try:
        get_redis().set(_KEY, str(balance))
    except Exception as exc:
        logger.warning("steam_wallet: could not persist to Redis: %s", exc)
    logger.info("Saved wallet balance: %.0f", balance)


def _parse_amount(text: str) -> float | None:
    """Parse an amount string like '12 345 ₸' or '12,345.00' → float."""
    # Remove currency symbols and letters, keep digits, spaces, commas, dots
    cleaned = re.sub(r"[^\d\s,.]", "", text).strip()
    # Remove whitespace used as thousands separator (e.g. "12 345" → "12345")
    cleaned = re.sub(r"\s+", "", cleaned)
    if not cleaned:
        return None
    # Handle decimal comma (European style: 12.345,67 → 12345.67)
    if "," in cleaned and "." in cleaned:
        if cleaned.rfind(".") > cleaned.rfind(","):
            cleaned = cleaned.replace(",", "")
        else:
            cleaned = cleaned.replace(".", "").replace(",", ".")
    elif "," in cleaned:
        parts = cleaned.split(",")
        if len(parts) == 2 and len(parts[1]) <= 2:
            cleaned = cleaned.replace(",", ".")
        else:
            cleaned = cleaned.replace(",", "")
    try:
        return float(cleaned)
    except ValueError:
        return None


def fetch_wallet_balance() -> tuple[float | None, str]:
    """
    Fetch Steam wallet balance by scraping the Community Market page.

    Uses STEAM_LOGIN_SECURE cookie from settings (same as backfill).
    Returns (balance, message) — balance is None on failure.
    """
    cookie = settings.steam_login_secure
    if not cookie:
        # Try to read directly from Chrome browser on Windows
        logger.info("STEAM_LOGIN_SECURE not in .env — trying Chrome cookie store...")
        cookie = read_cookie_from_chrome()
        if not cookie:
            return None, "NO_COOKIE"
        logger.info("Cookie read from Chrome successfully")

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Referer": "https://steamcommunity.com/market/",
        "Accept-Language": "ru-RU,ru;q=0.9",
    }

    try:
        resp = httpx.get(
            "https://steamcommunity.com/market/",
            cookies={"steamLoginSecure": cookie},
            headers=headers,
            timeout=10,
            follow_redirects=True,
        )
    except Exception as exc:
        logger.warning("Steam wallet fetch failed: %s", exc)
        return None, f"Network error: {exc}"

    if resp.status_code != 200:
        return None, f"Steam returned HTTP {resp.status_code}"

    # Auto-extract sessionid from response cookies (PV-52) — best-effort, non-fatal
    try:
        sid = resp.cookies.get("sessionid")
        if sid and sid != settings.steam_session_id:
            settings.steam_session_id = sid
            import os as _os
            _os.environ["STEAM_SESSION_ID"] = sid
            logger.info("steam_wallet: sessionid auto-extracted from Set-Cookie and saved to runtime")
    except Exception:
        pass

    html = resp.text

    # Auth check: g_steamID is injected into the page only for logged-in users.
    steamid_js = re.search(r'g_steamID\s*=\s*([^;]+);', html)
    if not steamid_js or steamid_js.group(1).strip().lower() in ("false", "null", '""', "''", ""):
        logger.warning("Steam wallet: not logged in (g_steamID absent or false) — cookie expired")
        return None, "Cookie устарел — войди в Steam и обнови cookie: cs2 cookie"

    # Primary: <span id="marketWalletBalanceAmount">12 345 ₸</span>
    match = re.search(
        r'id=["\']marketWalletBalanceAmount["\'][^>]*>([^<]+)<',
        html,
    )
    if not match:
        # Fallback: wallet_header_link contains balance text
        match = re.search(
            r"wallet_header_link[^>]*>[^<]*<[^>]+>([^<]*[₸₽$€\$][^<]*)<",
            html,
        )

    if not match:
        logger.debug("Steam wallet: balance element not found in page HTML")
        raise AuthError("Balance element not found — cookie likely expired")

    raw = match.group(1).strip()
    logger.debug("Steam wallet raw balance string: %r", raw)

    balance = _parse_amount(raw)
    if balance is None or balance <= 0:
        return None, f"Не удалось распарсить сумму: {raw!r}"

    logger.info("Steam wallet balance fetched: %.0f (raw: %r)", balance, raw)
    return balance, f"Загружено: {int(balance):,} {settings.currency_symbol}"


def read_cookie_from_chrome() -> str | None:
    """
    Read steamLoginSecure cookie directly from Chrome on Windows.

    Decrypts using Windows DPAPI + AES-256-GCM (no admin required).
    Chrome must be CLOSED — the Cookies SQLite file is locked while Chrome runs.

    Returns the cookie value string, or None on any failure.
    """
    import base64
    import os
    import shutil
    import sqlite3
    import sys
    import tempfile
    from pathlib import Path

    if sys.platform != "win32":
        return None

    local_app_data = os.environ.get("LOCALAPPDATA", "")
    chrome_base = Path(local_app_data) / "Google" / "Chrome" / "User Data"
    cookie_file = chrome_base / "Default" / "Network" / "Cookies"
    local_state_file = chrome_base / "Local State"

    if not cookie_file.exists() or not local_state_file.exists():
        logger.debug("Chrome profile not found at %s", chrome_base)
        return None

    # ── Decrypt the AES key stored in Local State ─────────────────────────────
    try:
        local_state = json.loads(local_state_file.read_text(encoding="utf-8"))
        encrypted_key_b64 = local_state["os_crypt"]["encrypted_key"]
        encrypted_key = base64.b64decode(encrypted_key_b64)[5:]  # strip "DPAPI" prefix

        import win32crypt  # type: ignore[import-untyped]

        aes_key = win32crypt.CryptUnprotectData(encrypted_key, None, None, None, 0)[1]
    except Exception as exc:
        logger.debug("Chrome key decryption failed: %s", exc)
        return None

    # ── Copy locked SQLite file to temp location ──────────────────────────────
    tmp_path = Path(tempfile.gettempdir()) / "cs2_chrome_cookies.tmp"
    try:
        shutil.copy2(cookie_file, tmp_path)
    except PermissionError:
        logger.debug("Chrome Cookies file is locked — Chrome must be closed")
        return None
    except Exception as exc:
        logger.debug("Could not copy Chrome Cookies: %s", exc)
        return None

    # ── Query and decrypt the steamLoginSecure cookie ─────────────────────────
    try:
        con = sqlite3.connect(str(tmp_path))
        rows = con.execute(
            "SELECT encrypted_value FROM cookies "
            "WHERE host_key LIKE '%steamcommunity%' AND name='steamLoginSecure'"
        ).fetchall()
        con.close()
    except Exception as exc:
        logger.debug("Chrome Cookies SQLite query failed: %s", exc)
        return None
    finally:
        with contextlib.suppress(Exception):
            tmp_path.unlink(missing_ok=True)

    if not rows:
        logger.debug("steamLoginSecure not found in Chrome Cookies")
        return None

    encrypted_value: bytes = rows[0][0]

    # v10/v20 → AES-256-GCM
    if encrypted_value[:3] in (b"v10", b"v20"):
        try:
            from Crypto.Cipher import AES  # type: ignore[import-untyped]

            nonce = encrypted_value[3:15]
            ciphertext = encrypted_value[15:-16]
            tag = encrypted_value[-16:]
            cipher = AES.new(aes_key, AES.MODE_GCM, nonce=nonce)
            return cipher.decrypt_and_verify(ciphertext, tag).decode("utf-8")
        except Exception as exc:
            logger.debug("AES-GCM decryption failed: %s", exc)
            return None

    # Legacy DPAPI-encrypted value
    try:
        return win32crypt.CryptUnprotectData(encrypted_value, None, None, None, 0)[1].decode(
            "utf-8"
        )
    except Exception as exc:
        logger.debug("DPAPI cookie decryption failed: %s", exc)
        return None
