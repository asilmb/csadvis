"""
Low-level Steam Community Market HTTP client (PV-48).

TLS impersonation
-----------------
Uses curl_cffi.requests.AsyncSession with impersonate="chrome131" so the
TLS ClientHello (JA3), HTTP/2 SETTINGS frames, and pseudo-header order all
match a real Chrome 131 browser — Steam's bot-detection cannot distinguish
these requests from a normal browser session.

Session reuse
-------------
One AsyncSession is created per SteamMarketClient instance (in __aenter__)
and shared across every request in the batch.  This avoids redundant TCP /
TLS handshakes that would otherwise occur if a new client were created per
call.

Cookie persistence
------------------
Cookies that Steam sets during a session (sessionid, steamMachineAuth*, etc.)
are saved to Redis on __aexit__ and reloaded on __aenter__, giving continuity
across Celery task invocations.  steamLoginSecure comes from the Redis
credential store (infra.steam_credentials) and is never overwritten by the
session cookie cache.

Usage
-----
    async with SteamMarketClient() as client:
        overview = await client.fetch_price_overview(name)

The class also works without a context manager (lazy init on first request),
but __aexit__ will not run and cookies will not be persisted to Redis.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta

import structlog

from infra.redis_client import get_redis
from infra.steam_credentials import get_login_secure, get_session_id
from scrapper.steam.formatter import InvalidHashNameError, to_api_name
from scrapper.steam_rate_limit import request_delay
from scrapper.steam.parser import (
    parse_history_response,
    parse_nameid_html,
    parse_order_book_response,
    parse_overview_response,
)

logger = structlog.get_logger()

_BASE          = "https://steamcommunity.com"
_HISTORY_URL   = _BASE + "/market/pricehistory/"
_OVERVIEW_URL  = _BASE + "/market/priceoverview/"
_LISTINGS_URL  = _BASE + "/market/listings/730/"
_ORDER_BOOK_URL = _BASE + "/market/itemordershistogram"

_STEALTH_KEY           = "STEALTH_BLOCK_EXPIRES"
_EMERGENCY_BLOCK_HOURS = 6
_EMERGENCY_BLOCK_TTL   = _EMERGENCY_BLOCK_HOURS * 3600

# Exponential backoff for 429 within the client (independent of Celery-level retry).
# Penalty grows as: 120 s → 240 s → 480 s → … capped at the emergency block duration.
_BACKOFF_BASE_SECONDS  = 120
_BACKOFF_CAP_SECONDS   = _EMERGENCY_BLOCK_TTL

_IMPERSONATE  = "chrome131"
_COOKIE_KEY   = "cs2:steam:session_cookies"
_COOKIE_TTL   = 86_400   # 24 h — refresh on every successful batch

# Cookies Steam sets server-side that are worth carrying across sessions.
# steamLoginSecure is deliberately excluded — it comes from the credential store.
_PERSIST_PREFIXES = ("steamMachineAuth", "sessionid", "steamCountry", "timezoneOffset")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _is_emergency_blocked() -> bool:
    """Return True if a 429-triggered 6 h block is currently active."""
    try:
        return bool(get_redis().exists(_STEALTH_KEY))
    except Exception:
        return False


def _exp_backoff_seconds(attempt: int) -> int:
    """
    Exponential backoff penalty for 429 responses.

    attempt=0 → 120 s, attempt=1 → 240 s, attempt=2 → 480 s …
    Capped at _BACKOFF_CAP_SECONDS (== emergency block TTL == 6 h).
    """
    return min(_BACKOFF_CAP_SECONDS, _BACKOFF_BASE_SECONDS * (2 ** attempt))


def _trigger_emergency_stop(item_name: str, attempt: int = 0) -> int:
    """
    Block all Steam requests after a 429 response.

    The Redis TTL grows exponentially with *attempt* (capped at 6 h) so repeated
    bans escalate the cooldown rather than always resetting to the same window.
    Returns the actual TTL written to Redis (in seconds).
    """
    ttl = _exp_backoff_seconds(attempt)
    try:
        # NX so an existing, longer block is never shortened by a new hit.
        redis = get_redis()
        if not redis.set(_STEALTH_KEY, item_name, nx=True, ex=ttl):
            # Key already exists — only extend if our new TTL is longer.
            existing_ttl = redis.ttl(_STEALTH_KEY)
            if ttl > existing_ttl:
                redis.expire(_STEALTH_KEY, ttl)
    except Exception as exc:
        logger.warning("emergency_stop_write_failed", service="steam_client", error=str(exc))
    blocked_until = datetime.now(UTC).replace(tzinfo=None) + timedelta(seconds=ttl)
    logger.warning(
        "emergency_stop_triggered",
        service="steam_client",
        triggered_by=item_name,
        attempt=attempt,
        backoff_seconds=ttl,
        blocked_until=blocked_until.strftime("%H:%M UTC"),
    )
    return ttl


def _publish_auth_error(item_name: str, status_code: int) -> None:
    """Notify about Steam auth error. Direct call — no event bus."""
    try:
        from src.domain.events import AuthError
        from infra.signal_handler import notify_auth_error

        notify_auth_error(
            AuthError(
                timestamp=datetime.now(UTC).replace(tzinfo=None),
                item_name=item_name,
                status_code=status_code,
                payload=f"Steam Market HTTP {status_code} for {item_name}",
            )
        )
    except Exception as exc:
        logger.debug("_publish_auth_error: failed — %s", exc)


# ── Client ────────────────────────────────────────────────────────────────────

class SteamMarketClient:
    """Async Steam Market client — TLS-impersonated, session-persistent."""

    def __init__(self, attempt: int = 0) -> None:
        cookie = get_login_secure()
        if not cookie:
            raise RuntimeError(
                "No Steam cookie set — open the dashboard and enter your steamLoginSecure cookie."
            )
        self._login_secure: str = cookie
        self._session_id_cfg: str = get_session_id()
        self._session = None   # curl_cffi.requests.AsyncSession — created lazily
        # Celery retry index passed in by the caller so the exponential backoff
        # TTL escalates correctly across task retries (0 = first attempt).
        self._attempt: int = attempt

    # ── Cookie cache ──────────────────────────────────────────────────────────

    def _load_persisted_cookies(self) -> dict[str, str]:
        """
        Pull cookies saved by a previous batch from Redis.
        Returns {} when Redis is unavailable or no entry exists.
        """
        try:
            raw = get_redis().get(_COOKIE_KEY)
            if raw:
                data: dict[str, str] = json.loads(raw)
                logger.debug("steam_cookies_loaded", service="steam_client", count=len(data))
                return data
        except Exception as exc:
            logger.debug("steam_cookies_load_failed", service="steam_client", error=str(exc))
        return {}

    def _save_persisted_cookies(self) -> None:
        """
        Write eligible cookies from the live session back to Redis so the
        next batch can reuse them without a fresh login flow.
        steamLoginSecure is intentionally excluded — it lives in the credential store only.
        """
        if self._session is None:
            return
        try:
            jar: dict[str, str] = {}
            for k, v in dict(self._session.cookies).items():
                if any(k.startswith(p) for p in _PERSIST_PREFIXES):
                    jar[k] = v
            if jar:
                get_redis().setex(_COOKIE_KEY, _COOKIE_TTL, json.dumps(jar))
                logger.debug("steam_cookies_saved", service="steam_client", count=len(jar))
        except Exception as exc:
            logger.debug("steam_cookies_save_failed", service="steam_client", error=str(exc))

    # ── Session lifecycle ─────────────────────────────────────────────────────

    async def _ensure_session(self) -> None:
        """
        Create the curl_cffi AsyncSession if it does not yet exist.

        Called automatically by every request method, so the class works both
        as a context manager and standalone (without __aenter__).
        """
        if self._session is not None:
            return

        from curl_cffi.requests import AsyncSession

        # Start with any cookies persisted from a prior batch, then overlay
        # the authoritative auth cookie from the credential store so it always wins.
        cookies = self._load_persisted_cookies()
        cookies["steamLoginSecure"] = self._login_secure
        if self._session_id_cfg:
            cookies["sessionid"] = self._session_id_cfg

        self._session = AsyncSession(impersonate=_IMPERSONATE, cookies=cookies)
        logger.debug(
            "steam_session_created",
            service="steam_client",
            impersonate=_IMPERSONATE,
            cookies=list(cookies.keys()),
        )

    async def __aenter__(self) -> SteamMarketClient:
        await self._ensure_session()
        return self

    async def __aexit__(self, *_: object) -> None:
        self._save_persisted_cookies()
        if self._session is not None:
            await self._session.close()
            self._session = None

    # ── Steam-specific headers ────────────────────────────────────────────────

    @property
    def _steam_headers(self) -> dict[str, str]:
        """
        Full Chrome 131 header set added on top of the TLS fingerprint that
        curl_cffi injects via impersonation.

        All Sec-Ch-Ua / Sec-Fetch-* values are kept in exact syntactic lock-step
        with the impersonate="chrome131" profile (Windows, desktop, non-mobile).
        Changing _IMPERSONATE without updating these headers would produce a
        detectable fingerprint mismatch.
        """
        return {
            # ── Content negotiation ────────────────────────────────────────────
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "en-US,en;q=0.9",
            # ── Steam-specific ─────────────────────────────────────────────────
            "Referer": "https://steamcommunity.com/market/",
            "X-Requested-With": "XMLHttpRequest",
            # ── Client Hints — must match Chrome 131 on Windows ───────────────
            "Sec-Ch-Ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            # ── Fetch metadata ─────────────────────────────────────────────────
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
        }

    # ── Internal GET helper ───────────────────────────────────────────────────

    async def _get(self, url: str, params: dict | None = None) -> object:
        """Shared GET through the persistent session."""
        from curl_cffi.requests import RequestsError

        await self._ensure_session()
        try:
            return await self._session.get(
                url,
                params=params,
                headers=self._steam_headers,
                timeout=45,
            )
        except RequestsError as exc:
            raise exc  # callers handle network errors per-method

    # ── Public API ────────────────────────────────────────────────────────────

    async def fetch_history(self, market_hash_name: str) -> list[dict]:
        """
        Fetch daily price history for one item.

        Returns list of {"date": datetime, "price": float, "volume": int}
        sorted oldest-first.
        """
        if _is_emergency_blocked():
            logger.debug("[Stealth] fetch_history skipped — emergency stop active")
            return []

        api_name = to_api_name(market_hash_name)
        params = {"appid": "730", "market_hash_name": api_name, "currency": "37"}

        from curl_cffi.requests import RequestsError
        try:
            resp = await self._get(_HISTORY_URL, params=params)
        except RequestsError as exc:
            logger.warning(
                "steam_network_error",
                service="steam_client",
                name=market_hash_name,
                exc_type=exc.__class__.__name__,
            )
            return []

        if resp.status_code == 429:
            _trigger_emergency_stop(market_hash_name, attempt=self._attempt)
            return []
        if resp.status_code == 400:
            logger.warning("steam_http_400", service="steam_client", name=market_hash_name)
            return []
        if resp.status_code == 403:
            logger.warning("steam_http_403", service="steam_client", name=market_hash_name)
            _publish_auth_error(market_hash_name, 403)
            return []
        if resp.status_code in (404, 500):
            raise InvalidHashNameError(api_name, resp.status_code)
        if resp.status_code != 200:
            resp.raise_for_status()

        data = resp.json()
        if not data.get("success"):
            logger.warning("steam_success_false", service="steam_client", name=market_hash_name)
            return []

        return parse_history_response(data)

    async def fetch_price_overview(self, market_hash_name: str) -> dict:
        """
        Fetch current price from /market/priceoverview/.

        Returns {"market_hash_name": str, "median_price": float,
                 "lowest_price": float, "volume": int} or {} on failure.
        """
        if _is_emergency_blocked():
            logger.debug("[Stealth] fetch_price_overview skipped — emergency stop active")
            return {}

        api_name = to_api_name(market_hash_name)
        params = {"appid": "730", "market_hash_name": api_name, "currency": "37"}

        from curl_cffi.requests import RequestsError
        try:
            resp = await self._get(_OVERVIEW_URL, params=params)
        except RequestsError as exc:
            logger.warning(
                "priceoverview_request_failed",
                service="steam_client",
                name=market_hash_name,
                error=str(exc),
            )
            return {}

        if resp.status_code == 429:
            _trigger_emergency_stop(market_hash_name, attempt=self._attempt)
            return {}
        if resp.status_code in (401, 403):
            logger.warning(
                "priceoverview_auth_error",
                service="steam_client",
                status_code=resp.status_code,
                name=market_hash_name,
            )
            _publish_auth_error(market_hash_name, resp.status_code)
            return {}
        if resp.status_code in (404, 500):
            raise InvalidHashNameError(api_name, resp.status_code)
        if resp.status_code != 200:
            logger.warning(
                "priceoverview_http_error",
                service="steam_client",
                status_code=resp.status_code,
                name=market_hash_name,
            )
            return {}

        data = resp.json()
        if not data.get("success"):
            return {}

        return parse_overview_response(data, market_hash_name)

    async def fetch_nameid(self, market_hash_name: str) -> int | None:
        """
        Fetch item_nameid from the Steam Market listing page HTML.

        item_nameid is a stable integer required by the itemordershistogram
        endpoint.  Rate-limited: sleeps _DELAY_SECONDS after the request.
        """
        if _is_emergency_blocked():
            logger.debug("[Stealth] fetch_nameid skipped — emergency stop active")
            return None

        url = _LISTINGS_URL + market_hash_name

        from curl_cffi.requests import RequestsError
        try:
            resp = await self._get(url)
        except RequestsError as exc:
            logger.warning(
                "fetch_nameid_network_error",
                service="steam_client",
                name=market_hash_name,
                error=str(exc),
            )
            return None
        finally:
            await asyncio.sleep(request_delay())

        if resp.status_code == 429:
            _trigger_emergency_stop(market_hash_name, attempt=self._attempt)
            return None
        if resp.status_code != 200:
            logger.warning(
                "fetch_nameid_http_error",
                service="steam_client",
                status_code=resp.status_code,
                name=market_hash_name,
            )
            return None

        nameid = parse_nameid_html(resp.text)
        if nameid is None:
            logger.debug("fetch_nameid_not_found", service="steam_client", name=market_hash_name)
        return nameid

    async def fetch_order_book(self, item_nameid: int) -> dict:
        """
        Fetch the order book histogram for an item.

        Returns {"sell_order_graph": [...], "buy_order_graph": [...]} or {}.
        Rate-limited: sleeps _DELAY_SECONDS after the request.
        """
        params = {
            "country": "KZ",
            "language": "russian",
            "currency": "37",
            "item_nameid": str(item_nameid),
            "two_factor": "0",
        }

        from curl_cffi.requests import RequestsError
        try:
            resp = await self._get(_ORDER_BOOK_URL, params=params)
        except RequestsError as exc:
            logger.warning(
                "fetch_order_book_network_error",
                service="steam_client",
                nameid=item_nameid,
                error=str(exc),
            )
            return {}
        finally:
            await asyncio.sleep(request_delay())

        if resp.status_code == 429:
            logger.warning("fetch_order_book_rate_limited", service="steam_client", nameid=item_nameid)
            _trigger_emergency_stop(str(item_nameid), attempt=self._attempt)
            return {}
        if resp.status_code != 200:
            logger.warning(
                "fetch_order_book_http_error",
                service="steam_client",
                status_code=resp.status_code,
                nameid=item_nameid,
            )
            return {}

        try:
            data = resp.json()
        except Exception as exc:
            logger.warning(
                "fetch_order_book_json_error",
                service="steam_client",
                nameid=item_nameid,
                error=str(exc),
            )
            return {}

        if not data.get("success"):
            return {}

        return parse_order_book_response(data)
