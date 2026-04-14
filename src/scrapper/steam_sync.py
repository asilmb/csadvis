"""
Services layer — Steam HTTP data sync.

Wraps the three Steam HTTP ingestion calls (wallet, inventory, transactions)
behind clean synchronous service functions with typed result objects.

This decouples Dash callbacks from ingestion details and makes the sync
operations testable and reusable outside the dashboard context.

Functions:
    sync_wallet()           — fetch wallet balance, persist to DB, return result
    sync_inventory(steam_id) — fetch Steam inventory, return item list + stats
    sync_transactions(max_pages) — fetch market history, persist to DB, return result
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from config import settings

logger = logging.getLogger(__name__)


class SteamAuthError(Exception):
    """Raised when Steam API rejects requests due to an expired or missing cookie."""


# ─── Result types ──────────────────────────────────────────────────────────────


@dataclass
class WalletResult:
    """Result of sync_wallet()."""

    ok: bool
    balance: float | None  # None on failure
    message: str  # human-readable status (Russian UI strings)
    error_code: str | None  # "NO_COOKIE" | "STALE_COOKIE" | "NETWORK" | None


@dataclass
class InventoryResult:
    """Result of sync_inventory()."""

    ok: bool
    items: list[dict] = field(default_factory=list)  # raw item dicts from Steam
    count: int = 0
    message: str = ""
    error_code: str | None = None  # "NO_STEAM_ID" | "NETWORK" | None


@dataclass
class TransactionsResult:
    """Result of sync_transactions()."""

    ok: bool
    transactions: list[dict] = field(default_factory=list)
    buy_count: int = 0
    sell_count: int = 0
    annual_pnl: dict[int, float] = field(default_factory=dict)
    message: str = ""
    error_code: str | None = None  # "NO_COOKIE" | "STALE_COOKIE" | "NETWORK" | None


# ─── Service functions ─────────────────────────────────────────────────────────


def sync_wallet() -> WalletResult:
    """
    Fetch Steam wallet balance from the Community Market page.

    On success: persists the balance to the local cache and returns
    the balance amount.

    On failure: returns the last cached balance (if any) with ok=False so
    the caller can show a degraded-but-not-empty state.
    """
    from scrapper.steam_wallet import (
        AuthError,
        fetch_wallet_balance,
        get_saved_balance,
        save_balance,
    )

    try:
        balance, msg = fetch_wallet_balance()
    except AuthError as exc:
        cached = get_saved_balance()
        try:
            from src.domain.connection import SessionLocal
            from src.domain.sql_repositories import set_cookie_status
            with SessionLocal() as _db:
                set_cookie_status(_db, "EXPIRED")
                _db.commit()
        except Exception:
            pass
        return WalletResult(
            ok=False,
            balance=cached,
            message=str(exc),
            error_code="STALE_COOKIE",
        )

    if balance is None:
        cached = get_saved_balance()
        if msg == "NO_COOKIE":
            error_code = "NO_COOKIE"
        elif "403" in msg or "устарел" in msg.lower():
            error_code = "STALE_COOKIE"
        else:
            error_code = "NETWORK"
        logger.warning("sync_wallet failed: %s (cached=%.0f)", msg, cached or 0)
        return WalletResult(
            ok=False,
            balance=cached,
            message=msg,
            error_code=error_code,
        )

    save_balance(balance)
    try:
        from src.domain.connection import SessionLocal
        from src.domain.sql_repositories import set_cookie_status
        with SessionLocal() as _db:
            set_cookie_status(_db, "VALID")
            _db.commit()
    except Exception:
        pass
    logger.info("sync_wallet ok: %.0f", balance)
    return WalletResult(
        ok=True,
        balance=balance,
        message=f"Загружено: {int(balance):,} {settings.currency_symbol}",
        error_code=None,
    )


def sync_inventory(steam_id: str) -> InventoryResult:
    """
    Fetch the Steam inventory for the given steam_id.

    Returns raw item dicts as returned by frontend.inventory.fetch_inventory().
    The caller is responsible for persisting/displaying items — this function
    only fetches and normalises the response.
    """
    # fetch_inventory is synchronous — it owns its own event loop internally.
    # Do NOT wrap in asyncio.new_event_loop().run_until_complete() — that
    # would pass a list (the return value) to run_until_complete, which
    # requires an awaitable and raises TypeError.
    from ui.inventory import fetch_inventory

    if not steam_id or not steam_id.strip():
        return InventoryResult(
            ok=False,
            message="STEAM_ID не настроен — добавь STEAM_ID= в .env",
            error_code="NO_STEAM_ID",
        )

    sid = steam_id.strip()
    try:
        items: list[dict] = fetch_inventory(sid)
    except Exception as exc:
        logger.error("sync_inventory failed for steam_id=%r: %s", sid, exc)
        return InventoryResult(
            ok=False,
            message=f"Ошибка загрузки инвентаря: {exc}",
            error_code="NETWORK",
        )

    count = len(items) if items else 0
    logger.info("sync_inventory ok: %d items for steam_id=%r", count, sid)
    return InventoryResult(
        ok=True,
        items=items or [],
        count=count,
        message=f"Загружено {count} предметов" if count else "Инвентарь пуст",
    )


def sync_transactions(max_pages: int = 10) -> TransactionsResult:
    """
    Fetch Steam Market CS2 transaction history and compute annual P&L.

    Does NOT persist to DB — the caller (callback or CLI) handles persistence.
    This is intentional: the service layer stays stateless so it can be used
    in test/CLI contexts without side effects.

    Returns computed annual_pnl so callers can upsert without re-importing
    ingestion modules.
    """
    from scrapper.steam_transactions import compute_annual_pnl, fetch_market_history

    transactions, msg = fetch_market_history(max_pages=max_pages)

    # Detect real errors: fetch_market_history returns "Загружено N транзакций CS2"
    # on success (even N=0).  Any other message string signals an actual failure.
    _is_auth_error = (
        msg == "NO_COOKIE"
        or "403" in msg
        or "устарел" in msg.lower()
        or "success=false" in msg.lower()
    )
    _is_error = _is_auth_error or (
        not msg.startswith("Загружено") and msg != "NO_COOKIE"
        and not transactions  # truly unexpected empty response
    )

    if _is_auth_error:
        logger.warning("sync_transactions: auth error — %s", msg)
        if msg == "NO_COOKIE":
            error_code = "NO_COOKIE"
        else:
            error_code = "STALE_COOKIE"
        return TransactionsResult(ok=False, message=msg, error_code=error_code)

    if _is_error:
        logger.warning("sync_transactions: network/parse error — %s", msg)
        return TransactionsResult(ok=False, message=msg, error_code="NETWORK")

    # HTTP 200 + success=true — zero transactions is valid (empty history)
    if not transactions:
        logger.info("sync_transactions: no CS2 transactions found (empty history) — %s", msg)
        return TransactionsResult(
            ok=True,
            transactions=[],
            message="No CS2 transactions found",
        )

    buy_count = sum(1 for t in transactions if t["action"] == "BUY")
    sell_count = sum(1 for t in transactions if t["action"] == "SELL")
    annual_pnl = compute_annual_pnl(transactions)

    logger.info(
        "sync_transactions ok: %d total (%d buy / %d sell), %d years",
        len(transactions),
        buy_count,
        sell_count,
        len(annual_pnl),
    )
    return TransactionsResult(
        ok=True,
        transactions=transactions,
        buy_count=buy_count,
        sell_count=sell_count,
        annual_pnl=annual_pnl,
        message=f"Загружено {len(transactions)} транзакций CS2",
    )
