"""
Pydantic response schemas — JSON contract for the Container Investment API.

These schemas reflect the current domain: container price tracking and investment signals.
Dead EV/ROI/Risk schemas removed (2026-03-27) — see git history if needed.
"""

from __future__ import annotations

from pydantic import BaseModel

# ─── Container list response ──────────────────────────────────────────────────


class ContainerListItem(BaseModel):
    container_id: str
    container_name: str
    container_type: str
    base_cost: float
    verdict: str  # BUY / LEAN BUY / HOLD / LEAN SELL / SELL / NO DATA
    current_price: float | None = None
    baseline_price: float | None = None
    price_ratio_pct: float | None = None
    momentum_pct: float | None = None
    quantity: int = 0
    score: int = 0
    sell_at_loss: bool | None = None


# ─── Container detail response ────────────────────────────────────────────────


class PriceHistoryEntry(BaseModel):
    timestamp: str
    price: float | None = None
    mean_price: float | None = None
    volume_7d: int | None = None


class ContainerDetail(ContainerListItem):
    price_history: list[PriceHistoryEntry] = []


# ─── Sync response schemas ─────────────────────────────────────────────────────


class SyncWalletResponse(BaseModel):
    ok: bool
    balance: float | None = None
    message: str
    error_code: str | None = None  # "NO_COOKIE" | "STALE_COOKIE" | "NETWORK" | None


class SyncInventoryResponse(BaseModel):
    ok: bool
    count: int = 0
    message: str
    error_code: str | None = None  # "NO_STEAM_ID" | "NETWORK" | None


class SyncTransactionsResponse(BaseModel):
    ok: bool
    buy_count: int = 0
    sell_count: int = 0
    message: str
    error_code: str | None = None  # "NO_COOKIE" | "STALE_COOKIE" | "NETWORK" | None


class SyncDispatchResponse(BaseModel):
    ok: bool
    already_running: bool = False
    task_id: str | None = None
    message: str
