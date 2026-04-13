"""
Domain events — immutable records of significant things that happened in the domain.

Publishers call services.signal_handler functions directly (no event bus):
    signal_handler.notify_super_deal(event)
    signal_handler.notify_liquidity_warning(event)
    signal_handler.notify_auth_error(event)

Event hierarchy:
    DomainEvent          (base, frozen dataclass)
    ├─ SuperDealDetected  (engine/portfolio_advisor: ultra-buy opportunity found)
    ├─ LiquidityWarning   (engine/investment: BUY signal suppressed — thin market)
    ├─ PriceAlert         (generic price-threshold crossing)
    └─ AuthError          (ingestion: Steam API returned 401 / 403 — stale cookie)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class DomainEvent:
    """Base class for all domain events. Frozen — events are immutable facts."""

    timestamp: datetime  # naive UTC — consistent with DB storage convention
    item_name: str       # human-readable container name for logging / display


@dataclass(frozen=True)
class SuperDealDetected(DomainEvent):
    """
    Published when _detect_super_deal() passes all 7 AND-filters.

    payload: the full super-deal dict returned by _detect_super_deal
             {verdict, buy_price, target_exit_price, stop_loss_price,
              expected_margin_pct, z_score, days_at_low, …}
    """

    payload: dict  # mutable by Python, but reference is frozen — treat as read-only


@dataclass(frozen=True)
class LiquidityWarning(DomainEvent):
    """
    Published when InvestmentDomainService.evaluate_investment() returns
    is_liquid=False, causing a BUY/LEAN BUY verdict to be suppressed to HOLD.

    payload: the reason string from LiquidityDecision.reason
    """

    payload: str


@dataclass(frozen=True)
class PriceAlert(DomainEvent):
    """
    Published when a container's current price crosses a configured threshold.

    payload: {current_price: float, threshold: float, direction: "above"|"below"}
    """

    payload: dict  # treat as read-only


@dataclass(frozen=True)
class AuthError(DomainEvent):
    """
    Published when Steam Market API returns HTTP 401 or 403.

    Indicates a stale or invalid steamLoginSecure cookie.  Triggers a webhook
    alert so the operator can refresh credentials before rate-limit windows close.

    item_name: the market_hash_name that triggered the auth failure.
    status_code: 401 or 403.
    payload: human-readable error context string.
    """

    status_code: int   # 401 or 403
    payload: str       # e.g. "Steam Market HTTP 403 for Recoil Case"
