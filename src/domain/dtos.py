"""
Data Transfer Objects for the CS2 Market Analytics Service Layer (PV-05).

All DTOs are Pydantic models (frozen=True): immutable, validated, serialisable.
They cross every layer boundary — Celery tasks, FastAPI responses, Dash callbacks —
without leaking SQLAlchemy ORM objects.

DTOs defined here:
  ItemDTO         — current market snapshot for one container (main table row).
  PriceHistoryDTO — one price observation (sparkline / detail chart data point).
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, computed_field


class ItemDTO(BaseModel):
    """
    Current market snapshot for a single CS2 container.

    Fields
    ------
    id              : dim_containers.container_id (UUID string)
    name            : market-facing container name
    current_price   : latest Steam Market price
    volume_7d       : 7-day traded volume (Steam units)
    is_suspicious   : True when price deviates >20 % above its 30-day mean
    volatility      : coefficient of variation (std / mean × 100) of 30-day prices

    Computed fields (derived, no extra storage)
    ---------------
    formatted_price : "1 234 ₸"  — uses settings.currency_symbol
    net_proceeds    : price after Steam Market 15 % fee + fixed fee
    """

    model_config = {"frozen": True}

    id: str
    name: str
    current_price: float
    volume_7d: int
    is_suspicious: bool
    volatility: float

    # optional — populated when 30-day history is available
    mean_price: float | None = None
    lowest_price: float | None = None

    @computed_field  # type: ignore[misc]
    @property
    def formatted_price(self) -> str:
        """Human-readable price with currency symbol from settings."""
        from config import settings
        return f"{self.current_price:,.0f}\u202f{settings.currency_symbol}"

    @computed_field  # type: ignore[misc]
    @property
    def net_proceeds(self) -> float:
        """
        Net amount received after Steam Market fee.

        Formula: current_price / steam_fee_divisor − steam_fee_fixed
        Example: 1 000 ₸ → 1 000 / 1.15 − 5 = 864.96 ₸
        """
        from config import settings
        return round(
            self.current_price / settings.steam_fee_divisor - settings.steam_fee_fixed,
            2,
        )

    def roi(self, buy_price: float) -> float:
        """
        Return-on-investment relative to buy_price.

        Formula: (net_proceeds / buy_price − 1) × 100   [percent]
        Positive = profit; negative = loss.
        """
        if buy_price <= 0:
            return 0.0
        return round((self.net_proceeds / buy_price - 1) * 100, 2)

    def to_price_dict(self) -> dict:
        """
        Convert to the legacy price-dict shape consumed by engine/investment.py
        (compute_all_investment_signals) and portfolio service helpers.

        Shape: {name: {current_price, mean_price, quantity, lowest_price}}
        """
        return {
            "current_price": self.current_price,
            "mean_price": self.mean_price,
            "quantity": self.volume_7d,
            "lowest_price": self.lowest_price,
        }


class PriceHistoryDTO(BaseModel):
    """
    One price observation — used for sparklines and the detail chart.

    Fields
    ------
    timestamp  : UTC datetime of the observation
    price      : Steam Market price (alias: price in chart dicts)
    mean_price : 30-day rolling mean price (optional)
    volume_7d  : 7-day traded volume
    source     : data origin tag (default "steam_market")
    """

    model_config = {"frozen": True}

    timestamp: datetime
    price: float
    mean_price: float | None = None
    volume_7d: int = 0
    source: str = "steam_market"

    @computed_field  # type: ignore[misc]
    @property
    def formatted_price(self) -> str:
        from config import settings
        return f"{self.price:,.0f}\u202f{settings.currency_symbol}"

    @computed_field  # type: ignore[misc]
    @property
    def iso_timestamp(self) -> str:
        """ISO-8601 string for JSON serialisation."""
        return self.timestamp.isoformat()

    def to_chart_dict(self) -> dict:
        """Return the dict format consumed by chart builder helpers."""
        return {
            "timestamp": self.timestamp.strftime("%Y-%m-%d %H:%M"),
            "price": self.price,
            "mean_price": self.mean_price,
            "volume_7d": self.volume_7d,
        }
