"""
Domain repository interfaces.

These ABCs define the contract between the domain/services layer and the
infrastructure layer (SQLAlchemy). Concrete implementations live in
database/repositories.py and must not import src.domain logic.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from src.domain.value_objects import Amount


class InventoryRepository(ABC):
    """Abstract contract for inventory and portfolio balance data access."""

    @abstractmethod
    def get_all_items(self) -> list[dict]:
        """
        Return current price data for every tracked container.

        Each dict shape:
            {
                "name":          str,
                "current_price": float | None,
                "mean_price":    float | None,   # 30-day mean
                "quantity":      int,             # 7-day traded volume
                "lowest_price":  float | None,
            }
        """

    @abstractmethod
    def update_item_quantity(self, item_name: str, qty: int) -> None:
        """
        Persist an updated daily-volume figure for a container.

        Updates the volume_7d field of the latest FactContainerPrice row
        whose container name matches item_name.
        Does NOT commit — caller owns the transaction.
        """

    @abstractmethod
    def get_total_balance(self) -> Amount:
        """
        Return the total portfolio value (wallet + inventory) from the most
        recent FactPortfolioSnapshot row.  Returns Amount(0) when no snapshot exists.
        """
