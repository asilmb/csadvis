from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import List


@dataclass(frozen=True)
class ContainerDTO:
    item_id: str
    name: str
    container_type: str
    base_cost: float
    is_blacklisted: bool
    error_count: int


@dataclass(frozen=True)
class PriceDTO:
    item_id: str
    price: float
    timestamp: datetime
    volume_7d: int


class AbstractRepository(ABC):

    @abstractmethod
    def get_all_containers(self) -> List[ContainerDTO]:
        ...

    @abstractmethod
    def add_price_record(
        self,
        item_id: str,
        price: float,
        timestamp: datetime,
    ) -> None:
        ...

    @abstractmethod
    def update_container_tier(self, item_id: str, tier: int) -> None:
        ...

    @abstractmethod
    def get_price_history(self, item_id: str, days: int) -> List[PriceDTO]:
        ...

    @abstractmethod
    def get_market_sync_list(self) -> List[str]:
        ...

    @abstractmethod
    def increment_error_count(self, container_name: str) -> None:
        """Increment error_count; auto-blacklist when count reaches 3."""
        ...

    @abstractmethod
    def get_prices_since(self, cutoff: datetime) -> List[tuple]:
        """Return list of (container_id: str, price: float) since cutoff."""
        ...

    @abstractmethod
    def get_container_id_by_name(self, container_name: str) -> "str | None":
        ...

    @abstractmethod
    def get_max_timestamps_by_container(self) -> "dict[str, datetime]":
        """Return {container_id: max_timestamp} for all containers that have price rows."""
        ...

    @abstractmethod
    def bulk_add_prices(self, rows: "list[dict]") -> None:
        """
        Insert multiple price rows in a single flush.

        Each dict must have:
          container_id: str
          timestamp:    datetime
          price:        float
        Optional keys (default to None/0/"steam_market"):
          volume_7d:    int
          mean_price:   float | None
          lowest_price: float | None
          source:       str
        """
        ...
