"""
Domain repository abstractions.

This module defines *all* abstract repository interfaces used by the domain and
application layers.  Concrete implementations live in sql_repositories.py and
must not be imported from here — only the abstractions are referenced across
layer boundaries (Dependency Inversion Principle).

ABCs defined here
-----------------
  AbstractRepository      — legacy container-price sync interface (retained for
                            backward compat with postgres_repo / factory)
  PositionRepository      — position ledger (trade ledger, reconciler)
  PriceRepository         — JIT price read/write (market validator, armory advisor)
  TaskQueueRepository     — persistent task queue (task manager, Celery tasks)

DTOs defined here (pure value objects, no infrastructure imports)
-----------------------------------------------------------------
  ContainerDTO            — used by AbstractRepository
  PriceDTO                — used by AbstractRepository
  PositionDTO             — used by PositionRepository
  PriceSnapshotDTO        — used by PriceRepository
  TaskDTO                 — used by TaskQueueRepository
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import List


# ─── Legacy DTOs (AbstractRepository) ────────────────────────────────────────


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


# ─── Position DTOs ────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class PositionDTO:
    """Immutable position snapshot — safe to pass across layer boundaries."""

    id: str
    asset_id: int
    market_hash_name: str
    buy_price: float        # price paid per unit
    quantity: int
    status: str             # "OPEN" | "CLOSED"
    opened_at: datetime
    closed_at: datetime | None
    classid: str | None = None      # Steam classid — groups same item type (PV-33)
    market_id: str | None = None    # Steam listing ID when listed on market (PV-33)
    is_on_market: bool = False      # True when item is currently listed (PV-33)


# ─── Price DTOs ───────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class PriceSnapshotDTO:
    """Latest known price for a container — returned by PriceRepository."""

    container_name: str
    price: float
    timestamp: datetime


# ─── Task Queue DTOs ──────────────────────────────────────────────────────────


@dataclass(frozen=True)
class TaskDTO:
    """Immutable task snapshot — safe to pass across layer boundaries."""

    id: str
    type: str
    priority: int
    status: str
    payload: dict | None
    retries: int
    deadline_at: datetime | None
    created_at: datetime


# ─── Legacy abstract interface (retained for backward compat) ─────────────────


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
    def get_container_id_by_name(self, container_name: str) -> str | None:
        ...

    @abstractmethod
    def get_max_timestamps_by_container(self) -> dict[str, datetime]:
        """Return {container_id: max_timestamp} for all containers that have price rows."""
        ...

    @abstractmethod
    def bulk_add_prices(self, rows: list[dict]) -> None:
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


# ─── PositionRepository ───────────────────────────────────────────────────────


class PositionRepository(ABC):
    """
    Abstract contract for the position ledger.

    Consumers (TradeLedger, PositionReconciler) depend only on this interface;
    the SQLAlchemy implementation is injected at the application boundary.
    Session lifecycle is owned by the caller — commit after mutations.
    """

    @abstractmethod
    def get_open_positions(self) -> list[PositionDTO]:
        """Return all OPEN positions ordered by opened_at DESC."""

    @abstractmethod
    def add_position(
        self,
        asset_id: int,
        market_hash_name: str,
        buy_price: float,
        quantity: int = 1,
    ) -> PositionDTO:
        """
        Insert a new OPEN position.

        Does NOT commit — caller owns the transaction.
        Returns an immutable PositionDTO for the new row.
        """

    @abstractmethod
    def close_position(self, asset_id: int) -> PositionDTO | None:
        """
        Find the first OPEN position with the given asset_id and mark it CLOSED.

        Returns the updated DTO, or None when no matching OPEN position exists.
        Does NOT commit — caller owns the transaction.
        """

    @abstractmethod
    def update_asset_identity(
        self,
        position_id: str,
        new_asset_id: int,
        new_classid: str | None = None,
        new_market_id: str | None = None,
        is_on_market: bool | None = None,
    ) -> None:
        """
        Update Steam asset identity fields on a position row.

        Does NOT commit — caller owns the transaction.
        No-ops silently when position_id is not found.
        """

    @abstractmethod
    def get_open_by_classid(self, classid: str) -> list[PositionDTO]:
        """
        Return OPEN positions matching classid, FIFO ordered (opened_at ASC, id ASC).
        """

    @abstractmethod
    def get_open_by_market_id(self, market_id: str) -> PositionDTO | None:
        """Return the first OPEN position matching market_id, or None."""


# ─── PriceRepository ─────────────────────────────────────────────────────────


class PriceRepository(ABC):
    """
    Abstract contract for JIT price read/write access.

    Operations are scoped to containers registered in DimContainer — unknown
    container names are silently ignored (commodity filter).
    Session lifecycle is owned by the caller.
    """

    @abstractmethod
    def is_fresh(self, container_name: str) -> bool:
        """
        Return True when the latest price for container_name exists and was
        recorded within the freshness window.

        Returns False when the container is not in DimContainer.
        """

    @abstractmethod
    def get_latest_price(self, container_name: str) -> PriceSnapshotDTO | None:
        """
        Return the most recent price snapshot for container_name, or None when
        the container is unknown or has no price rows.
        """

    @abstractmethod
    def save_jit_price(
        self,
        container_name: str,
        price: float,
        lowest_price: float | None = None,
        volume: int = 0,
        source: str = "jit_valuation",
    ) -> bool:
        """
        Insert a new price row for the given container.

        Returns True on success, False when the container is not in DimContainer.
        Does NOT commit — caller owns the transaction.
        """

    @abstractmethod
    def get_price_history(self, container_name: str) -> list[dict]:
        """
        Return all price rows for container_name ordered by timestamp ASC.

        Shape: [{"timestamp": "YYYY-MM-DD HH:MM", "price": float, "volume_7d": int}]
        Returns an empty list when the container is unknown or has no rows.
        """


# ─── TaskQueueRepository ─────────────────────────────────────────────────────


class TaskQueueRepository(ABC):
    """
    Abstract contract for the persistent task queue.

    Deduplication key: (type, payload). A task is a duplicate when an identical
    row already exists in PENDING, PROCESSING, or RETRY state.
    Session lifecycle is owned by the caller — commit after mutations.
    """

    @abstractmethod
    def enqueue(
        self,
        task_type: str,
        priority: int,
        payload: dict | None = None,
    ) -> TaskDTO | None:
        """
        Insert a new task.

        Returns None (without inserting) when an identical active task already
        exists (deduplication by type + payload).  When the duplicate is PENDING
        with a worse priority number, the existing row is promoted in-place and
        None is still returned.
        """

    @abstractmethod
    def pick_task(self) -> TaskDTO | None:
        """
        Atomically claim the highest-priority PENDING or RETRY task.

        Marks the claimed row PROCESSING and returns its DTO.
        Returns None when the queue is empty.
        """

    @abstractmethod
    def complete(self, task_id: str) -> None:
        """Mark task COMPLETED and stamp completed_at / updated_at. No-ops for unknown ids."""

    @abstractmethod
    def fail(
        self,
        task_id: str,
        max_retries: int = 3,
        error_msg: str | None = None,
    ) -> str:
        """
        Increment the retries counter.

        Moves to RETRY when retries < max_retries, otherwise FAILED.
        Returns the new status string.  Returns "FAILED" for unknown ids.
        """

    @abstractmethod
    def pause_auth(self, task_id: str) -> None:
        """Mark task PAUSED_AUTH — auth loop detected; waits for cookie update."""

    @abstractmethod
    def requeue_pending(self, task_id: str) -> None:
        """Reset a task back to PENDING without incrementing the retry counter."""

    @abstractmethod
    def create_processing(
        self,
        task_type: str,
        payload: dict | None = None,
        priority: int = 3,
    ) -> TaskDTO:
        """
        Insert a task directly in PROCESSING state (bypasses PENDING→worker flow).

        Used by subsystems that manage their own execution lifecycle.
        """

    @abstractmethod
    def update_task_progress(self, task_id: str, progress: dict) -> None:
        """Merge progress dict into task payload for live UI visibility (best-effort)."""

    @abstractmethod
    def has_paused_auth_tasks(self) -> bool:
        """Return True if any task is currently in PAUSED_AUTH state."""
