"""
SQLAlchemy implementations of domain repository interfaces.

All queries are executed against the Session passed at construction time.
Callers own the Session lifecycle (open / commit / close).

Concrete classes inherit from the ABCs defined in abstract_repo.py:
  SqlAlchemyPositionRepository  → PositionRepository
  SqlAlchemyPriceRepository     → PriceRepository
  SqlAlchemyTaskQueueRepository → TaskQueueRepository
  SqlAlchemyInventoryRepository → InventoryRepository  (repositories.py)

DTOs are defined in abstract_repo.py and re-exported here for backward
compatibility — callers that already import from sql_repositories continue
to work without changes.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime, timedelta

from sqlalchemy.orm import Session

from src.domain.abstract_repo import (
    PositionDTO,
    PositionRepository,
    PriceRepository,
    PriceSnapshotDTO,
    TaskDTO,
    TaskQueueRepository,
)
from src.domain.repositories import InventoryRepository
from src.domain.value_objects import Amount

# Re-export DTOs so that existing `from src.domain.sql_repositories import XxxDTO`
# imports continue to resolve without modification.
__all__ = [
    "PositionDTO",
    "PriceSnapshotDTO",
    "TaskDTO",
    "SqlAlchemyInventoryRepository",
    "SqlAlchemyPositionRepository",
    "SqlAlchemyPriceRepository",
    "SqlAlchemyTaskQueueRepository",
    "get_cookie_status",
    "set_cookie_status",
]


class SqlAlchemyInventoryRepository(InventoryRepository):
    """
    Concrete implementation of InventoryRepository backed by SQLAlchemy.

    Encapsulates the two most expensive portfolio queries:
      - current price + 30-day mean per container   (get_all_items)
      - latest snapshot total balance               (get_total_balance)
    """

    def __init__(self, db: Session) -> None:
        self._db = db

    # ── get_all_items ──────────────────────────────────────────────────────────

    def get_all_items(self) -> list[dict]:
        """
        Bulk-fetch current price data for all tracked containers.
        Uses 2 queries (latest row + 30-day window) — no per-container loop.
        """
        from sqlalchemy import func

        from src.domain.models import DimContainer, FactContainerPrice

        cutoff_30d = datetime.now(UTC).replace(tzinfo=None) - timedelta(days=30)

        containers = self._db.query(DimContainer).all()
        id_to_name = {str(c.container_id): str(c.container_name) for c in containers}

        # Latest price row per container
        latest_ts_subq = (
            self._db.query(
                FactContainerPrice.container_id,
                func.max(FactContainerPrice.timestamp).label("max_ts"),
            )
            .filter(FactContainerPrice.price.isnot(None))
            .group_by(FactContainerPrice.container_id)
            .subquery()
        )
        latest_rows = (
            self._db.query(FactContainerPrice)
            .join(
                latest_ts_subq,
                (FactContainerPrice.container_id == latest_ts_subq.c.container_id)
                & (FactContainerPrice.timestamp == latest_ts_subq.c.max_ts),
            )
            .all()
        )
        latest_map = {str(r.container_id): r for r in latest_rows}

        # 30-day rows for mean price
        recent_rows = (
            self._db.query(FactContainerPrice)
            .filter(
                FactContainerPrice.timestamp >= cutoff_30d,
                FactContainerPrice.price.isnot(None),
            )
            .all()
        )
        prices_by_cid: dict[str, list[float]] = defaultdict(list)
        for r in recent_rows:
            prices_by_cid[str(r.container_id)].append(float(r.price))

        result: list[dict] = []
        for cid, name in id_to_name.items():
            latest = latest_map.get(cid)
            if not latest:
                continue
            prices_30d = prices_by_cid.get(cid, [])
            mean_30d = sum(prices_30d) / len(prices_30d) if prices_30d else None
            result.append(
                {
                    "name": name,
                    "current_price": latest.price,
                    "mean_price": mean_30d,
                    "quantity": int(latest.volume_7d or 0),
                    "lowest_price": latest.lowest_price,
                }
            )
        # Sort: items with stock (quantity > 0) first, then alphabetically by name.
        result.sort(key=lambda r: (0 if (r.get("quantity") or 0) > 0 else 1, r["name"]))
        return result

    # ── update_item_quantity ───────────────────────────────────────────────────

    def update_item_quantity(self, item_name: str, qty: int) -> None:
        """
        Update volume_7d on the latest FactContainerPrice row for item_name.
        No-ops silently when the container or its price row is not found.
        Does NOT commit — caller owns the transaction.
        """
        from sqlalchemy import func

        from src.domain.models import DimContainer, FactContainerPrice

        container = (
            self._db.query(DimContainer)
            .filter(DimContainer.container_name == item_name)
            .first()
        )
        if container is None:
            return

        latest_ts = (
            self._db.query(func.max(FactContainerPrice.timestamp))
            .filter(FactContainerPrice.container_id == container.container_id)
            .scalar()
        )
        if latest_ts is None:
            return

        row = (
            self._db.query(FactContainerPrice)
            .filter(
                FactContainerPrice.container_id == container.container_id,
                FactContainerPrice.timestamp == latest_ts,
            )
            .first()
        )
        if row is not None:
            row.volume_7d = qty  # type: ignore[assignment]

    # ── update_trade_unlock_at ────────────────────────────────────────────────

    def update_trade_unlock_at(self, item_name: str, unlock_at: datetime | None) -> None:
        """
        Set trade_unlock_at on every DimUserPosition row whose container_name
        matches item_name (case-sensitive).

        Passing unlock_at=None clears the trade lock (item is freely tradable).
        Does NOT commit — caller owns the transaction.
        """
        from src.domain.models import DimUserPosition

        rows = (
            self._db.query(DimUserPosition)
            .filter(DimUserPosition.container_name == item_name)
            .all()
        )
        for row in rows:
            row.trade_unlock_at = unlock_at

    # ── is_deeply_trade_banned ────────────────────────────────────────────────

    def is_deeply_trade_banned(
        self,
        container_name: str,
        jit_window_hours: int = 12,
    ) -> bool:
        """
        Return True when ALL DimUserPosition rows for container_name are
        trade-banned with unlock dates more than jit_window_hours in the future.

        Conservative semantics — returns False (allow JIT) when:
          - No DimUserPosition rows exist for the name.
          - At least one row has trade_unlock_at = None (freely tradable).
          - At least one row unlocks within jit_window_hours (prepare-to-sell window).

        Only returns True when every known position is in a deep ban,
        making a JIT price fetch wasteful.
        """
        from src.domain.models import DimUserPosition

        rows = (
            self._db.query(DimUserPosition)
            .filter(DimUserPosition.container_name == container_name)
            .all()
        )

        if not rows:
            return False  # no data → conservative, allow fetch

        soon = datetime.now(UTC).replace(tzinfo=None) + timedelta(hours=jit_window_hours)

        return all(
            row.trade_unlock_at is not None and row.trade_unlock_at > soon
            for row in rows
        )

    # ── get_total_balance ──────────────────────────────────────────────────────

    def get_total_balance(self) -> Amount:
        """
        Return wallet + inventory from the most recent portfolio snapshot.
        Returns Amount(0) when no snapshot has been saved yet.
        """
        from src.domain.models import FactPortfolioSnapshot

        row = (
            self._db.query(FactPortfolioSnapshot)
            .order_by(FactPortfolioSnapshot.snapshot_date.desc())
            .first()
        )
        if row is None:
            return Amount(0)
        wallet = float(row.wallet or 0)
        inventory = float(row.inventory or 0)
        return Amount(wallet + inventory)


# ─── Position Repository ─────────────────────────────────────────────────────


class SqlAlchemyPositionRepository(PositionRepository):
    """
    CRUD repository for the Position ledger (PV-31).

    Session lifecycle is owned by the caller — call db.commit() after mutations.
    """

    def __init__(self, db: Session) -> None:
        self._db = db

    # ── internal helpers ───────────────────────────────────────────────────────

    def _to_dto(self, row) -> PositionDTO:
        return PositionDTO(
            id=str(row.id),
            asset_id=int(row.asset_id),
            market_hash_name=str(row.market_hash_name),
            buy_price=float(row.buy_price),
            quantity=int(row.quantity),
            status=str(row.status),
            opened_at=row.opened_at,
            closed_at=row.closed_at,
            classid=str(row.classid) if row.classid is not None else None,
            market_id=str(row.market_id) if row.market_id is not None else None,
            is_on_market=bool(row.is_on_market),
        )

    # ── public API ─────────────────────────────────────────────────────────────

    def get_open_positions(self) -> list[PositionDTO]:
        """Return all positions with status=OPEN ordered by opened_at DESC."""
        from src.domain.models import Position, PositionStatus

        rows = (
            self._db.query(Position)
            .filter(Position.status == PositionStatus.OPEN)
            .order_by(Position.opened_at.desc())
            .all()
        )
        return [self._to_dto(r) for r in rows]

    def add_position(
        self,
        asset_id: int,
        market_hash_name: str,
        buy_price: float,
        quantity: int = 1,
    ) -> PositionDTO:
        """
        Insert a new OPEN position.  Does NOT commit — caller owns the transaction.

        Delegates construction to Position.open() so domain invariants
        (positive buy_price, quantity ≥ 1) are enforced by the entity.
        """
        from src.domain.models import Position

        row = Position.open(
            asset_id=asset_id,
            market_hash_name=market_hash_name,
            buy_price=buy_price,
            quantity=quantity,
        )
        self._db.add(row)
        return self._to_dto(row)

    def close_position(self, asset_id: int) -> PositionDTO | None:
        """
        Find the first OPEN position with the given asset_id and mark it CLOSED.

        Returns the updated DTO, or None when no matching OPEN position exists.
        Does NOT commit — caller owns the transaction.
        """
        from src.domain.models import Position, PositionStatus

        row = (
            self._db.query(Position)
            .filter(
                Position.asset_id == asset_id,
                Position.status == PositionStatus.OPEN,
            )
            .first()
        )
        if row is None:
            return None

        # Delegate the state transition to the entity — it owns the invariant.
        row.close()
        return self._to_dto(row)

    def update_asset_identity(
        self,
        position_id: str,
        new_asset_id: int,
        new_classid: str | None = None,
        new_market_id: str | None = None,
        is_on_market: bool | None = None,
    ) -> None:
        """Update asset identity fields on a position. Does NOT commit."""
        from src.domain.models import Position

        row = self._db.query(Position).filter(Position.id == position_id).first()
        if row is None:
            return
        # Delegate to entity — keeps field-update logic out of the repository.
        row.update_identity(
            new_asset_id=new_asset_id,
            new_classid=new_classid,
            new_market_id=new_market_id,
            is_on_market=is_on_market,
        )

    def get_open_by_classid(self, classid: str) -> list[PositionDTO]:
        """Return OPEN positions matching classid, ordered by opened_at ASC (FIFO), then id ASC."""
        from src.domain.models import Position, PositionStatus

        rows = (
            self._db.query(Position)
            .filter(
                Position.classid == classid,
                Position.status == PositionStatus.OPEN,
            )
            .order_by(Position.opened_at.asc(), Position.id.asc())
            .all()
        )
        return [self._to_dto(r) for r in rows]

    def get_open_by_market_id(self, market_id: str) -> PositionDTO | None:
        """Return the first OPEN position matching market_id."""
        from src.domain.models import Position, PositionStatus

        row = (
            self._db.query(Position)
            .filter(
                Position.market_id == market_id,
                Position.status == PositionStatus.OPEN,
            )
            .first()
        )
        return self._to_dto(row) if row is not None else None


# ─── Price Repository (JIT valuation) ────────────────────────────────────────


class SqlAlchemyPriceRepository(PriceRepository):
    """
    Read / write access to FactContainerPrice keyed by container_name.

    Only operates on containers that exist in DimContainer (commodity filter).
    Session lifecycle is owned by the caller.
    """

    _STALE_HOURS: int = 1  # threshold for JIT staleness check

    def __init__(self, db: Session) -> None:
        self._db = db

    def is_fresh(self, container_name: str) -> bool:
        """
        Return True when the latest price for container_name exists and
        was recorded within the last STALE_HOURS hours.

        Returns False (stale) when the container is unknown to DimContainer —
        non-commodity items are never fresh from this repository's perspective.
        """
        snapshot = self.get_latest_price(container_name)
        if snapshot is None:
            return False
        cutoff = datetime.now(UTC).replace(tzinfo=None) - timedelta(hours=self._STALE_HOURS)
        return snapshot.timestamp >= cutoff

    def get_latest_price(self, container_name: str) -> PriceSnapshotDTO | None:
        """
        Return the most recent FactContainerPrice for the given container_name,
        or None when the container is not in DimContainer or has no price rows.
        """

        from src.domain.models import DimContainer, FactContainerPrice

        container = (
            self._db.query(DimContainer)
            .filter(DimContainer.container_name == container_name)
            .first()
        )
        if container is None:
            return None

        row = (
            self._db.query(FactContainerPrice)
            .filter(
                FactContainerPrice.container_id == container.container_id,
                FactContainerPrice.price.isnot(None),
            )
            .order_by(FactContainerPrice.timestamp.desc())
            .first()
        )
        if row is None:
            return None

        return PriceSnapshotDTO(
            container_name=container_name,
            price=float(row.price),
            timestamp=row.timestamp,
        )

    def save_jit_price(
        self,
        container_name: str,
        price: float,
        lowest_price: float | None = None,
        volume: int = 0,
        source: str = "jit_valuation",
    ) -> bool:
        """
        Insert a new FactContainerPrice row for the given container.

        Silently skips (returns False) when container_name is not in DimContainer.
        Does NOT commit — caller owns the transaction.
        Returns True on success, False when the container is not found.
        """
        from src.domain.models import DimContainer, FactContainerPrice

        container = (
            self._db.query(DimContainer)
            .filter(DimContainer.container_name == container_name)
            .first()
        )
        if container is None:
            return False

        row = FactContainerPrice(
            container_id=container.container_id,
            price=price,
            lowest_price=lowest_price,
            volume_7d=volume,
            source=source,
        )
        self._db.add(row)
        return True

    def get_price_history(self, container_name: str) -> list[dict]:
        """
        Return all FactContainerPrice rows for container_name ordered by
        timestamp ASC, in the format expected by engine/portfolio_advisor helpers:
        [{"timestamp": "YYYY-MM-DD HH:MM", "price": float, "volume_7d": int}, ...]

        Returns an empty list when container_name is not in DimContainer or has no rows.
        """
        from src.domain.models import DimContainer, FactContainerPrice

        container = (
            self._db.query(DimContainer)
            .filter(DimContainer.container_name == container_name)
            .first()
        )
        if container is None:
            return []

        rows = (
            self._db.query(FactContainerPrice)
            .filter(
                FactContainerPrice.container_id == container.container_id,
                FactContainerPrice.price.isnot(None),
            )
            .order_by(FactContainerPrice.timestamp.asc())
            .all()
        )
        return [
            {
                "timestamp": r.timestamp.strftime("%Y-%m-%d %H:%M"),
                "price": float(r.price),
                "volume_7d": int(r.volume_7d or 0),
            }
            for r in rows
        ]


# ─── Task Queue Repository ────────────────────────────────────────────────────


class SqlAlchemyTaskQueueRepository(TaskQueueRepository):
    """
    CRUD repository for the persistent task queue.

    Session lifecycle is owned by the caller — call db.commit() after mutations.

    Deduplication key: (type, payload). A task is a duplicate when an identical
    row already exists in PENDING or PROCESSING state.
    """

    def __init__(self, db: Session) -> None:
        self._db = db

    # ── internal helpers ───────────────────────────────────────────────────────

    def _to_dto(self, row) -> TaskDTO:  # type: ignore[return]
        return TaskDTO(
            id=str(row.id),
            type=str(row.type),
            priority=int(row.priority),
            status=str(row.status),
            payload=row.payload,
            retries=int(row.retries),
            deadline_at=row.deadline_at,
            created_at=row.created_at,
        )

    # ── public API ─────────────────────────────────────────────────────────────

    def enqueue(
        self,
        task_type: str,
        priority: int,
        payload: dict | None = None,
    ) -> TaskDTO | None:
        """
        Insert a new task.  Returns None (without inserting) when an identical
        active task already exists (deduplication by type + payload).

        Deduplication scope: PENDING, PROCESSING, RETRY.
        Priority upsert: if the duplicate is PENDING with a worse (higher) priority
        number than the incoming request, the existing row is promoted in-place.
        Returns None in both cases — the caller receives no new DTO.
        """
        from src.domain.models import TaskQueue, TaskStatus

        active = (
            self._db.query(TaskQueue)
            .filter(
                TaskQueue.type == task_type,
                TaskQueue.status.in_(
                    [TaskStatus.PENDING, TaskStatus.PROCESSING, TaskStatus.RETRY]
                ),
            )
            .all()
        )
        for row in active:
            if row.payload == payload:
                # Upgrade priority on PENDING duplicates (lower number = higher urgency)
                if row.status == TaskStatus.PENDING and row.priority > priority:
                    row.priority = priority
                    row.updated_at = datetime.now(UTC).replace(tzinfo=None)
                return None  # deduplicated — no new row

        row = TaskQueue(type=task_type, priority=priority, payload=payload)
        self._db.add(row)
        return self._to_dto(row)

    def pick_task(self) -> TaskDTO | None:
        """
        Atomically claim the highest-priority PENDING or RETRY task.

        Uses a single UPDATE … WHERE id = (SELECT … LIMIT 1) RETURNING * so
        that concurrent workers can never pick the same row twice (no TOCTOU).
        """
        import sqlalchemy

        result = self._db.execute(
            sqlalchemy.text(
                """
                UPDATE task_queue
                SET status = 'PROCESSING',
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = (
                    SELECT id FROM task_queue
                    WHERE status IN ('PENDING', 'RETRY')
                    ORDER BY priority ASC, created_at ASC
                    LIMIT 1
                )
                RETURNING id, type, priority, status, payload, retries,
                          deadline_at, created_at
                """
            )
        )
        row = result.fetchone()
        if row is None:
            return None
        import json

        raw_payload = row[4]
        payload = json.loads(raw_payload) if isinstance(raw_payload, str) else raw_payload
        return TaskDTO(
            id=str(row[0]),
            type=str(row[1]),
            priority=int(row[2]),
            status=str(row[3]),
            payload=payload,
            retries=int(row[5]),
            deadline_at=row[6],
            created_at=row[7],
        )

    def complete(self, task_id: str) -> None:
        """Mark task COMPLETED and stamp completed_at / updated_at."""
        from src.domain.models import TaskQueue, TaskStatus

        now = datetime.now(UTC).replace(tzinfo=None)
        row = self._db.query(TaskQueue).filter(TaskQueue.id == task_id).first()
        if row is not None:
            row.status = TaskStatus.COMPLETED
            row.completed_at = now
            row.updated_at = now

    def fail(self, task_id: str, max_retries: int = 3, error_msg: str | None = None) -> str:
        """
        Increment retries counter.  Moves to RETRY when retries < max_retries,
        otherwise FAILED.  Returns the new status string.
        """
        from src.domain.models import TaskQueue, TaskStatus

        now = datetime.now(UTC).replace(tzinfo=None)
        row = self._db.query(TaskQueue).filter(TaskQueue.id == task_id).first()
        if row is None:
            return str(TaskStatus.FAILED)
        row.retries = (row.retries or 0) + 1
        row.status = TaskStatus.RETRY if row.retries < max_retries else TaskStatus.FAILED
        row.updated_at = now
        if error_msg:
            row.error_message = error_msg[:500]  # truncate to column length
        return str(row.status)

    def pause_auth(self, task_id: str) -> None:
        """Mark task PAUSED_AUTH — auth loop detected; task waits for cookie update."""
        from src.domain.models import TaskQueue, TaskStatus

        row = self._db.query(TaskQueue).filter(TaskQueue.id == task_id).first()
        if row is not None:
            row.status = TaskStatus.PAUSED_AUTH
            row.updated_at = datetime.now(UTC).replace(tzinfo=None)

    def requeue_pending(self, task_id: str) -> None:
        """Reset a task back to PENDING without incrementing the retry counter (network backoff)."""
        from src.domain.models import TaskQueue, TaskStatus

        row = self._db.query(TaskQueue).filter(TaskQueue.id == task_id).first()
        if row is not None:
            row.status = TaskStatus.PENDING
            row.updated_at = datetime.now(UTC).replace(tzinfo=None)

    def create_processing(
        self, task_type: str, payload: dict | None = None, priority: int = 3
    ) -> TaskDTO:
        """
        Insert a task directly in PROCESSING state (bypasses PENDING→worker flow).

        Used by subsystems that manage their own execution lifecycle (e.g. scraper)
        and only need the task_queue row for UI visibility.  Workers do not pick
        PROCESSING tasks — they only claim PENDING/RETRY rows.
        """
        import json as _json

        from src.domain.models import TaskQueue, TaskStatus

        row = TaskQueue(type=task_type, priority=priority, payload=payload)
        row.status = TaskStatus.PROCESSING
        self._db.add(row)
        return self._to_dto(row)

    def update_task_progress(self, task_id: str, progress: dict) -> None:
        """Merge progress dict into task payload for live UI visibility (best-effort)."""
        from src.domain.models import TaskQueue

        row = self._db.query(TaskQueue).filter(TaskQueue.id == task_id).first()
        if row is not None:
            current = dict(row.payload or {})
            current.update(progress)
            row.payload = current
            row.updated_at = datetime.now(UTC).replace(tzinfo=None)

    def has_paused_auth_tasks(self) -> bool:
        """Return True if any task is currently in PAUSED_AUTH state."""
        from src.domain.models import TaskQueue, TaskStatus

        return (
            self._db.query(TaskQueue)
            .filter(TaskQueue.status == TaskStatus.PAUSED_AUTH)
            .first()
        ) is not None


# ─── Cookie status (PV-43) ────────────────────────────────────────────────────


def get_cookie_status(db) -> str:
    """Return current cookie status: 'VALID' | 'EXPIRED' | 'UNKNOWN'."""
    from src.domain.models import SystemSettings
    row = db.get(SystemSettings, "cookie_status")
    return row.value if row and row.value else "UNKNOWN"


def set_cookie_status(db, status: str) -> None:
    """Upsert cookie_status key in SystemSettings. Caller must commit."""
    from src.domain.models import SystemSettings
    from datetime import datetime, timezone
    row = db.get(SystemSettings, "cookie_status")
    if row is None:
        row = SystemSettings(key="cookie_status", value=status)
        db.add(row)
    else:
        row.value = status
        row.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
