"""
Simplified schema — investment model only.

Tables:
  dim_containers           — case/capsule metadata
  fact_container_prices    — price history per container (time-series)
  dim_user_positions       — user's open positions (buy price, qty) for P&L / stop-loss
  fact_portfolio_snapshots — daily wallet + inventory value snapshots
  fact_transactions        — manual trade log (buy/sell history)
  dim_annual_summary       — yearly P&L summary (manual input)
  event_log                — domain event audit log (PV-17)
  task_queue               — persistent background task queue (PV-25)
  worker_registry          — worker heartbeat / status registry (PV-28)
  positions                — inventory position ledger with asset_id (PV-31)
"""

import uuid
from datetime import UTC, datetime
from enum import StrEnum

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy import JSON
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


class ContainerType(StrEnum):
    Weapon_Case = "Weapon Case"
    Souvenir_Package = "Souvenir Package"
    Sealed_Terminal = "Sealed Terminal"
    Sticker_Capsule = "Sticker Capsule"
    Autograph_Capsule = "Autograph Capsule"
    Event_Capsule = "Event Capsule"


class DimContainer(Base):
    __tablename__ = "dim_containers"

    container_id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    container_name = Column(String(200), unique=True, nullable=False, index=True)
    container_type: Column = Column(Enum(ContainerType), nullable=False)
    base_cost = Column(Float, nullable=False)  # key+case for weapon cases, fiat for capsules

    error_count = Column(Integer, nullable=False, default=0)       # PV-50: consecutive Steam errors
    is_blacklisted = Column(Integer, nullable=False, default=0)   # PV-50: skip-list flag (0/1)

    price_history = relationship("FactContainerPrice", back_populates="container", lazy="dynamic")

    def __repr__(self) -> str:
        return f"<DimContainer {self.container_name} base_cost={self.base_cost}>"


class FactContainerPrice(Base):
    """Price snapshot per container, polled every hour by the scheduler."""

    __tablename__ = "fact_container_prices"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    container_id = Column(String(36), ForeignKey("dim_containers.container_id"), nullable=False)
    timestamp = Column(
        DateTime, default=lambda: datetime.now(UTC).replace(tzinfo=None), nullable=False, index=True
    )
    price = Column(Float, nullable=True)  # Steam median price
    mean_price = Column(Float, nullable=True)  # 30-day mean price
    lowest_price = Column(Float, nullable=True)  # Steam lowest listing price
    volume_7d = Column(Integer, nullable=True, default=0)
    source = Column(String(50), default="steam_market")

    container = relationship("DimContainer", back_populates="price_history")

    __table_args__ = (Index("ix_container_price_ts", "container_id", "timestamp"),)


class DimUserPosition(Base):
    """User-defined open positions for P&L tracking and stop-loss alerts."""

    __tablename__ = "dim_user_positions"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    container_name = Column(String(200), nullable=False, index=True)
    buy_price = Column(Float, nullable=False)  # price paid per unit
    quantity = Column(Integer, nullable=False, default=1)
    buy_date = Column(DateTime, nullable=True)
    trade_unlock_at = Column(DateTime, nullable=True)  # PV-29: Steam trade ban expiry

    def __repr__(self) -> str:
        return (
            f"<DimUserPosition {self.container_name} x{self.quantity} @ {self.buy_price:.0f}>"
        )


class FactPortfolioSnapshot(Base):
    """Daily snapshot of wallet balance + inventory value (for 30-day chart)."""

    __tablename__ = "fact_portfolio_snapshots"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    snapshot_date = Column(DateTime, nullable=False, index=True)
    wallet = Column(Float, nullable=False)
    inventory = Column(Float, nullable=True, default=0)

    def __repr__(self) -> str:
        return f"<Snapshot {self.snapshot_date.date()} wallet={self.wallet:.0f} inv={self.inventory:.0f}>"


class FactTransaction(Base):
    """Manual trade log entry."""

    __tablename__ = "fact_transactions"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    trade_date = Column(DateTime, nullable=False, index=True)
    action = Column(String(20), nullable=False)  # BUY / SELL / FLIP
    item_name = Column(String(200), nullable=False)
    quantity = Column(Integer, default=1)
    price = Column(Float, nullable=False)  # price per unit
    total = Column(Float, nullable=False)  # price × qty
    pnl = Column(Float, nullable=True)  # P&L for this trade (SELL only)
    listing_id = Column(String(64), nullable=True, index=True)  # Steam listing ID for dedup
    notes = Column(String(500), nullable=True)

    def __repr__(self) -> str:
        return f"<Transaction {self.trade_date.date()} {self.action} {self.item_name}>"


class DimAnnualSummary(Base):
    """Yearly P&L summary — manual input."""

    __tablename__ = "dim_annual_summary"

    year = Column(Integer, primary_key=True)
    pnl = Column(Float, nullable=False)
    notes = Column(String(500), nullable=True)

    def __repr__(self) -> str:
        return f"<AnnualSummary {self.year} pnl={self.pnl:.0f}>"


class FactPortfolioAdvice(Base):
    """Single-row cache of allocate_portfolio() output. Replaced on every refresh."""

    __tablename__ = "fact_portfolio_advice"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    computed_at = Column(DateTime, nullable=False, index=True)
    wallet = Column(Float, nullable=False)
    total_capital = Column(Float, nullable=False)
    inventory_value = Column(Float, nullable=False)
    flip_budget = Column(Float, nullable=False)
    invest_budget = Column(Float, nullable=False)
    reserve_amount = Column(Float, nullable=False)
    flip_json = Column(String, nullable=True)  # JSON blob: best_flip dict
    invest_json = Column(String, nullable=True)  # JSON blob: best_invest dict
    top_flips_json = Column(String, nullable=True)  # JSON blob: list of up to 5 flip candidates
    top_invests_json = Column(String, nullable=True)  # JSON blob: list of up to 5 invest candidates
    sell_json = Column(String, nullable=True)  # JSON blob: list of sell candidates
    correlation_warning = Column(String, nullable=True)

    def __repr__(self) -> str:
        return f"<FactPortfolioAdvice computed_at={self.computed_at}>"


class FactInvestmentSignal(Base):
    """Per-container investment signal cache. All rows replaced on every refresh."""

    __tablename__ = "fact_investment_signals"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    container_id = Column(String(36), ForeignKey("dim_containers.container_id"), nullable=False)
    computed_at = Column(DateTime, nullable=False, index=True)
    verdict = Column(String(50), nullable=False)  # BUY / HOLD / SELL / SELL_AT_LOSS / UNCERTAIN
    score = Column(Integer, nullable=False)
    ratio_signal = Column(String(50), nullable=True)
    momentum_signal = Column(String(50), nullable=True)
    trend_signal = Column(String(50), nullable=True)
    event_signal = Column(String(50), nullable=True)
    sell_at_loss = Column(Integer, nullable=False, default=0)  # boolean 0/1
    unrealized_pnl = Column(Float, nullable=True)

    __table_args__ = (Index("ix_investment_signal_computed_at", "computed_at"),)

    def __repr__(self) -> str:
        return f"<FactInvestmentSignal {self.container_id} {self.verdict} @ {self.computed_at}>"


# ─── Phase 4 — Infrastructure & Reliability ───────────────────────────────────


class EventLog(Base):
    """Domain event audit log — written by SignalHandler (PV-17)."""

    __tablename__ = "event_log"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    timestamp = Column(
        DateTime,
        nullable=False,
        default=lambda: datetime.now(UTC).replace(tzinfo=None),
        index=True,
    )
    level = Column(String(20), nullable=False)       # DEBUG / INFO / WARNING / ERROR / CRITICAL
    module = Column(String(100), nullable=False)     # originating module name
    message = Column(String(1000), nullable=False)

    def __repr__(self) -> str:
        return f"<EventLog {self.timestamp} [{self.level}] {self.module}: {self.message[:60]}>"


class TaskStatus(StrEnum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    RETRY = "RETRY"
    FAILED = "FAILED"
    COMPLETED = "COMPLETED"
    PAUSED_AUTH = "PAUSED_AUTH"


class TaskQueue(Base):
    """Persistent background task queue — survives process restarts (PV-25)."""

    __tablename__ = "task_queue"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    type = Column(String(100), nullable=False)       # task type name, e.g. "price_poll"
    priority = Column(Integer, nullable=False, default=5)  # lower value = higher priority
    status = Column(
        Enum(TaskStatus), nullable=False, default=TaskStatus.PENDING, index=True
    )
    payload = Column(JSON, nullable=True)            # arbitrary task parameters
    retries = Column(Integer, nullable=False, default=0)
    deadline_at = Column(DateTime, nullable=True)    # None = no deadline
    created_at = Column(
        DateTime,
        nullable=False,
        default=lambda: datetime.now(UTC).replace(tzinfo=None),
    )
    updated_at = Column(DateTime, nullable=True)     # set on every status transition
    completed_at = Column(DateTime, nullable=True)   # set when status → COMPLETED
    error_message = Column(Text, nullable=True)      # last error from fail()

    __table_args__ = (Index("ix_task_queue_status_priority", "status", "priority"),)

    def __repr__(self) -> str:
        return f"<TaskQueue {self.type!r} [{self.status}] priority={self.priority}>"


class WorkerRegistry(Base):
    """Worker heartbeat and status registry — one row per named worker (PV-28)."""

    __tablename__ = "worker_registry"

    name = Column(String(100), primary_key=True)    # e.g. "price_poller", "backfill"
    status = Column(String(20), nullable=False, default="IDLE", index=True)  # IDLE/BUSY/DEAD
    last_heartbeat = Column(DateTime, nullable=True)
    current_task_id = Column(String(36), nullable=True)  # FK-free; task may be deleted

    def __repr__(self) -> str:
        return f"<WorkerRegistry {self.name!r} [{self.status}]>"


class PositionStatus(StrEnum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"


# ─── Position domain exceptions ───────────────────────────────────────────────


class PositionError(Exception):
    """Base exception for invariant violations on the Position entity."""


class PositionAlreadyClosedError(PositionError):
    """
    Raised when a state-change method is called on an already-CLOSED position.

    Example:
        position.close()  # first call — OK
        position.close()  # second call — raises PositionAlreadyClosedError
    """


class InvalidPositionError(PositionError):
    """
    Raised when Position.open() is called with arguments that violate domain
    invariants (e.g. non-positive buy_price or zero quantity).
    """


# ─── Position entity ──────────────────────────────────────────────────────────


class Position(Base):
    """Inventory position ledger — tracks individual Steam assets for P&L (PV-31)."""

    __tablename__ = "positions"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    asset_id = Column(BigInteger, nullable=False, index=True)   # Steam 64-bit asset ID (PV-33)
    classid = Column(String(64), nullable=True, index=True)     # Steam classid — groups same item type (PV-33)
    market_id = Column(String(64), nullable=True, index=True)   # Steam listing ID when listed on market (PV-33)
    is_on_market = Column(Integer, nullable=False, default=0)   # 0=in inventory 1=listed on market (PV-33)
    market_hash_name = Column(String(200), nullable=False, index=True)
    buy_price = Column(Float, nullable=False)        # KZT paid per unit
    quantity = Column(Integer, nullable=False, default=1)
    status = Column(
        Enum(PositionStatus), nullable=False, default=PositionStatus.OPEN, index=True
    )
    opened_at = Column(
        DateTime,
        nullable=False,
        default=lambda: datetime.now(UTC).replace(tzinfo=None),
    )
    closed_at = Column(DateTime, nullable=True)      # populated on CLOSED

    __table_args__ = (Index("ix_positions_name_status", "market_hash_name", "status"),)

    def __repr__(self) -> str:
        return (
            f"<Position {self.market_hash_name!r} asset={self.asset_id}"
            f" x{self.quantity} @ {self.buy_price:.0f} [{self.status}]>"
        )

    # ── Factory ───────────────────────────────────────────────────────────────

    @classmethod
    def open(
        cls,
        asset_id: int,
        market_hash_name: str,
        buy_price: float,
        quantity: int = 1,
    ) -> "Position":
        """
        Create a new OPEN position.

        Enforces domain invariants before construction:
          - buy_price must be > 0
          - quantity must be >= 1

        Raises InvalidPositionError on violation.
        Does NOT add to the session — caller is responsible.
        """
        if buy_price <= 0:
            raise InvalidPositionError(
                f"buy_price must be positive, got {buy_price!r}"
            )
        if quantity < 1:
            raise InvalidPositionError(
                f"quantity must be at least 1, got {quantity!r}"
            )
        return cls(
            asset_id=asset_id,
            market_hash_name=market_hash_name,
            buy_price=buy_price,
            quantity=quantity,
            # Explicitly set status so the invariant holds in pure-Python tests
            # (SQLAlchemy column defaults only fire at INSERT time, not at
            # object-construction time).
            status=PositionStatus.OPEN,
        )

    # ── State transitions ─────────────────────────────────────────────────────

    def close(self) -> None:
        """
        Transition this position from OPEN → CLOSED.

        Sets closed_at to the current UTC timestamp.
        Raises PositionAlreadyClosedError when the position is already CLOSED —
        prevents the entity from entering an inconsistent state (double-close).

        Does NOT flush or commit — the caller (repository) owns the transaction.
        """
        if self.status == PositionStatus.CLOSED:
            raise PositionAlreadyClosedError(
                f"Position {self.id!r} ({self.market_hash_name!r}) is already CLOSED"
            )
        self.status = PositionStatus.CLOSED
        self.closed_at = datetime.now(UTC).replace(tzinfo=None)

    def update_identity(
        self,
        new_asset_id: int,
        new_classid: str | None = None,
        new_market_id: str | None = None,
        is_on_market: bool | None = None,
    ) -> None:
        """
        Update Steam asset identity fields in-place.

        Called by the reconciler when a newer asset_id, classid, or market_id
        is discovered for this position (e.g. after inventory re-index).
        Does NOT flush or commit — caller owns the transaction.
        """
        self.asset_id = new_asset_id
        if new_classid is not None:
            self.classid = new_classid
        if new_market_id is not None:
            self.market_id = new_market_id
        if is_on_market is not None:
            self.is_on_market = int(is_on_market)

    def list_on_market(self, market_id: str) -> None:
        """
        Mark this position as actively listed on Steam Market.

        Sets is_on_market = 1 and records the Steam listing ID.
        Raises PositionAlreadyClosedError when called on a CLOSED position —
        a sold item cannot be listed again through this entity.
        Does NOT flush or commit — caller owns the transaction.
        """
        if self.status == PositionStatus.CLOSED:
            raise PositionAlreadyClosedError(
                f"Cannot list CLOSED position {self.id!r} on market"
            )
        self.market_id = market_id
        self.is_on_market = 1

    def delist_from_market(self) -> None:
        """
        Mark this position as returned from Steam Market back to inventory.

        Clears is_on_market but preserves market_id for history.
        Does NOT flush or commit — caller owns the transaction.
        """
        self.is_on_market = 0


class SystemSettings(Base):
    """Generic key-value store for runtime system configuration (PV-43)."""

    __tablename__ = "system_settings"

    key = Column(String(100), primary_key=True)
    value = Column(Text, nullable=True)
    updated_at = Column(
        DateTime,
        nullable=False,
        default=lambda: datetime.now(UTC).replace(tzinfo=None),
        onupdate=lambda: datetime.now(UTC).replace(tzinfo=None),
    )
