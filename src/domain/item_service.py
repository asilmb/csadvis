"""
ItemService — Service Layer for CS2 Market Analytics (PV-05).

Owns all financial computations that were previously scattered across:
  - frontend/helpers.py  (_get_current_steam_prices inline logic)
  - frontend/callbacks.py (fallback signal computation)
  - engine/investment.py  (still delegates signal verdict logic there)

Public API
----------
ItemService(repo)                           — inject via InventoryRepository
  .get_market_overview()  → list[ItemDTO]   — main table data + volatility
  .get_item_details(id)   → ItemDTO | None  — detail panel data
  .get_signals()          → dict            — BUY/HOLD/SELL per container
  .process_new_price(id, raw_price) → bool  — validated save (Celery entry)

Financial calculations encapsulated here
-----------------------------------------
  volatility     = std(prices_30d) / mean(prices_30d) × 100   (coeff. of variation)
  is_suspicious  = current > mean × 1.20  (20 % spike above 30-day mean)
  net_proceeds   = price / fee_divisor − fee_fixed              (on ItemDTO)
  roi(buy)       = (net_proceeds / buy − 1) × 100              (on ItemDTO)
"""

from __future__ import annotations

import math
import time
from collections import defaultdict
from datetime import UTC, datetime, timedelta

import structlog

from src.domain.repositories import InventoryRepository

logger = structlog.get_logger()

# ── Validation thresholds for process_new_price ───────────────────────────────
_PRICE_MIN: float = 0.01       # KZT — prices at or below this are invalid
_PRICE_MAX: float = 1_000_000  # KZT — sanity cap; rejects obviously wrong values

# ── Anomaly detection (PV-06) ─────────────────────────────────────────────────
_SANITY_WINDOW_DAYS: int = 7         # rolling window for median computation
_SANITY_MIN_SAMPLES: int = 3         # skip check when history is too thin
_SANITY_THRESHOLD: float = 0.50      # 50 % deviation from median → anomaly


# ─── Helpers ──────────────────────────────────────────────────────────────────


def _coeff_of_variation(prices: list[float]) -> float:
    """
    Coefficient of variation as a percentage.

    Returns 0.0 when fewer than 2 data points or mean is zero.
    """
    if len(prices) < 2:
        return 0.0
    mean = sum(prices) / len(prices)
    if mean == 0:
        return 0.0
    variance = sum((p - mean) ** 2 for p in prices) / len(prices)
    return round(math.sqrt(variance) / mean * 100, 2)


def _median(values: list[float]) -> float | None:
    """Return the median of a non-empty list, or None if empty."""
    if not values:
        return None
    s = sorted(values)
    n = len(s)
    mid = n // 2
    return s[mid] if n % 2 else (s[mid - 1] + s[mid]) / 2.0


def _is_price_suspicious(current: float, mean: float | None) -> bool:
    """
    Flag a price as suspicious when it is more than 20 % above its 30-day mean.

    A spike *below* the mean (super-deal territory) is not flagged — that is
    intentional signal data captured by the investment engine.
    """
    if mean is None or mean <= 0:
        return False
    return current > mean * 1.20


# ─── Service ──────────────────────────────────────────────────────────────────


class ItemService:
    """
    Service layer for market item data.

    Accepts any InventoryRepository implementation (dependency injection).
    For operations that need the price repository (price history, price writes)
    the service reaches into the concrete repo's ``_db`` session via duck typing;
    production code always passes SqlAlchemyInventoryRepository so this is safe.
    """

    def __init__(self, repo: InventoryRepository) -> None:
        self._repo = repo
        # Extract the SQLAlchemy Session from the concrete repo (duck-typed).
        # Falls back to None; affected methods will open their own sessions.
        self._db = getattr(repo, "_db", None)

    # ── Public API ────────────────────────────────────────────────────────────

    def get_market_overview(self) -> list:
        """
        Return current market snapshot for all tracked containers.

        Steps
        -----
        1. repo.get_all_items() — current prices, 30-day mean, volume_7d.
        2. Bulk load container IDs and metadata from DimContainer.
        3. Bulk load 30-day price window to compute per-item volatility.
        4. Assemble ItemDTO per container, sorted by volume desc then name.

        Returns
        -------
        list[ItemDTO]
        """
        from src.domain.dtos import ItemDTO

        # ── Step 1: base data from InventoryRepository ────────────────────────
        base_items: list[dict] = self._repo.get_all_items()
        if not base_items:
            return []

        # ── Step 2: container metadata (id, is_blacklisted) ──────────────────
        name_to_meta: dict[str, dict] = {}
        if self._db is not None:
            try:
                from src.domain.models import DimContainer
                rows = self._db.query(
                    DimContainer.container_id,
                    DimContainer.container_name,
                    DimContainer.is_blacklisted,
                ).all()
                for row in rows:
                    name_to_meta[str(row.container_name)] = {
                        "id": str(row.container_id),
                        "is_blacklisted": bool(row.is_blacklisted),
                    }
            except Exception as exc:
                logger.warning("container_metadata_failed", service="item_service", error=str(exc))

        # ── Step 3: 30-day price window for volatility ────────────────────────
        prices_by_name: dict[str, list[float]] = defaultdict(list)
        if self._db is not None:
            try:
                from src.domain.models import DimContainer, FactContainerPrice
                cutoff = datetime.now(UTC).replace(tzinfo=None) - timedelta(days=30)
                recent = (
                    self._db.query(
                        DimContainer.container_name,
                        FactContainerPrice.price,
                    )
                    .join(
                        FactContainerPrice,
                        DimContainer.container_id == FactContainerPrice.container_id,
                    )
                    .filter(
                        FactContainerPrice.timestamp >= cutoff,
                        FactContainerPrice.price.isnot(None),
                    )
                    .all()
                )
                for row in recent:
                    prices_by_name[str(row.container_name)].append(float(row.price))
            except Exception as exc:
                logger.warning("price_window_query_failed", service="item_service", error=str(exc))

        # ── Step 4: assemble ItemDTOs ─────────────────────────────────────────
        result: list[ItemDTO] = []
        for item in base_items:
            name: str = item["name"]
            current: float = float(item.get("current_price") or 0)
            if current <= 0:
                continue

            mean: float | None = item.get("mean_price")
            if mean is not None:
                mean = float(mean)

            prices_30d = prices_by_name.get(name, [])
            volatility = _coeff_of_variation(prices_30d)
            suspicious = _is_price_suspicious(current, mean)

            meta = name_to_meta.get(name, {})
            container_id: str = meta.get("id", name)  # fall back to name when no DB

            result.append(
                ItemDTO(
                    id=container_id,
                    name=name,
                    current_price=current,
                    volume_7d=int(item.get("quantity") or 0),
                    is_suspicious=suspicious,
                    volatility=volatility,
                    mean_price=mean,
                    lowest_price=(
                        float(item["lowest_price"]) if item.get("lowest_price") else None
                    ),
                )
            )

        return result

    def get_item_details(self, item_id: str):
        """
        Return a full ItemDTO for a single container identified by container_id.

        Returns None when the container is not found or has no price history.
        """
        from src.domain.dtos import ItemDTO

        if self._db is None:
            logger.warning("no_db_session", service="item_service", method="get_item_details")
            return None

        try:
            from src.domain.models import DimContainer, FactContainerPrice
            from sqlalchemy import func

            container = self._db.get(DimContainer, item_id)
            if container is None:
                logger.debug("item_not_found", service="item_service", item_id=item_id)
                return None

            cutoff_30d = datetime.now(UTC).replace(tzinfo=None) - timedelta(days=30)

            # Latest price
            latest = (
                self._db.query(FactContainerPrice)
                .filter(
                    FactContainerPrice.container_id == item_id,
                    FactContainerPrice.price.isnot(None),
                )
                .order_by(FactContainerPrice.timestamp.desc())
                .first()
            )
            if latest is None:
                return None

            # 30-day window for volatility + mean
            recent_prices = (
                self._db.query(FactContainerPrice.price)
                .filter(
                    FactContainerPrice.container_id == item_id,
                    FactContainerPrice.timestamp >= cutoff_30d,
                    FactContainerPrice.price.isnot(None),
                )
                .all()
            )
            prices_30d = [float(r.price) for r in recent_prices]
            mean_30d = (sum(prices_30d) / len(prices_30d)) if prices_30d else None
            volatility = _coeff_of_variation(prices_30d)

            current = float(latest.price)
            return ItemDTO(
                id=str(item_id),
                name=str(container.container_name),
                current_price=current,
                volume_7d=int(latest.volume_7d or 0),
                is_suspicious=_is_price_suspicious(current, mean_30d),
                volatility=volatility,
                mean_price=mean_30d,
                lowest_price=(
                    float(latest.lowest_price) if latest.lowest_price else None
                ),
            )

        except Exception as exc:
            logger.error("get_item_details_error", service="item_service", item_id=item_id, error=str(exc))
            return None

    def get_price_history(self, item_id: str, source: str | None = None) -> list:
        """
        Return ordered price history for a container as PriceHistoryDTO list.

        Parameters
        ----------
        item_id : container UUID
        source  : optional source filter (e.g. "steam_live"); None = all records

        Returns an empty list when no price rows exist.
        """
        from src.domain.dtos import PriceHistoryDTO

        if self._db is None:
            return []

        try:
            from src.domain.models import DimContainer, FactContainerPrice

            container = self._db.get(DimContainer, item_id)
            if container is None:
                return []

            q = (
                self._db.query(FactContainerPrice)
                .filter(
                    FactContainerPrice.container_id == item_id,
                    FactContainerPrice.price.isnot(None),
                )
                .order_by(FactContainerPrice.timestamp.asc())
            )
            if source:
                q = q.filter(FactContainerPrice.source == source)
            rows = q.all()
            return [
                PriceHistoryDTO(
                    timestamp=r.timestamp,
                    price=float(r.price),
                    mean_price=float(r.mean_price) if r.mean_price is not None else None,
                    volume_7d=int(r.volume_7d or 0),
                    source=str(r.source or "steam_market"),
                )
                for r in rows
            ]
        except Exception as exc:
            logger.error("get_price_history_error", service="item_service", item_id=item_id, error=str(exc))
            return []

    def get_bulk_price_histories(
        self, container_ids: list[str], source: str | None = None
    ) -> dict[str, list]:
        """
        Bulk-fetch price histories for multiple containers in one query (N+1 → 1).

        Returns {container_id: list[PriceHistoryDTO]} sorted by timestamp ascending.
        Missing container IDs map to an empty list.
        """
        from src.domain.dtos import PriceHistoryDTO

        if self._db is None or not container_ids:
            return {cid: [] for cid in container_ids}

        try:
            from src.domain.models import FactContainerPrice

            q = (
                self._db.query(FactContainerPrice)
                .filter(
                    FactContainerPrice.container_id.in_(container_ids),
                    FactContainerPrice.price.isnot(None),
                )
                .order_by(FactContainerPrice.timestamp.asc())
            )
            if source:
                q = q.filter(FactContainerPrice.source == source)
            rows = q.all()

            result: dict[str, list] = {cid: [] for cid in container_ids}
            for r in rows:
                cid = str(r.container_id)
                if cid in result:
                    result[cid].append(
                        PriceHistoryDTO(
                            timestamp=r.timestamp,
                            price=float(r.price),
                            mean_price=float(r.mean_price) if r.mean_price is not None else None,
                            volume_7d=int(r.volume_7d or 0),
                            source=str(r.source or "steam_market"),
                        )
                    )
            return result
        except Exception as exc:
            logger.error(
                "get_bulk_price_histories_error",
                service="item_service",
                count=len(container_ids),
                error=str(exc),
            )
            return {cid: [] for cid in container_ids}

    def get_signals(self) -> dict:
        """
        Return BUY/HOLD/SELL investment signals for all containers.

        Strategy
        --------
        1. Try CACHE-1 (FactInvestmentSignal) — fast path, no computation.
        2. Fall back to live computation via engine.investment.

        Returns
        -------
        dict[container_id_or_name → {verdict, score, ...}]
        """
        from src.domain.portfolio import get_cached_signals
        cached = get_cached_signals()
        if cached:
            return cached

        # Live fallback: build price_data dict and run signal engine
        items = self.get_market_overview()
        if not items:
            return {}

        price_data = {item.name: item.to_price_dict() for item in items}

        try:
            from src.domain.connection import SessionLocal
            from src.domain.models import DimContainer

            with SessionLocal() as db:
                containers = db.query(DimContainer).all()

            from src.domain.investment import compute_all_investment_signals
            return compute_all_investment_signals(containers, price_data)
        except Exception as exc:
            logger.error("get_signals_error", service="item_service", error=str(exc))
            return {}

    def process_new_price(self, item_id: str, raw_price: float) -> bool:
        """
        Validate and persist a new price observation — Celery task entry point.

        Validation
        ----------
        - raw_price must be a finite positive float within [_PRICE_MIN, _PRICE_MAX].
        - Container must exist in DimContainer.
        - Container must not be blacklisted.

        Returns True on successful write, False when validation fails or container
        is unknown/blacklisted. Raises on unexpected DB errors so Celery can retry.
        """
        _t0 = time.monotonic()

        # ── Validate raw_price ────────────────────────────────────────────────
        if raw_price is None or not math.isfinite(raw_price):
            logger.debug(
                "price_rejected_non_finite",
                service="item_service",
                item_id=item_id,
                raw_price=repr(raw_price),
            )
            return False

        if raw_price < _PRICE_MIN or raw_price > _PRICE_MAX:
            logger.debug(
                "price_rejected_out_of_range",
                service="item_service",
                item_id=item_id,
                raw_price=raw_price,
            )
            return False

        # ── Resolve container ─────────────────────────────────────────────────
        from src.domain.connection import SessionLocal
        from src.domain.models import DimContainer
        from src.domain.sql_repositories import SqlAlchemyPriceRepository

        with SessionLocal() as db:
            container = db.get(DimContainer, item_id)
            if container is None:
                logger.debug("price_rejected_unknown_item", service="item_service", item_id=item_id)
                return False

            if container.is_blacklisted:
                logger.debug(
                    "price_rejected_blacklisted",
                    service="item_service",
                    item_id=item_id,
                    name=str(container.container_name),
                )
                return False

            container_name: str = str(container.container_name)

            # ── PV-06 Sanity Check: anomaly detection ─────────────────────────
            from src.domain.models import FactContainerPrice

            cutoff_sanity = (
                datetime.now(UTC).replace(tzinfo=None) - timedelta(days=_SANITY_WINDOW_DAYS)
            )
            recent_rows = (
                db.query(FactContainerPrice.price)
                .filter(
                    FactContainerPrice.container_id == item_id,
                    FactContainerPrice.timestamp >= cutoff_sanity,
                    FactContainerPrice.price.isnot(None),
                )
                .all()
            )
            recent_prices = [float(r.price) for r in recent_rows]
            med = _median(recent_prices)

            if med is not None and len(recent_prices) >= _SANITY_MIN_SAMPLES:
                deviation = abs(raw_price - med) / med
                if deviation > _SANITY_THRESHOLD:
                    logger.warning(
                        "price_anomaly_rejected",
                        service="item_service",
                        item_id=item_id,
                        raw_price=raw_price,
                        median=round(med, 2),
                        deviation_pct=round(deviation * 100, 1),
                    )
                    return False  # do not persist anomalous price
            # ── End sanity check ──────────────────────────────────────────────

            price_repo = SqlAlchemyPriceRepository(db)
            saved = price_repo.save_jit_price(
                container_name=container_name,
                price=raw_price,
                source="celery_fetch",
            )
            db.commit()

        duration_ms = round((time.monotonic() - _t0) * 1000)
        if saved:
            logger.info(
                "price_saved",
                service="item_service",
                item_id=item_id,
                name=container_name,
                price=raw_price,
                duration_ms=duration_ms,
            )
        return saved

    # ── Convenience factory ───────────────────────────────────────────────────

    @classmethod
    def from_session(cls, db) -> "ItemService":
        """
        Construct ItemService from a raw SQLAlchemy Session.

        Usage (inside a ``with SessionLocal() as db:`` block):
            svc = ItemService.from_session(db)
        """
        from src.domain.sql_repositories import SqlAlchemyInventoryRepository
        return cls(SqlAlchemyInventoryRepository(db))

    @classmethod
    def open(cls) -> "ItemService":
        """
        Open a new DB session and return an ItemService bound to it.

        Caller is responsible for calling ``.close()`` when done, OR use
        ItemService.from_session() inside a managed context instead.
        """
        from src.domain.connection import SessionLocal
        from src.domain.sql_repositories import SqlAlchemyInventoryRepository

        db = SessionLocal()
        svc = cls(SqlAlchemyInventoryRepository(db))
        svc._owned_db = db  # store for close()
        return svc

    def close(self) -> None:
        """Close the session opened by ItemService.open()."""
        owned = getattr(self, "_owned_db", None)
        if owned is not None:
            owned.close()
            self._owned_db = None
