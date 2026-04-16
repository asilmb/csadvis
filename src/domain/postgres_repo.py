from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import List

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.domain.abstract_repo import AbstractRepository, ContainerDTO, PriceDTO


class PostgresRepository(AbstractRepository):
    """
    PostgreSQL/TimescaleDB-backed repository using SQLAlchemy 2.0 style.
    Session lifecycle is owned by the caller (Sessionmaker passed at construction).
    """

    def __init__(self, db: Session) -> None:
        self._db = db

    def get_all_containers(self) -> List[ContainerDTO]:
        from src.domain.models import DimContainer

        rows = self._db.execute(select(DimContainer)).scalars().all()
        return [
            ContainerDTO(
                item_id=str(r.container_id),
                name=str(r.container_name),
                container_type=str(r.container_type),
                base_cost=float(r.base_cost),
                is_blacklisted=bool(r.is_blacklisted),
                error_count=int(r.error_count or 0),
            )
            for r in rows
        ]

    def add_price_record(
        self,
        item_id: str,
        price: float,
        timestamp: datetime,
    ) -> None:
        from sqlalchemy.dialects.postgresql import insert
        from src.domain.models import FactContainerPrice

        stmt = (
            insert(FactContainerPrice)
            .values(
                id=str(uuid.uuid4()),
                container_id=item_id,
                timestamp=timestamp,
                price=price,
                source="steam_market",
            )
            .on_conflict_do_nothing(index_elements=["container_id", "timestamp"])
        )
        self._db.execute(stmt)

    def update_container_tier(self, item_id: str, tier: int) -> None:
        """
        Persist tier as a SystemSettings key-value entry: key='tier:{item_id}'.
        Tier values: 1 = Active, 3 = Cold (as defined by classify_tier).
        """
        from sqlalchemy.dialects.postgresql import insert

        from src.domain.models import SystemSettings

        stmt = insert(SystemSettings).values(
            key=f"tier:{item_id}",
            value=str(tier),
            updated_at=datetime.now(UTC).replace(tzinfo=None),
        ).on_conflict_do_update(
            index_elements=["key"],
            set_={"value": str(tier), "updated_at": datetime.now(UTC).replace(tzinfo=None)},
        )
        self._db.execute(stmt)

    def get_price_history(self, item_id: str, days: int) -> List[PriceDTO]:
        from src.domain.models import FactContainerPrice

        cutoff = datetime.now(UTC).replace(tzinfo=None) - timedelta(days=days)
        stmt = (
            select(FactContainerPrice)
            .where(
                FactContainerPrice.container_id == item_id,
                FactContainerPrice.price.isnot(None),
                FactContainerPrice.timestamp >= cutoff,
            )
            .order_by(FactContainerPrice.timestamp.asc())
        )
        rows = self._db.execute(stmt).scalars().all()
        return [
            PriceDTO(
                item_id=str(r.container_id),
                price=float(r.price),
                timestamp=r.timestamp,
                volume_7d=int(r.volume_7d or 0),
            )
            for r in rows
        ]

    def get_market_sync_list(self) -> List[str]:
        from src.domain.models import DimContainer

        stmt = select(DimContainer.container_name).where(DimContainer.is_blacklisted == 0)
        rows = self._db.execute(stmt).all()
        return [str(r.container_name) for r in rows]

    def increment_error_count(self, container_name: str) -> None:
        from src.domain.models import DimContainer

        stmt = select(DimContainer).where(DimContainer.container_name == container_name)
        container = self._db.execute(stmt).scalar_one_or_none()
        if container is None:
            return
        container.error_count = (container.error_count or 0) + 1
        if container.error_count >= 3:
            container.is_blacklisted = 1
        self._db.flush()

    def get_prices_since(self, cutoff: datetime) -> List[tuple]:
        from src.domain.models import FactContainerPrice

        stmt = (
            select(FactContainerPrice.container_id, FactContainerPrice.price)
            .where(
                FactContainerPrice.timestamp >= cutoff,
                FactContainerPrice.price.isnot(None),
            )
        )
        rows = self._db.execute(stmt).all()
        return [(str(r.container_id), float(r.price)) for r in rows]

    def get_container_id_by_name(self, container_name: str) -> "str | None":
        from src.domain.models import DimContainer

        stmt = select(DimContainer.container_id).where(
            DimContainer.container_name == container_name
        )
        result = self._db.execute(stmt).scalar_one_or_none()
        return str(result) if result is not None else None

    def get_max_timestamps_by_container(self) -> "dict[str, datetime]":
        from sqlalchemy import func

        from src.domain.models import FactContainerPrice

        stmt = select(
            FactContainerPrice.container_id,
            func.max(FactContainerPrice.timestamp),
        ).group_by(FactContainerPrice.container_id)
        rows = self._db.execute(stmt).all()
        return {str(cid): ts for cid, ts in rows if ts is not None}

    def bulk_add_prices(self, rows: "list[dict]") -> None:
        if not rows:
            return
        from sqlalchemy.dialects.postgresql import insert
        from src.domain.models import FactContainerPrice

        values = [
            {
                "id": str(uuid.uuid4()),
                "container_id": r["container_id"],
                "timestamp": r["timestamp"],
                "price": r["price"],
                "volume_7d": r.get("volume_7d", 0),
                "mean_price": r.get("mean_price"),
                "lowest_price": r.get("lowest_price"),
                "source": r.get("source", "steam_market"),
            }
            for r in rows
        ]
        stmt = (
            insert(FactContainerPrice)
            .values(values)
            .on_conflict_do_nothing(index_elements=["container_id", "timestamp"])
        )
        self._db.execute(stmt)

    def downsample_old_prices(self, days_threshold: int = 90) -> tuple[int, int]:
        """
        Aggregate detail price rows older than `days_threshold` days into one
        row per (container_id, day).  Runs inside the caller's transaction.

        Strategy:
          1. SELECT rows older than cutoff, grouped by container_id + date.
          2. INSERT one aggregated row per group (AVG price, SUM volume, source='daily_aggregate').
          3. DELETE all original detail rows in those groups.

        Returns:
            (rows_deleted, summaries_inserted) — used for the log message.
        """
        from sqlalchemy import func, text
        from sqlalchemy.dialects.postgresql import insert

        from src.domain.models import FactContainerPrice

        cutoff = datetime.now(UTC).replace(tzinfo=None) - timedelta(days=days_threshold)

        # ── Step 1: aggregate ─────────────────────────────────────────────────
        date_trunc = func.date_trunc("day", FactContainerPrice.timestamp)

        agg_stmt = (
            select(
                FactContainerPrice.container_id,
                date_trunc.label("day"),
                func.avg(FactContainerPrice.price).label("avg_price"),
                func.avg(FactContainerPrice.mean_price).label("avg_mean"),
                func.avg(FactContainerPrice.lowest_price).label("avg_lowest"),
                func.sum(FactContainerPrice.volume_7d).label("total_volume"),
                func.count().label("cnt"),
            )
            .where(
                FactContainerPrice.timestamp < cutoff,
                FactContainerPrice.source != "daily_aggregate",
            )
            .group_by(FactContainerPrice.container_id, date_trunc)
        )
        groups = self._db.execute(agg_stmt).all()

        if not groups:
            return 0, 0

        # ── Step 2: upsert aggregated rows ────────────────────────────────────
        new_rows = [
            {
                "id": str(uuid.uuid4()),
                "container_id": g.container_id,
                "timestamp": g.day,          # midnight of that day
                "price": round(g.avg_price, 2) if g.avg_price is not None else None,
                "mean_price": round(g.avg_mean, 2) if g.avg_mean is not None else None,
                "lowest_price": round(g.avg_lowest, 2) if g.avg_lowest is not None else None,
                "volume_7d": int(g.total_volume or 0),
                "source": "daily_aggregate",
            }
            for g in groups
        ]

        ins_stmt = insert(FactContainerPrice).values(new_rows).on_conflict_do_nothing()
        self._db.execute(ins_stmt)

        # ── Step 3: delete original detail rows ───────────────────────────────
        del_stmt = text(
            """
            DELETE FROM fact_container_prices
            WHERE  timestamp < :cutoff
              AND  source != 'daily_aggregate'
            """
        )
        deleted_result = self._db.execute(del_stmt, {"cutoff": cutoff})
        rows_deleted = deleted_result.rowcount

        self._db.flush()
        return rows_deleted, len(new_rows)
