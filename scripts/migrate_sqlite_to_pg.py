"""
PV-03: SQLite → PostgreSQL/TimescaleDB migration script.

Usage:
    python scripts/migrate_sqlite_to_pg.py [--dry-run] [--batch-size 500]

Env vars (override via .env or shell):
    DATABASE_PATH    SQLite source path (default: storage/db/cs2_analytics.db)
    DATABASE_URL     PostgreSQL target  (e.g. postgresql+psycopg2://user:pw@host/db)
    POSTGRES_HOST    Used to build DATABASE_URL if DATABASE_URL is absent
    POSTGRES_PORT
    POSTGRES_DB
    POSTGRES_USER
    POSTGRES_PASSWORD
"""

from __future__ import annotations

import argparse
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

# ── Bootstrap project root on sys.path ─────────────────────────────────────────
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

import os

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from config import settings
from src.domain.models import Base, DimContainer
from src.domain.postgres_repo import PostgresRepository

# ─── Helpers ───────────────────────────────────────────────────────────────────

_CURRENCY_RE = re.compile(r"[^\d.\-]")


def _clean_numeric(value: object) -> float | None:
    """Strip currency symbols / whitespace; return float or None."""
    if value is None:
        return None
    cleaned = _CURRENCY_RE.sub("", str(value)).strip()
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_ts(value: object) -> datetime | None:
    """Parse ISO-format timestamp string into naive datetime."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    s = str(value).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def _build_pg_url() -> str:
    url = os.environ.get("DATABASE_URL", "")
    if url:
        return url
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db   = os.environ.get("POSTGRES_DB", "cs2")
    user = os.environ.get("POSTGRES_USER", "cs2user")
    pw   = os.environ.get("POSTGRES_PASSWORD", "cs2pass")
    return f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}"


# ─── Migration logic ───────────────────────────────────────────────────────────

def migrate(dry_run: bool = False, batch_size: int = 500) -> None:
    sqlite_path = settings.database_path
    pg_url = _build_pg_url()

    print(f"[Migrate] Source SQLite : {sqlite_path}")
    print(f"[Migrate] Target PG     : {pg_url.split('@')[-1]}")  # hide credentials
    if dry_run:
        print("[Migrate] DRY RUN — no data will be written")

    # ── SQLite connection ───────────────────────────────────────────────────────
    sqlite_conn = sqlite3.connect(sqlite_path)
    sqlite_conn.row_factory = sqlite3.Row
    cur = sqlite_conn.cursor()

    # ── PostgreSQL engine + schema setup ───────────────────────────────────────
    pg_engine = create_engine(pg_url, future=True)
    PgSession = sessionmaker(bind=pg_engine)

    if not dry_run:
        # Create tables that don't exist yet (idempotent)
        Base.metadata.create_all(pg_engine)
        # Promote fact_container_prices to hypertable if not already
        with pg_engine.connect() as conn:
            try:
                conn.execute(text(
                    "SELECT create_hypertable('fact_container_prices', 'timestamp', "
                    "if_not_exists => TRUE, migrate_data => TRUE)"
                ))
                conn.commit()
            except Exception as exc:  # noqa: BLE001
                print(f"[Migrate] Hypertable note: {exc}")

    # ── Phase 1: containers ────────────────────────────────────────────────────
    cur.execute("SELECT container_id, container_name, container_type, base_cost_kzt, "
                "error_count, is_blacklisted FROM dim_containers")
    sqlite_containers = cur.fetchall()
    print(f"[Migrate] Containers in SQLite: {len(sqlite_containers)}")

    migrated_containers = 0
    if not dry_run:
        with PgSession() as pg_session:
            for row in sqlite_containers:
                existing = pg_session.get(DimContainer, row["container_id"])
                if existing is not None:
                    continue  # skip already-migrated
                pg_session.add(DimContainer(
                    container_id=row["container_id"],
                    container_name=row["container_name"],
                    container_type=row["container_type"],
                    base_cost_kzt=_clean_numeric(row["base_cost_kzt"]) or 0.0,
                    error_count=int(row["error_count"] or 0),
                    is_blacklisted=int(row["is_blacklisted"] or 0),
                ))
                migrated_containers += 1
            pg_session.commit()
        print(f"[Migrate] Containers inserted: {migrated_containers}")
    else:
        print(f"[Migrate] (dry) Would insert up to {len(sqlite_containers)} containers")

    # ── Phase 2: price history ─────────────────────────────────────────────────
    cur.execute("SELECT COUNT(*) FROM fact_container_prices")
    sqlite_price_count = cur.fetchone()[0]
    print(f"[Migrate] Price rows in SQLite: {sqlite_price_count}")

    cur.execute(
        "SELECT container_id, timestamp, price_kzt, mean_kzt, lowest_price_kzt, "
        "volume_7d, source FROM fact_container_prices ORDER BY timestamp"
    )

    migrated_prices = 0
    skipped_prices  = 0
    batch: list[dict] = []

    def _flush(pg_session_factory: "sessionmaker") -> int:
        if not batch:
            return 0
        with pg_session_factory() as sess:
            repo = PostgresRepository(sess)
            repo.bulk_add_prices(batch)
            sess.commit()
        n = len(batch)
        batch.clear()
        return n

    if not dry_run:
        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break
            for row in rows:
                price = _clean_numeric(row["price_kzt"])
                if price is None:
                    skipped_prices += 1
                    continue
                ts = _parse_ts(row["timestamp"])
                if ts is None:
                    skipped_prices += 1
                    continue
                batch.append({
                    "container_id":      row["container_id"],
                    "timestamp":         ts,
                    "price_kzt":         price,
                    "mean_kzt":          _clean_numeric(row["mean_kzt"]),
                    "lowest_price_kzt":  _clean_numeric(row["lowest_price_kzt"]),
                    "volume_7d":         int(row["volume_7d"] or 0),
                    "source":            row["source"] or "steam_market",
                })
                if len(batch) >= batch_size:
                    migrated_prices += _flush(PgSession)
                    print(f"[Migrate] Перенесено {migrated_prices} записей истории цен.")

        migrated_prices += _flush(PgSession)
        print(f"[Migrate] Перенесено {migrated_prices} записей истории цен.")
        if skipped_prices:
            print(f"[Migrate] Пропущено (нечисловые/пустые): {skipped_prices}")
    else:
        print(f"[Migrate] (dry) Would migrate up to {sqlite_price_count} price rows")

    # ── Validation: COUNT(*) comparison ───────────────────────────────────────
    print("\n[Migrate] === Validation ===")

    cur.execute("SELECT COUNT(*) FROM dim_containers")
    sq_containers = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM fact_container_prices")
    sq_prices = cur.fetchone()[0]

    if not dry_run:
        with pg_engine.connect() as conn:
            pg_containers = conn.execute(text("SELECT COUNT(*) FROM dim_containers")).scalar()
            pg_prices     = conn.execute(text("SELECT COUNT(*) FROM fact_container_prices")).scalar()

        c_match = "OK" if pg_containers == sq_containers else "MISMATCH"
        p_match = "OK" if pg_prices     == sq_prices     else "MISMATCH"

        print(f"[Validate] dim_containers     SQLite={sq_containers}  PG={pg_containers}  [{c_match}]")
        print(f"[Validate] fact_container_prices  SQLite={sq_prices}  PG={pg_prices}  [{p_match}]")

        if c_match != "OK" or p_match != "OK":
            print("[Validate] WARNING: count mismatch — check skipped rows above")
            sys.exit(1)
        else:
            print("[Validate] Migration complete — counts match.")
    else:
        print(f"[Validate] (dry) SQLite: {sq_containers} containers, {sq_prices} prices")

    sqlite_conn.close()


# ─── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate cs2_analytics.db → TimescaleDB")
    parser.add_argument("--dry-run", action="store_true", help="Count rows without writing")
    parser.add_argument("--batch-size", type=int, default=500, metavar="N",
                        help="Rows per PostgreSQL flush (default: 500)")
    args = parser.parse_args()
    migrate(dry_run=args.dry_run, batch_size=args.batch_size)
