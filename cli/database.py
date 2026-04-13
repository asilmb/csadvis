"""
Database commands: cmd_backfill, cmd_scrape, cmd_events_refresh,
cmd_db_seed, cmd_db_prune, cmd_db_cleanup, cmd_db_reset.
"""
from __future__ import annotations

import logging
import sys

logger = logging.getLogger(__name__)


def cmd_backfill(args) -> None:
    """Load Steam Market price history for all tracked containers into the DB."""
    import asyncio
    import uuid
    from datetime import date

    from sqlalchemy import func

    from database.connection import SessionLocal, init_db
    from database.models import DimContainer, FactContainerPrice
    from ingestion.steam.client import SteamMarketClient
    from ingestion.steam.logic import fetch_all as steam_fetch_all

    force: bool = getattr(args, "force", False)
    missing_only: bool = getattr(args, "missing", False)

    init_db()
    db = SessionLocal()
    try:
        containers = db.query(DimContainer).all()

        if missing_only:
            # Find container IDs that already have at least one price record
            from sqlalchemy import distinct

            seeded_ids: set[str] = {
                str(r[0]) for r in db.query(distinct(FactContainerPrice.container_id)).all()
            }
            containers = [c for c in containers if str(c.container_id) not in seeded_ids]
    finally:
        db.close()

    if not containers:
        if missing_only:
            print("\n  All containers already have price data. Nothing to backfill.\n")
        else:
            print("\n  No containers found in database. Run: cs2 db seed\n")
        return

    names = [str(c.container_name) for c in containers]
    id_map = {str(c.container_name): str(c.container_id) for c in containers}

    if missing_only:
        print(f"\n  [--missing] Backfilling {len(names)} containers with no price data ...")
    else:
        print(f"\n  Loading Steam Market history for {len(names)} containers ...")

    delay_secs = 0.0 if force else 4.0
    if force:
        print("  No delay between requests (--force mode)\n")
    else:
        print("  4s delay between requests (safe: 15 req/min, limit: 20 req/min)\n")

    client = SteamMarketClient()

    def on_progress(name: str, idx: int, total: int) -> None:
        print(f"  [{idx:>2}/{total}] {name}")

    import httpx

    loop = asyncio.new_event_loop()
    try:
        all_history = loop.run_until_complete(
            steam_fetch_all(client, names, on_progress=on_progress, delay=delay_secs)
        )
    except (httpx.TimeoutException, httpx.ConnectError) as exc:
        loop.close()
        logger.error(
            "Backfill failed — Steam Market недоступен (%s). Попробуйте позже.",
            exc.__class__.__name__,
        )
        sys.exit(1)
    finally:
        loop.close()

    db = SessionLocal()
    try:
        # Incremental insert: query only the max timestamp per container (O(1) RAM).
        # We no longer delete-before-reinsert — Steam history already received is kept.
        # Only rows with date > max_date_in_db for that container are inserted.
        _max_ts_rows = (
            db.query(FactContainerPrice.container_id, func.max(FactContainerPrice.timestamp))
            .group_by(FactContainerPrice.container_id)
            .all()
        )
        max_dates: dict[str, date] = {str(cid): ts.date() for cid, ts in _max_ts_rows}

        total_inserted = 0
        for name, rows in all_history.items():
            if name not in id_map:
                logger.warning("Backfill: container %r not in DB ->skipping", name)
                continue
            cid = id_map[name]
            new_rows = []
            for row in rows:
                if row["date"].date() <= max_dates.get(cid, date.min):
                    continue
                new_rows.append(
                    FactContainerPrice(
                        id=str(uuid.uuid4()),
                        container_id=cid,
                        timestamp=row["date"],
                        price=row["price"],
                        mean_price=None,
                        volume_7d=row["volume"],
                        source="steam_market",
                    )
                )
            if new_rows:
                db.bulk_save_objects(new_rows)
                db.commit()
                total_inserted += len(new_rows)
                logger.info("Backfill: %s -> %d days saved", name, len(new_rows))
    finally:
        db.close()

    print(f"\n  Done. {total_inserted} daily price records saved.\n")


def cmd_scrape(args) -> None:
    """Run the Steam Market container scraper right now (ignores the date check)."""
    import asyncio

    from database.connection import SessionLocal, init_db
    from scraper.db_writer import write_new_containers
    from scraper.state import mark_done
    from scraper.steam_market_scraper import scrape_all_containers

    init_db()
    logger.info("Running scraper ...")

    loop = asyncio.new_event_loop()
    try:
        containers = loop.run_until_complete(scrape_all_containers())
    finally:
        loop.close()

    logger.info("Scraped %d containers from Steam Market API", len(containers))

    with SessionLocal() as db:
        inserted = write_new_containers(db, containers)

    if inserted:
        print(f"\n  Added {inserted} new container(s) to the database.\n")
    else:
        print("\n  No new containers - database is already up to date.\n")
    mark_done()


def cmd_events_refresh(args) -> None:
    """Fetch an updated event calendar YAML from the remote URL and save it locally."""
    from pathlib import Path

    from config import settings

    url = settings.events_remote_url.strip()
    if not url:
        print("\n  No remote URL configured.")
        print("  Set EVENTS_REMOTE_URL= in your .env file, then re-run: cs2 events refresh\n")
        return

    import httpx

    yaml_path = settings.events_yaml_path
    print(f"\n  Fetching events from: {url}")
    try:
        resp = httpx.get(url, timeout=30, follow_redirects=True)
        resp.raise_for_status()
    except httpx.HTTPStatusError as exc:
        print(f"  HTTP error {exc.response.status_code}: {exc}\n")
        sys.exit(1)
    except (httpx.TimeoutException, httpx.ConnectError) as exc:
        print(f"  Network error: {exc}\n")
        sys.exit(1)

    # Validate the downloaded YAML before writing
    import tempfile

    from engine.event_loader import load_events

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=False, encoding="utf-8"
    ) as tmp:
        tmp.write(resp.text)
        tmp_path = tmp.name

    try:
        events = load_events(tmp_path)
    except (FileNotFoundError, ValueError) as exc:
        print(f"  Validation failed: {exc}")
        print("  File NOT saved. Fix the remote source and retry.\n")
        Path(tmp_path).unlink(missing_ok=True)
        sys.exit(1)

    dest = Path(yaml_path)
    dest.parent.mkdir(parents=True, exist_ok=True)
    Path(tmp_path).replace(dest)
    print(f"  Saved {len(events)} event(s) to {dest}")
    print("  Restart the dashboard to apply changes.\n")


def cmd_db_seed(args) -> None:
    """Re-run the static data seeder (safe to run multiple times)."""
    from database.connection import SessionLocal, init_db
    from seed.data import seed_database

    init_db()
    with SessionLocal() as db:
        seed_database(db)
    print("\n  Seed complete.\n")


def cmd_db_prune(args) -> None:
    """Delete price snapshots older than 2 years to keep DB size manageable."""
    from datetime import UTC, datetime, timedelta

    from database.connection import SessionLocal, init_db
    from database.models import FactContainerPrice

    cutoff = datetime.now(UTC).replace(tzinfo=None) - timedelta(days=730)
    print(f"\n  Pruning price records older than {cutoff.strftime('%Y-%m-%d')} ...")

    init_db()
    db = SessionLocal()
    try:
        deleted = (
            db.query(FactContainerPrice)
            .filter(FactContainerPrice.timestamp < cutoff)
            .delete(synchronize_session=False)
        )
        db.commit()
    finally:
        db.close()

    print(f"  Deleted {deleted:,} price record(s).\n")


def cmd_db_cleanup(args) -> None:
    """
    Run the database garbage collector manually.

    Deletes COMPLETED/FAILED tasks older than 24 h, EventLog rows older than
    7 days (protecting ERROR/CRITICAL within 48 h), then runs VACUUM.
    """
    from database.connection import init_db
    from services.maintenance import MaintenanceService

    init_db()
    svc = MaintenanceService()

    print("\n  DB Maintenance — running cleanup ...")
    result = svc.run_all()
    print(f"  Tasks deleted:      {result.tasks_deleted:,}")
    print(f"  Event log deleted:  {result.events_deleted:,}")
    print("  VACUUM complete.\n")


def cmd_db_reset(args) -> None:
    """Drop and recreate the entire database (DESTRUCTIVE)."""
    answer = (
        input(
            "\n  ВНИМАНИЕ: Это действие удалит всю историю цен и инвентарь. Продолжить? (y/n): "
        )
        .strip()
        .lower()
    )
    if answer != "y":
        print("  Отменено.\n")
        return

    from sqlalchemy import text

    from database.connection import SessionLocal, engine, init_db
    from database.models import Base
    from seed.data import seed_database

    Base.metadata.drop_all(bind=engine)
    logger.info("All tables dropped.")

    init_db()
    logger.info("New schema initialized.")

    with SessionLocal() as db:
        seed_database(db)

    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text("VACUUM ANALYZE"))
    logger.info("VACUUM complete.")

    print("\n  Database reset complete. New schema initialized.\n")
