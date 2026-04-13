# STATUS.md — Project State

---
## Last Updated: 2026-04-09
**Current State:** All 6 dashboard tabs and all API endpoints verified healthy. DB schema column rename migration applied (16 `_kzt` → currency-agnostic names); `entrypoint.sh` POSIX fix applied; app image rebuilt; worker/beat containers stable.
**Next Goal:** Seed real price data via `cs2 backfill` to replace empty-state dashboard with live content.

---

## Infrastructure State (Docker)
- **cs2-app-1**: Up. FastAPI (port 8000) + Dash (port 8050).
- **cs2-worker-1**: Up. Celery worker — BUT price tasks fail (see A-01).
- **cs2-beat-1**: Up. Celery Beat scheduler.
- **cs2-db-1**: Healthy. TimescaleDB 16.
- **cs2-redis-1**: Healthy. Redis 7 (appendonly, named volume).

---

## State Persistence (as of 2026-04-08)
All file-based state migrated to Redis today:

| Key | Content |
|-----|---------|
| `cs2:backfill:last_run` | ISO datetime of last backfill |
| `cs2:scraper:last_parsed` | Date of last scraper run |
| `cs2:wallet:balance` | Cached wallet balance |
| `cs2:nameid:cache` | Redis Hash {name: item_nameid} |
| `STEALTH_BLOCK_EXPIRES` | 429 emergency block (TTL 6h) |

`storage/` directory deleted. `./storage:/app/storage` volume mounts removed from docker-compose.

---

## Known Broken (Post-Migration)
- **A-01 BLOCKER:** `fetch_steam_price` Celery task crashes — `SteamMarketClient` is not async context manager
- **A-02 HIGH:** `cli/main.py` runs `init_db()` at module import (dangerous anti-pattern)
- **A-03 HIGH:** `backup_db.sh` writes to `/app/storage/backups` which no longer exists
- **A-04 HIGH:** 7 test files import deleted `services/worker_engine` — test suite broken
- **B-01–B-07:** Scraping, inventory, backfill, dashboard tabs — unverified post-migration
- **D-01:** CLI missing commands: `backfill`, `scrape`, `seed`, `reset-db`, `cookie`, `db prune`

---

## Completed This Session (2026-04-08)
- Fixed `cli/__init__.py` wrong import path
- Fixed `cli/service.py` + `cli/main.py` `"main:app"` → `"cli.main:app"`
- Deleted root `main.py` (obsolete)
- Migrated all 6 state files from `storage/states/` to Redis
- Removed `storage/` directory and docker-compose volume mounts
- Cleaned `config.py` (removed `STORAGE_ROOT`, `_ensure_storage_dirs`, `_DEFAULT_DB_PATH`)
- Added `infra/redis_client.py` shared factory
- Wired `cli/diagnostics.py` commands into `cli/__main__.py`

---

## CI & Engine State
- **Infra:** Docker Compose stack (db, redis, app, worker, beat). PostgreSQL+TimescaleDB active.
- **Core:** Celery + Redis broker. `worker_engine.py` deleted (replaced by Celery tasks).
- **Data Flow:** `float` prices in DB. Financial math via `ItemService`. Pydantic DTOs.
- **State:** All runtime state in Redis (no filesystem persistence).
- **Logging:** structlog JSON in app/worker containers. `logs/` mounted per-service.
