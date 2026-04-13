# CLAUDE_CONTEXT.md — Architecture & Tech Stack (2026-04-08)

## Tech Stack
- **Backend:** Python 3.11+, FastAPI + Uvicorn, Celery 5
- **Frontend:** Plotly Dash + Dash Bootstrap Components
- **Database:** PostgreSQL 16 (TimescaleDB), SQLAlchemy 2.0
- **Broker/State:** Redis 7 (Celery broker, token bucket, runtime state, nameid cache)
- **Ingestion:** HTTPX async + SteamRequestCoordinator (Token Bucket + Circuit Breaker)
- **Containers:** Docker Compose (db, redis, app, worker, beat)
- **Logging:** structlog (JSON format in containers)

## Entry Points
- `docker compose up` — full stack
- `python -m cli start` — app service (FastAPI + Dash)
- `python -m cli worker [--beat]` — Celery worker
- `entrypoint.sh` — Docker container entry (init DB → dispatch)

## Core Modules
- `services/item_service.py` — Business logic, sanity check, price persistence
- `services/cache_writer.py` — Computes + persists portfolio advice and signals
- `scheduler/tasks.py` — Celery task definitions (price fetch, inventory, backup)
- `infra/coordinator.py` — SteamRequestCoordinator: rate limiting, circuit breaker
- `infra/redis_client.py` — Shared Redis connection factory
- `database/postgres_repo.py` — PostgreSQL repository (production)
- `database/repositories.py` — SqlAlchemyInventoryRepository, SqlAlchemyPositionRepository

## Runtime State (all in Redis)
| Key | Type | Content |
|-----|------|---------|
| `cs2:backfill:last_run` | String | ISO datetime |
| `cs2:scraper:last_parsed` | String | Date string |
| `cs2:wallet:balance` | String | Float as string |
| `cs2:nameid:cache` | Hash | {name: item_nameid} |
| `STEALTH_BLOCK_EXPIRES` | String (TTL 6h) | Triggered on HTTP 429 |

## Celery Tasks (scheduler/tasks.py)
- `fetch_steam_price(item_id)` — fetch price for one container, persist via ItemService
- `poll_container_prices_task()` — Beat: enqueue fetch for all containers (hourly)
- `sync_inventory_task()` — Beat: fetch Steam inventory + reconcile (hourly)
- `cleanup_old_history_task()` — Beat: downsample >90d prices (Sundays 02:00 UTC)
- `daily_backup_task()` — Beat: run backup_db.sh (03:00 UTC daily)

## Known Critical Issues
- **A-01 BLOCKER:** `SteamMarketClient` is NOT async context manager — tasks.py uses
  `async with SteamMarketClient()` which crashes. Price fetching is broken.
- **A-02:** `cli/main.py` runs `init_db()` at module import time (dangerous).
- **A-03:** `backup_db.sh` writes to `/app/storage/backups` which no longer exists.
- **A-04:** 7 test files import deleted `services/worker_engine` — suite broken.
- See `CLAUDE_BACKLOG.md` for full list.

## Operational Flows
- **Price fetch:** Beat → `poll_container_prices_task` → `fetch_steam_price.delay(id)`
  per container → coordinator token → `SteamMarketClient.fetch_price_overview()` →
  `ItemService.process_new_price()` → `fact_container_prices`
- **Inventory sync:** Beat → `sync_inventory_task` → coordinator token →
  `SteamInventoryClient.fetch()` → `PositionReconciler.sync()`
- **Dashboard render:** Dash callback → `services/portfolio.py` → `fact_portfolio_advice`
  (pre-computed) → render
- **Auth update:** Tampermonkey → `POST /auth/update_session` → update runtime +
  `.env` + clear Redis `STEALTH_BLOCK_EXPIRES` + reset coordinator
