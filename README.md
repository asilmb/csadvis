# CS2 Market Analytics Platform

Real-time EV calculator, risk engine, inventory advisor, and trade tracking system for CS2 containers and skins. Data source: Steam Market only.

---

## Quick Start (Docker — recommended)

```bash
cp .env.example .env
# Fill in STEAM_LOGIN_SECURE and STEAM_ID in .env

docker compose up -d
```

- Dashboard: http://localhost:8050
- REST API:  http://localhost:8000
- Healthcheck: http://localhost:8000/health

The `app` service runs the dashboard + API. The `worker` service processes background tasks (price backfill, inventory sync, valuation). Both share a named Docker volume `cs2_data` for the SQLite database.

---

## Quick Start (local dev)

```bash
pip install -r requirements.txt
cp .env.example .env
# Fill in STEAM_LOGIN_SECURE and STEAM_ID

python -m cli start
```

---

## CLI Reference

All commands run via `python -m cli <command>` (or `cs2 <command>` after `pip install -e .`).

### Core

| Command | Description |
|---|---|
| `cs2 start` | Dashboard + REST API + APScheduler (all-in-one) |
| `cs2 dashboard` | Dash dashboard only (no scheduler) |
| `cs2 worker [--workers N]` | Background task workers + watchdog loop |

### Prices & Data

| Command | Description |
|---|---|
| `cs2 backfill [--days N] [--delay S]` | Load Steam Market price history into DB |
| `cs2 scrape` | Run the container scraper now |
| `cs2 validate-prices [--top N]` | Compare DB prices vs live Steam Market |
| `cs2 cookie` | Save Steam login cookie to `.env` interactively |

### Database

| Command | Description |
|---|---|
| `cs2 db seed` | Re-seed static container/item metadata (idempotent) |
| `cs2 db reset` | Drop + VACUUM + recreate schema (destructive, confirms in Russian) |
| `cs2 db prune` | Delete price snapshots older than 2 years |
| `cs2 db cleanup` | Run MaintenanceService (compact EventLog, purge old tasks) |
| `cs2 status` | Database statistics (row counts, worker state) |

### Operations

| Command | Description |
|---|---|
| `cs2 monitor` | Worker Registry + Task Queue live table |
| `cs2 watchdog` | One-shot reclaim of stuck tasks (heartbeat stale / TTL expired) |
| `cs2 events refresh` | Reload event calendar from YAML / remote URL |
| `cs2 validate-top [--top N] [--delay S]` | Validate top-N DB prices against Steam API |

---

## Architecture

### Phase Map

| Phase | Modules | Description |
|---|---|---|
| 1 | `engine/`, `seed/` | Probability engine, EV/ROI calculator, static container metadata |
| 2 | `ingestion/steam_market.py`, `ingestion/steam_inventory.py` | Steam Market price history + inventory sync |
| 3 | `frontend/`, `engine/portfolio_advisor.py` | Dash dashboard, portfolio advisor, action links |
| 4 | `database/`, `services/worker_engine.py`, `services/task_manager.py` | Persistent task queue, state-machine workers, watchdog |
| 5 | `services/trade_ledger.py`, `services/reconciler.py`, `ingestion/steam_transactions.py` | Trade ledger, position manager, asset-ID reconciliation |
| 6 | `infra/coordinator.py`, `Dockerfile`, `docker-compose.yml` | Rate-limit coordinator, containerization |

### Module Map

```
cs2/
├── cli.py                        # All CLI commands
├── config.py                     # Pydantic settings (reads .env)
├── requirements.txt
│
├── database/
│   ├── connection.py             # SQLite WAL engine + init_db()
│   ├── models.py                 # SQLAlchemy ORM models
│   └── repositories.py          # Repository pattern (DTOs, no ORM leakage)
│
├── domain/
│   ├── bus.py                    # Synchronous EventBus (thread-safe)
│   ├── events.py                 # Domain events (PriceUpdated, SignalDetected…)
│   ├── repositories.py           # Abstract repository interfaces
│   └── value_objects.py          # Amount, ROI, Percentage value objects
│
├── engine/
│   ├── portfolio_advisor.py      # SELL / FLIP / INVEST recommendations
│   ├── trade_advisor.py          # Trade EV / net P&L calculation
│   ├── investment.py             # Investment signal engine
│   ├── wall_filter.py            # Order-book wall detection
│   ├── event_calendar.py         # CS2 event schedule model
│   └── event_loader.py           # YAML / remote event calendar loader
│
├── infra/
│   └── coordinator.py            # SteamRequestCoordinator (token bucket + circuit breaker)
│
├── ingestion/
│   ├── steam_market.py           # Steam Market price history fetcher
│   ├── steam_inventory.py        # Steam inventory sync + trade-ban tracker
│   ├── steam_transactions.py     # Transaction history parser
│   ├── steam_wallet.py           # Wallet balance reader
│   └── nameid_cache.py           # Market nameid cache (for listing URLs)
│
├── services/
│   ├── worker_engine.py          # TaskWorker (state-machine) + WorkerManager
│   ├── task_manager.py           # TaskQueueService (enqueue, reclaim, watchdog)
│   ├── event_logger.py           # EventLog writer
│   ├── signal_handler.py         # SignalHandler → EventLog + webhook
│   ├── webhook_dispatcher.py     # Webhook POST with retry
│   ├── maintenance.py            # MaintenanceService (GC old events/tasks)
│   ├── trade_ledger.py           # TradeService + PositionRepository
│   ├── reconciler.py             # PositionReconciler (3-step asset-ID matching)
│   ├── steam_sync.py             # Inventory sync orchestrator
│   └── analytics/
│       └── armory_advisor.py     # Dynamic Armory Pass ROI advisor
│
├── frontend/
│   ├── app.py                    # Dash layout + callbacks
│   ├── renderers/portfolio.py    # SELL / FLIP / INVEST table renderers
│   ├── url_generator.py          # Steam Market / Inventory / Inspect URL builder
│   ├── charts.py                 # Plotly chart builders
│   └── helpers.py                # Shared UI helpers
│
└── tests/
    ├── stress_test.py            # Concurrency + performance tests (pytest -m slow)
    └── test_*.py                 # Unit tests (988 passing)
```

---

## Environment Variables

See `.env.example` for the full annotated list. Required variables:

| Variable | Description |
|---|---|
| `STEAM_LOGIN_SECURE` | `steamLoginSecure` browser cookie — required for price history + inventory |
| `STEAM_ID` | Steam 64-bit account ID — required for inventory sync |
| `DATABASE_PATH` | SQLite file path. Docker overrides to `/data/cs2_analytics.db` |

All other variables have safe defaults. See `.env.example` for tunable thresholds.

---

## Running Tests

```bash
# Default suite (988 unit tests, excludes slow/e2e)
pytest

# Stress / concurrency tests (opt-in)
pytest tests/stress_test.py -v -m slow

# Single module
pytest tests/test_task_manager.py -v
```

---

## Adding New Containers

New cases and capsules require manual metadata entry in `seed/data.py` — they are not scraped automatically. `seed_database()` is idempotent (safe to re-run after adding entries).

```python
# seed/data.py — _CONTAINERS list
{
    "name": "Kilowatt Case",
    "type": ContainerType.Weapon_Case,
    "cost": 2.49,
    "items": [
        {"base": "AK-47 | Inheritance", "rarity": RarityTier.Covert, "min": 0.00, "max": 1.00},
        # ... full item list from csgostash.com
    ],
}
```

After editing: `cs2 db seed`

---

## Technical Notes

### Key Architecture Decisions

| ID | Decision | Reason |
|----|----------|--------|
| AD-13 | Explicit Case Filtering | Only target objects (cases) shown in analytics SQL. Other items ignored. |
| AD-14 | JSON parser over HTML for transactions | Steam `/market/myhistory` stopped returning `results_html`. Parser uses `events`/`listings`/`purchases`/`assets` JSON. HTML parser kept as fallback. |
| AD-15 | `PAUSED_AUTH` task status | Separate status (not FAILED) prevents retry loop on expired cookie. Clears only via UI after cookie update. |
| AD-16 | Pre-loop worker registration | `_heartbeat(IDLE)` called before `while` loop — worker visible in DB within 1s of thread start. |

### Worker & Task Queue

- Workers only claim `PENDING`/`RETRY` rows. `PROCESSING` rows inserted directly (e.g. by scraper) are UI-only markers — never re-claimed.
- Zombie flush: on API startup, PROCESSING tasks with 0 live workers reset to PENDING before `enqueue_initial_sync()`.
- Exponential backoff on `httpx.NetworkError`: 30 → 60 → 120 → 600s cap. Task re-queued to PENDING (no retry counter increment).

### Steam API

- Transaction parser: `event_type 3` = SELL, `event_type 4` = BUY. Prices in KZT cents (`/ 100`). `currencyid 2037 = KZT`.
- SELL price: `listing["original_price"]` (not `price` — Steam zeros it on completion).
- Asset lookup always via listing asset (`contextid=2`), not purchase asset (`contextid=16`).
- `sessionid` cookie required alongside `steamLoginSecure` for `/myhistory` endpoint.

### Rate Limiting

- `SteamRequestCoordinator` singleton: Token Bucket (15 capacity, 0.25/s refill) + Circuit Breaker (180s freeze on 429/5xx).
- All HTTP calls — workers AND scraper — go through the same singleton. Token count in System tab reflects combined load.
- Human-like jitter: 2–5s random sleep after every token acquisition.

### Database Path

- Default: `<project_root>/cs2_analytics.db` (absolute, computed from `config.py` location — safe in Google Drive).
- Docker: `/data/cs2_analytics.db` (named volume `cs2_data`).
- Never load `.db` file into AI context — provide DDL schema only (`sqlite3 cs2_analytics.db .schema`).
