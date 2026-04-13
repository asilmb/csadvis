# Architecture — File Tree (2026-04-08)

```
cs2/
├── entrypoint.sh            # Docker entry: init DB → dispatch to app|worker
├── run_dashboard.py         # Dev-only: start Dash standalone (not used in Docker)
├── config.py                # Settings via pydantic-settings (.env), no STORAGE_ROOT
│
├── cli/                     ── CLI PACKAGE ───────────────────────────────────────────
│   ├── __init__.py          # Re-exports main, build_parser
│   ├── __main__.py          # Entry: python -m cli <command>
│   ├── service.py           # cmd_start, cmd_worker, cmd_dashboard
│   └── diagnostics.py       # cmd_status, cmd_monitor, cmd_watchdog,
│                            # cmd_validate_prices, cmd_validate_top
│
├── domain/                  ── DOMAIN LAYER (zero infra deps) ───────────────────────
│   ├── value_objects.py     # Amount, ROI, Percentage (frozen dataclasses)
│   ├── services.py          # SuperDealDomainService, InvestmentDomainService,
│   │                        # compute_smart_buy_price()
│   ├── repositories.py      # InventoryRepository ABC
│   ├── specifications.py    # Specification ABC + And/Or/Not composites
│   │                        # Concrete: PriceWithinRange, ZScoreBelow, VolumeAbove, ROIAbove
│   └── events.py            # DomainEvent + SuperDealDetected, LiquidityWarning, AuthError
│
├── database/
│   ├── models.py            # ORM models (PostgreSQL) — 13 tables
│   ├── connection.py        # PostgreSQL pool, SessionLocal, init_db()
│   ├── postgres_repo.py     # PostgresRepository — production implementation
│   ├── sqlite_repo.py       # SqliteRepository — legacy fallback (not used in Docker)
│   ├── abstract_repo.py     # AbstractRepository ABC
│   ├── factory.py           # get_repository(db) — defaults to postgres
│   ├── repositories.py      # SqlAlchemyInventoryRepository, SqlAlchemyPositionRepository
│   └── dtos.py              # Pydantic transfer objects (ItemDTO, PriceHistoryDTO, etc.)
│
├── engine/                  ── ANALYTICS (pure functions, no infra deps) ──────────
│   ├── investment.py        # BUY/LEAN BUY/HOLD/LEAN SELL/SELL signals
│   ├── trade_advisor.py     # Buy/sell price targets (20th/70th percentile 90d)
│   ├── portfolio_advisor.py # 40/40/20 allocation: flip / invest / reserve
│   ├── correlation.py       # Pearson r on log-returns, 4h in-process cache
│   ├── event_calendar.py    # CS2 tournament calendar → signals, YAML-backed
│   ├── event_loader.py      # load_events(path) — parses data/events.yaml
│   ├── wall_filter.py       # Sell wall depth analysis from order book
│   └── armory_pass.py       # compare_armory_pass() — Pass vs. market cost-benefit
│
├── ingestion/
│   ├── steam/               # Steam Market HTTP client (curl_cffi — TLS impersonation)
│   │   ├── client.py        # SteamMarketClient: fetch_history, fetch_price_overview,
│   │   │                    #   fetch_nameid, fetch_order_book; impersonate="chrome120"
│   │   ├── formatter.py     # to_api_name(), InvalidHashNameError
│   │   ├── parser.py        # parse_history_response(), parse_overview_response()
│   │   ├── mapper.py        # DimContainer → ingestion format
│   │   └── logic.py         # Shared ingestion helpers
│   ├── steam_inventory.py   # SteamInventoryClient (async, supports async with)
│   ├── steam_transactions.py# fetch_market_history(), compute_annual_pnl()
│   ├── steam_wallet.py      # Balance: Redis persistence + Steam Market scrape
│   ├── steam_rate_limit.py  # Request jitter: uniform(6.5, 14.8) s between catalog requests
│   └── nameid_cache.py      # Redis Hash cache: cs2:nameid:cache
│
├── scraper/
│   ├── steam_market_scraper.py  # Scrape Steam Market Search API for container metadata
│   ├── db_writer.py             # Write scraped containers to PostgreSQL
│   ├── runner.py                # Orchestration: sync + async entry points
│   └── state.py                 # Redis state: cs2:scraper:last_parsed
│
├── api/
│   ├── app.py               # FastAPI app factory + /health endpoint
│   ├── schemas.py           # Pydantic schemas
│   └── routes/
│       ├── auth.py          # POST /auth/update_session (Tampermonkey bridge)
│       ├── containers.py    # GET /containers, GET /containers/{id}
│       ├── items.py         # Item management endpoints
│       ├── stats.py         # Statistics endpoints
│       ├── sync.py          # Sync control endpoints
│       └── system.py        # System status endpoints
│
├── services/
│   ├── item_service.py      # ItemService: price validation, sanity check, persistence
│   ├── cache_writer.py      # refresh_cache(), write_portfolio_advice(), write_signals()
│   ├── portfolio.py         # get_portfolio_data(), get_cached_portfolio_advice(), etc.
│   ├── steam_sync.py        # sync_wallet(), sync_inventory(), sync_transactions()
│   ├── signal_handler.py    # Direct-call handlers for domain events (notify_super_deal, etc.)
│   ├── reconciler.py        # PositionReconciler: match inventory items to positions
│   ├── task_manager.py      # TaskQueueService: enqueue, complete, fail, reclaim
│   ├── maintenance.py       # DB garbage collection (VACUUM AUTOCOMMIT, old record cleanup)
│   ├── trade_ledger.py      # Trade history ledger
│   ├── event_logger.py      # Log domain events to event_log table
│   └── webhook_dispatcher.py# POST webhook on SuperDealDetected / AuthError
│
├── scheduler/
│   ├── celery_app.py        # Celery app factory (no beat_schedule — triggered manually)
│   └── tasks.py             # fetch_steam_price, poll_container_prices_task,
│                            # backfill_history_task, sync_inventory_task,
│                            # cleanup_old_history_task, daily_backup_task
│
├── infra/
│   ├── logger.py            # configure_logging() — structlog JSON setup
│   ├── metrics.py           # Stub counters (inc_prices_fetched, inc_steam_429)
│   └── redis_client.py      # get_redis() — shared Redis factory
│
├── frontend/
│   ├── app.py               # Dash app factory + layout
│   ├── callbacks.py         # All Dash callbacks
│   ├── balance.py           # Balance tab layout
│   ├── charts.py            # build_30d_chart(), build_monthly_chart()
│   ├── cache.py             # Flask-Caching config (Redis backend)
│   ├── helpers.py           # Shared layout helpers + color constants
│   ├── inventory.py         # Inventory tab rendering
│   ├── theme.py             # COLORS dict, STYLES dict, verdict_color()
│   └── renderers/
│       ├── market.py        # _render_market() — signal table, sparklines
│       ├── portfolio.py     # _render_portfolio() — 40/40/20 cards
│       ├── analytics.py     # _render_analytics() — correlation heatmap, events
│       ├── inventory.py     # Inventory detailed renderer
│       └── system_status.py # System status tab renderer
│
├── seed/
│   └── data.py              # seed_database(db) — initial container data
│
├── data/
│   └── events.yaml          # CS2 tournament calendar (authoritative source)
│
├── scripts/
│   ├── backup_db.sh         # pg_dump with rotation (⚠ path /app/storage/backups broken — A-03)
│   ├── cleanup_prices.py    # Manual price cleanup utility
│   └── migrate_sqlite_to_pg.py  # One-time migration (⚠ settings.database_path broken — A-05)
│
└── tests/                   # pytest unit tests
    └── *.py                 # 38 files (863 pass, 9 pre-existing failures)
```

## Module Dependency Graph

```
config.py → (no deps)
domain/* → (stdlib only, zero infra)
infra/redis_client.py → redis
infra/logger.py → structlog
infra/coordinator.py → redis, structlog
database/connection.py → config, sqlalchemy (PostgreSQL)
database/models.py → sqlalchemy, config
database/postgres_repo.py → database/connection, database/models
database/repositories.py → domain/repositories, database/models, database/connection
engine/* → config, domain/* (pure functions)
ingestion/steam/* → config, infra/redis_client, curl_cffi
ingestion/nameid_cache.py → infra/redis_client
ingestion/steam_rate_limit.py → stdlib only (random)
ingestion/steam_wallet.py → config, infra/redis_client, httpx
scraper/state.py → infra/redis_client
scraper/runner.py → database/connection, scraper/*
services/item_service.py → database/*, domain/*, config
services/cache_writer.py → database/*, domain/value_objects, engine/*
services/portfolio.py → database/*, domain/*, engine/*
services/steam_sync.py → ingestion/*, database/*, config
services/signal_handler.py → domain/events
scheduler/celery_app.py → redis (broker)
scheduler/tasks.py → scheduler/celery_app, ingestion/steam/*,
                      services/item_service, infra/metrics, database/*
frontend/callbacks.py → services/*, frontend/renderers/*
api/app.py → fastapi
api/routes/* → database/*, services/*, config
cli/__main__.py → cli/service, cli/diagnostics
cli/service.py → database/connection, frontend/app, scheduler/celery_app
cli/diagnostics.py → database/*, engine/*, ingestion/*, services/*
```

## Domain Layer — Strict Isolation

`domain/` has **zero infrastructure dependencies**:
- No SQLAlchemy, no Redis, no httpx, no Dash, no config.py

**Integration points:**
- `engine/portfolio_advisor.py` → instantiates SuperDealDomainService, publishes events
- `engine/investment.py` → uses VolumeAbove spec, publishes LiquidityWarning
- `services/portfolio.py` → calls compute_smart_buy_price, accepts InventoryRepository via DI
- `database/repositories.py` → implements InventoryRepository ABC
- `services/signal_handler.py` → subscribes to domain events
