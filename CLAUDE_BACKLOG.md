# CLAUDE_BACKLOG.md — Master Task List

> Last audit: 2026-04-09. All Evolution phases complete. Current work: post-migration bug fixing and legacy artifact cleanup.

---

## Evolution 1–3: Foundation (Completed)
- [x] **PV-01: Docker Infrastructure** — TimescaleDB, Redis, App
- [x] **PV-02: Repository Pattern** — DAL layer (SQLite/Postgres)
- [x] **PV-03: Schema & Migration** — 742,102 rows transferred to PostgreSQL
- [x] **PV-04: Safe Celery Core** — Celery queue, 429 emergency stop
- [x] **PV-05: Service Layer & DTO** — ItemService, Pydantic DTOs
- [x] **PV-06: Data Integrity & Soft Start** — Sanity check, warmup, Auth Bridge
- [x] **PV-07: Logging & Backup** — structlog JSON, daily pg_dump
- [x] **PV-7.5: Data Downsampling** — weekly aggregation >90 days
- [x] **PV-08: FastAPI REST API** — async endpoints, Swagger
- [x] **PV-57: Ultimate Stealth** — Ghost Scheduler, Jitter, Night Mode
- [x] **PV-58: Currency Agnostic** — KZT-only, no hardcoded USD

---

## БЛОК A — КРИТИЧЕСКИЕ БАГИ

### A-01 · SteamMarketClient не поддерживает async with
**Приоритет: BLOCKER**

`scheduler/tasks.py` использует `async with SteamMarketClient() as client:` в задаче `fetch_steam_price`, но класс `SteamMarketClient` (`ingestion/steam/client.py`) не реализует `__aenter__` / `__aexit__`. Каждый вызов задачи падает с `AttributeError`.

**Следствие:** Ни один контейнер не получает обновлённую цену. Весь ценовой движок мёртв.

**Фикс:** Добавить `__aenter__`/`__aexit__` в `SteamMarketClient`, либо переписать вызов в `tasks.py` на прямой вызов метода.

---

### A-02 · cli/main.py запускает init_db() на уровне модуля
**Приоритет: BLOCKER**

`cli/main.py` строки 50–54 выполняют `init_db()` и `seed_database()` при каждом импорте модуля.

**Фикс:** Перенести `init_db()` + `seed_database()` внутрь lifespan FastAPI.

---

### A-03 · scripts/backup_db.sh: путь /app/storage/backups не существует
**Приоритет: HIGH**

`scripts/backup_db.sh:29` — `BACKUP_DIR="/app/storage/backups"`. Папка `storage/` удалена.

**Фикс:** Изменить `BACKUP_DIR` на `/app/backups`, добавить volume в `docker-compose.yml`.

---

### A-04 · 7 тестовых файлов импортируют удалённый services/worker_engine.py
**Приоритет: HIGH**

Файлы: `test_worker_engine.py`, `test_backfill_handler.py`, `test_market_validator.py`, `test_maintenance.py`, `test_jit_valuation.py`, `test_context_aware_jit.py`, `stress_test.py`.

**Фикс:** Удалить тесты worker_engine, переписать оставшиеся под новые модули.

---

### A-05 · scripts/migrate_sqlite_to_pg.py: ссылка на удалённый settings.database_path
**Приоритет: MEDIUM**

**Plan:**
- Role: Developer
- In `scripts/migrate_sqlite_to_pg.py` line 87: replace `sqlite_path = settings.database_path` with `sqlite_path = os.getenv("DATABASE_PATH", "storage/db/cs2_analytics.db")`
- Ensure `os` is imported at the top of the file
- Verify the script runs without AttributeError in dry-run mode

**Expected result:**
1. Script no longer references deleted config field `settings.database_path`
2. Path falls back to env var with sensible default
3. Script executes without crash in dry-run mode

---

## БЛОК B — СЛОМАННЫЕ ФУНКЦИИ (не верифицированы после миграции)

### B-01 · Скрейпинг контейнеров — неизвестный статус
**Приоритет: HIGH**

**Plan:**
- Role: Developer
- Manually trace the scraper pipeline end-to-end
- Verify `scraper/steam_market_scraper.py → scrape_all_containers()` returns the expected data structure
- Verify `scraper/db_writer.py` writes to the correct PostgreSQL tables (not SQLite)
- Confirm Redis key `cs2:scraper:last_parsed` is written after a successful scrape
- Fix any broken references or wrong table names found

**Expected result:**
1. `scrape_all_containers()` returns correct data structure
2. `db_writer.py` writes to PostgreSQL tables
3. Redis key `cs2:scraper:last_parsed` is updated after each successful scrape

---

### B-02 · Инвентаризация / Reconciler — неизвестный статус
**Приоритет: HIGH**

**Plan:**
- Role: Developer
- Audit `services/reconciler.py` and `database/repositories.py`
- Confirm `SqlAlchemyInventoryRepository.update_trade_unlock_at()` method exists
- Confirm `SqlAlchemyPositionRepository` is defined in `database/repositories.py`
- Trace `sync_inventory_task` end-to-end: Celery task → `SteamInventoryClient.fetch()` → `PositionReconciler.sync()` → DB write
- Fix any missing methods or broken references found

**Expected result:**
1. Both repository classes exist with the required methods
2. `sync_inventory_task` completes without AttributeError
3. Inventory data is persisted to PostgreSQL correctly

---

### B-03 · Бэкфилл истории цен — обработчик удалён
**Приоритет: HIGH**

`backfill_history` ставится в очередь при появлении новых контейнеров, но handler в `services/worker_engine.py` удалён.

**Фикс:** Реализовать Celery-задачу `backfill_history_task` в `scheduler/tasks.py`.

---

### B-04 · sync_wallet() — проверить источник данных
**Приоритет: MEDIUM**

**Plan:**
- Role: Developer
- Open `services/steam_sync.py`, inspect `sync_wallet()`
- Confirm it reads from Redis key `cs2:wallet:balance`, not from `data/wallet_balance.json`
- If still reading from file: rewrite to use `redis_client.get("cs2:wallet:balance")`
- Add a fallback log warning (not a crash) if the Redis key is missing

**Expected result:**
1. `sync_wallet()` reads balance from Redis key `cs2:wallet:balance`
2. No reference to `wallet_balance.json` remains in the method
3. Missing Redis key produces a log warning, not a crash

---

### B-05 · Dashboard — реальный рендеринг данных не проверялся
**Приоритет: HIGH**

**Plan:**
- Role: Developer
- Start the full Docker stack and manually verify each Dashboard tab
- `Анализ` — confirm signals load from `fact_investment_signals`
- `Inventory` — confirm positions display correctly
- `Portfolio` — confirm advice loads from `fact_portfolio_advice`
- `Balance` — confirm snapshots and transactions display
- `Analytics` — confirm correlation and event calendar render
- `System Status` — confirm worker states load from `worker_registry`
- For each broken tab: identify the failing Dash callback or service call, fix it, re-verify

**Expected result:**
1. All 6 tabs render without errors
2. Data shown reflects real PostgreSQL content
3. No "NoneType" or empty state where real data is expected

---

### B-06 · API эндпоинты — не верифицированы против реальной БД
**Приоритет: MEDIUM**

**Plan:**
- Role: Developer
- Start the Docker stack and run requests against each endpoint in `api/routes/items.py` and `api/routes/stats.py` using Swagger UI (`/docs`)
- Confirm each endpoint returns a valid response with real data
- Check for SQL errors or missing fields
- Fix any broken queries or wrong column names introduced during migration

**Expected result:**
1. All endpoints return HTTP 200 with valid payloads
2. No SQLAlchemy errors or missing column references
3. Swagger UI reflects correct response schemas

---

### B-07 · SteamRequestCoordinator — проверить под Celery multi-process
**Приоритет: MEDIUM**

**Plan:**
- Step 1 — Role: Architect
  - Review `infra/coordinator.py` — determine whether token bucket state is stored in-process (broken) or in Redis (correct)
  - Produce findings report: state storage location, concurrency safety, circuit breaker behavior
- Step 2 — Role: Developer
  - If token state is in-process: migrate to Redis using atomic increment/TTL pattern
  - If already in Redis: add a test confirming two concurrent workers share the same rate limit
  - Fix any thread/process safety issues found

**Expected result:**
1. Token bucket state persists in Redis, not in-process memory
2. Multiple Celery workers share the same rate limit correctly
3. Circuit breaker state also persisted in Redis

---

## БЛОК C — АРТЕФАКТЫ МОНОЛИТА

### C-01 — C-05 · Удалить legacy SQLite / монолит файлы
**Приоритет: LOW**

**Plan:**
- Role: Developer
- Delete `database/sqlite_repo.py` (SQLite no longer used)
- Fix comment in `database/factory.py` — remove "SQLite default" note
- Delete `data/wallet_balance.json` (wallet now stored in Redis `cs2:wallet:balance`)
- Delete or mark `run_dashboard.py` as dev-only with a comment
- Update `frontend/helpers.py` lines 314–317 to reference `cli/service.py` instead of `main.py`
- Delete `services/__pycache__/worker_engine.cpython-314.pyc` and all orphaned `__pycache__` directories

**Expected result:**
1. No references to SQLite repo in active code paths
2. Legacy files removed from the repository
3. Comments and docs point to current entry points

---

## БЛОК D — ОТСУТСТВУЮЩИЕ КОМАНДЫ CLI

### D-01 · Восстановить 7 команд CLI из монолита
**Приоритет: HIGH**

**Plan:**
- Role: Developer
- In `cli/service.py`, implement the following missing commands:
  - `backfill` — trigger Celery backfill task
  - `scrape` — trigger `scraper/runner.py`
  - `seed` — trigger `seed/data.py`
  - `reset-db` — with explicit confirmation prompt before executing
  - `cookie` — update Steam session cookie
  - `db prune` — call `services/maintenance.py`
  - `events refresh` — trigger `engine/event_loader.py`
- Register all 7 commands in `cli/__main__.py`

**Expected result:**
1. All 7 commands available via `python -m cli <command>`
2. Each command delegates to the correct service/task
3. `reset-db` requires explicit user confirmation before executing

---

## БЛОК E — МОНИТОРИНГ

### E-01 · Grafana + Prometheus observability stack
**Приоритет: HIGH**

**Plan:**
- Step 1 — Role: Architect
  - Define which metrics to expose: prices/hour, Steam 429 rate, task queue depth, Redis memory, DB pool
  - Identify instrumentation points: `scheduler/tasks.py`, `infra/coordinator.py`
  - Draft docker-compose additions: `prometheus` and `grafana` services with scrape config
- Step 2 — Role: Developer
  - Add `prometheus-client` to Python services
  - Instrument `scheduler/tasks.py` and `infra/coordinator.py` with counters and histograms
  - Add `prometheus` and `grafana` containers to `docker-compose.yml`
  - Create a basic Grafana dashboard JSON: prices fetched per hour, Steam 429 rate, Celery task queue depth

**Expected result:**
1. `prometheus-client` instrumented in tasks and coordinator
2. Prometheus and Grafana added to docker-compose
3. Grafana dashboard visible at localhost with live metrics

---

### E-02 · Подключить webhook-диспетчер к сбоям воркеров
**Приоритет: MEDIUM**

**Plan:**
- Role: Developer
- In `scheduler/tasks.py` or a new `scheduler/signals.py`: connect Celery `task_failure` signal to `services/webhook_dispatcher.py`
- When `fetch_steam_price` fails 3+ consecutive times, dispatch a webhook alert
- Configure failure threshold via env var `ALERT_FAILURE_THRESHOLD` (default: 3)

**Expected result:**
1. Celery failure signal connected to webhook dispatcher
2. Alert fires after threshold consecutive failures
3. Threshold configurable via `ALERT_FAILURE_THRESHOLD` env var

---

## БЛОК F — ЛОГИРОВАНИЕ

### F-01 · Стандартизировать логирование на structlog
**Приоритет: MEDIUM**

**Plan:**
- Role: Developer
- Audit all files in `services/`, `scraper/`, `engine/` for `import logging` and `logging.getLogger()` calls
- Replace each with `structlog.get_logger()` and update log calls accordingly
- In `cli/diagnostics.py`: keep `print()` only for direct user-facing output — all internal events must use structlog
- Do NOT touch `infra/`, `scheduler/tasks.py`, `cli/main.py` — these already use structlog

**Expected result:**
1. No `import logging` remaining in `services/`, `scraper/`, `engine/`
2. All log output in JSON structlog format in containers
3. `cli/diagnostics.py` uses structlog for internal events, print only for user-facing output

---

### F-02 · Добавить ротацию логов для Docker volumes
**Приоритет: LOW**

**Plan:**
- Role: Developer
- Add Docker logging options to each service in `docker-compose.yml` that writes to `logs/app/`, `logs/worker/`, `logs/beat/`:
  ```yaml
  logging:
    driver: "json-file"
    options:
      max-size: "50m"
      max-file: "5"
  ```
- Alternatively, add logrotate config at `docker/logrotate.conf` and mount into the container. Choose the simpler approach.

**Expected result:**
1. Log volumes are bounded (max ~250MB per service)
2. Old log files are rotated automatically
3. No manual cleanup required

---

## БЛОК G — ДОКУМЕНТАЦИЯ

### G-01 · Обновить все context/*.md до текущего стека
**Приоритет: HIGH**

**Plan:**
- Role: Developer
- Update each file in `context/` using `CLAUDE_CONTEXT.md` and `CLAUDE_BACKLOG.md` as source of truth:
  - `architecture.md` — replace old file tree and `main.py`/`cli.py`/`sqlite` references with current structure
  - `overview.md` — replace SQLite + APScheduler in tech stack with PostgreSQL + Celery + Redis
  - `database.md` — replace SQLite WAL schema with current TimescaleDB/PostgreSQL schema
  - `modules.md` — update module contracts to match current `services/`, `scheduler/`, `infra/` layout
  - `testing.md` — update test count, remove references to broken patterns, note that 7 test files are currently broken (A-04)

**Expected result:**
1. All 5 `context/*.md` files describe the current stack
2. No references to SQLite, APScheduler, or old `main.py` entry point
3. Known issues section added pointing to `CLAUDE_BACKLOG.md`

---

## Removed / Postponed
- [ ] ~~PV-09: React SPA~~ — Отложено, сохраняем Dash
- [ ] **PV-10: Dash Performance Tuning** — Callbacks на ItemService и Redis-кэш
  - Step 1 (Architect): Audit all Dash callbacks in `frontend/` — identify which query PostgreSQL directly on each interval tick vs. reading from pre-computed Redis cache or ItemService
  - Step 2 (Frontend Developer): Refactor identified callbacks to read from Redis cache (`services/cache_writer.py`) or call ItemService methods. Measure and confirm reduced DB load.
  - Expected: No PostgreSQL queries triggered on every Dash interval tick
- [ ] **PV-11: Telegram Bot** — Уведомления о профите через FastAPI webhook
  - Step 1 (Architect): Design notification flow — events, FastAPI endpoint, dispatcher format (amount, item name, signal type)
  - Step 2 (Backend Developer): Implement Telegram dispatcher in `services/webhook_dispatcher.py`, add `POST /notify/telegram` endpoint, wire to `fact_investment_signals`; format message as `(amount, item name, signal type)`
  - Step 3 (QA): Unit tests with mocked HTTP client for each event type; verify missing env vars produce clear config error
  - Expected: Bot sends notifications on profit signals; `TELEGRAM_BOT_TOKEN` + `TELEGRAM_CHAT_ID` via env vars
