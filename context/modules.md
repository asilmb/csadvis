# Module Contracts (2026-04-08)

## Engine

```python
engine/investment.py:
  compute_investment_signal(container_name, base_cost, container_type,
      current_price, mean_price, quantity, is_event_matched) -> dict
  compute_all_investment_signals(containers, price_data, positions_buy_price={}) -> dict[str, dict]

engine/trade_advisor.py:
  compute_trade_advice(container_name, base_cost, container_type,
      price_history_rows) -> dict

engine/portfolio_advisor.py:
  allocate_portfolio(balance, inventory_items, containers, price_data,
      trade_advice, price_history, invest_signals, positions_map,
      order_book_data) -> dict

engine/correlation.py:
  compute_correlation_matrix(price_history, id_to_name, use_cache=True) -> dict
  _resample_pair(series_i, series_j) -> tuple[list[float], list[float]]

engine/wall_filter.py:
  compute_wall_metrics(order_book) -> dict

engine/armory_pass.py:
  compare_armory_pass(...) -> dict

engine/event_calendar.py:
  get_event_signal(container_type, container_name) -> str
  is_calendar_stale() -> bool
```

## Services

```python
services/item_service.py:
  ItemService.open() -> ItemService   # creates SessionLocal internally
  ItemService(db: Session) -> ItemService
  svc.process_new_price(item_id: str, price: float) -> bool
  svc.close()

services/cache_writer.py:
  refresh_cache(db: Session) -> None
  write_portfolio_advice(db: Session, result: dict) -> None
  write_investment_signals(db: Session, signals: dict, computed_at: datetime) -> None
  _get_ratio_label(value) -> str      # float → CHEAP/EXPENSIVE/NEUTRAL
  _get_momentum_label(value) -> str   # float → RISING/FALLING/STABLE

services/portfolio.py:
  get_portfolio_data() -> dict
  get_cached_portfolio_advice() -> dict
  get_cached_signals() -> dict
  compute_pnl(snapshots) -> dict
  get_snapshots(db) -> list
  save_snapshot(wallet, inventory) -> None
  get_transactions(db) -> list
  add_transaction(db, ...) -> None
  delete_transaction(db, tx_id) -> None
  get_annual_summaries(db) -> list
  upsert_annual(year, pnl) -> None
  get_balance_data(repo=None) -> dict

services/steam_sync.py:
  sync_wallet() -> WalletResult
  sync_inventory(steam_id: str) -> InventoryResult
  sync_transactions(max_pages: int) -> TransactionsResult

services/task_manager.py:
  TaskQueueService().enqueue(type, priority, payload) -> TaskDTO | None
  TaskQueueService().complete(task_id) -> None
  TaskQueueService().fail(task_id, max_retries=3) -> None
  TaskQueueService().reclaim_stuck_tasks() -> int
  TASK_TTL: dict[str, int]
  WORKER_STUCK_THRESHOLD_S: int
```

## Scheduler Tasks (Celery)

```python
scheduler/tasks.py:
  fetch_steam_price.delay(item_id: str) -> AsyncResult
  poll_container_prices_task.delay() -> AsyncResult
  sync_inventory_task.delay() -> AsyncResult
  cleanup_old_history_task.delay() -> AsyncResult
  daily_backup_task.delay() -> AsyncResult
```

Beat schedule (scheduler/celery_app.py):
- `poll_container_prices_task` — every hour
- `sync_inventory_task` — every hour
- `daily_backup_task` — 03:00 UTC daily
- `cleanup_old_history_task` — Sundays 02:00 UTC

## Ingestion

```python
ingestion/steam/client.py:
  SteamMarketClient().__init__()   # reads settings.steam_login_secure
  # ⚠ NOT an async context manager — use directly, not via "async with"
  await client.fetch_history(market_hash_name) -> list[dict]
  await client.fetch_price_overview(market_hash_name) -> dict
  await client.fetch_nameid(market_hash_name) -> int | None
  await client.fetch_order_book(item_nameid) -> dict

ingestion/steam_inventory.py:
  async with SteamInventoryClient() as client:  # IS an async context manager
    await client.fetch(steam_id) -> list[dict]

ingestion/steam_wallet.py:
  get_saved_balance() -> float | None   # reads Redis cs2:wallet:balance
  save_balance(balance) -> None         # writes Redis
  fetch_wallet_balance() -> tuple[float | None, str]

ingestion/nameid_cache.py:
  load_nameid_cache() -> dict[str, int]     # Redis HGETALL cs2:nameid:cache
  save_nameid_cache(cache: dict[str, int])  # Redis pipeline delete + hmset

ingestion/steam_rate_limit.py:
  check_cooldown() -> tuple[bool, str]
  mark_backfill_done() -> None
  last_backfill_str() -> str
```

## Frontend

```python
frontend/app.py:
  create_dash_app() -> Dash

frontend/balance.py:
  render_balance() -> Component   # no DB access, data from services/

frontend/charts.py:
  build_30d_chart(snapshots) -> Figure
  build_monthly_chart(summaries) -> Figure
```

## API Routes

```
GET  /health                    → {"status": "ok", "db": bool, "timestamp": str}
GET  /containers                → list of containers with latest signals
GET  /containers/{id}           → container detail + price history
POST /auth/update_session       → update Steam cookie from Tampermonkey
GET  /stats/...                 → aggregate statistics
GET  /system/...                → worker registry, task queue
POST /sync/...                  → trigger manual sync
```
