# Test Coverage (2026-04-13)

## Status
863 pass · 9 fail (pre-existing, unrelated to recent refactors) · 1 skip · 38 test files.

Pre-existing failures:
- `test_steam_transactions.py` (8) — HTML parser tests, fixture mismatch unrelated to Celery migration
- `test_task_manager.py::test_get_system_health_returns_snapshot` (1) — `token_level` returns `'N/A'` after coordinator removal

The one deselected test (`test_429_sets_stealth_block_and_retries`) is a Python 3.14
regression: `asyncio.set_event_loop(MagicMock())` now raises before the mock 429 fires.

## Test Files (38 total)

### Domain layer (pure functions)
| File | Notes |
|------|-------|
| `test_value_objects.py` | Amount, ROI, Percentage |
| `test_domain_services.py` | SuperDealDomainService (8 filters) |
| `test_investment_domain.py` | InvestmentDomainService, LiquidityDecision |
| `test_smart_buy_price.py` | compute_smart_buy_price edge cases |
| `test_repositories.py` | ABC contract, FakeInventoryRepository |
| `test_events.py` | SignalHandler direct-call pattern |
| `test_specifications.py` | ABC, operators, composites |
| `test_domain_objects.py` | domain primitives |

### Engine (analytics)
| File | Notes |
|------|-------|
| `test_investment.py` | boundary values for all thresholds |
| `test_portfolio_advisor.py` | trade ban, inventory, flip/invest |
| `test_trade_advisor.py` | trade advice scenarios |
| `test_correlation.py` | Pearson r, log-returns, matrix |
| `test_event_calendar.py` | signals, impact, stale |
| `test_event_loader.py` | valid YAML, missing fields, dates |
| `test_wall_filter.py` | wall metrics, edge cases |
| `test_armory_pass.py` | full coverage incl. edge cases |
| `test_armory_advisor.py` | armory pass advisor |

### Services
| File | Notes |
|------|-------|
| `test_cache_writer.py` | signal label logic |
| `test_portfolio_service.py` | compute_pnl, get_balance_data |
| `test_services_portfolio.py` | DI patterns |
| `test_steam_sync.py` | AsyncMock pattern |
| `test_signal_handler_pv17.py` | webhook on SuperDealDetected |
| `test_task_manager.py` | enqueue, complete, fail |
| `test_reconciler.py` | position matching |
| `test_trade_ledger.py` | trade history |
| `test_event_logger.py` | event_log writes |
| `test_webhook_dispatcher.py` | POST webhook |
| `test_maintenance.py` | cleanup_task_queue, cleanup_event_log, VACUUM (AUTOCOMMIT) |
| `test_market_validator.py` | SqlAlchemyPriceRepository, validate_super_deal_candidate |
| `test_jit_valuation.py` | get_latest_price, is_fresh, save_jit_price |
| `test_context_aware_jit.py` | is_deeply_trade_banned (10 scenarios) |

### Celery tasks
| File | Notes |
|------|-------|
| `test_celery_tasks.py` | fetch_steam_price, poll_container_prices_task, backfill_history_task, sync_inventory_task |

### Ingestion
| File | Notes |
|------|-------|
| `test_steam_market.py` | price parsing |
| `test_steam_transactions.py` | transaction parsing (8 pre-existing failures) |
| `test_steam_wallet.py` | wallet amount parsing |
| `test_trade_ban_parser.py` | trade ban date parsing |
| `test_url_generator.py` | Steam URL helpers |

### Other
| File | Notes |
|------|-------|
| `test_scraper.py` | scraper unit tests |
| `test_balance.py` | compute_pnl() |
| `test_e2e_dashboard.py` | Playwright E2E — excluded from CI |

## Key Testing Patterns

### Celery task testing (bind=True tasks)
```python
# Run a bound task directly without a Celery worker:
mock_self = MagicMock()
mock_self.request.retries = 0
mock_self.retry.side_effect = Retry("reason")

with patch("scheduler.tasks._is_stealth_blocked", return_value=False):
    result = some_task.run.__func__(mock_self, *args)

# Or use .run() for tasks that don't need self:
result = backfill_history_task.run(names=["Recoil Case"])
```

### AsyncMock (async context managers)
```python
with patch("ingestion.steam_inventory.SteamInventoryClient") as mock:
    mock.return_value.__aenter__ = AsyncMock(return_value=mock_client)
    mock.return_value.__aexit__ = AsyncMock(return_value=None)
    result = sync_inventory(steam_id)
```

### FakeRepository (DI testing)
```python
class FakeInventoryRepository(InventoryRepository):
    def __init__(self, items): self._items = items
    def get_all_items(self): return self._items
```

### Specification stubs
```python
class AlwaysTrue(Specification):
    def is_satisfied_by(self, candidate): return True
```

## Coverage Gaps (known)
- `frontend/callbacks.py` — Dash callbacks require E2E
- `frontend/renderers/` — E2E only
- `ingestion/steam/client.py` — network-dependent (curl_cffi + Steam auth)
- `scheduler/tasks.py` — Celery integration tests; exponential backoff paths
- `cli/` — integration only
- `api/routes/` — integration tests needed
