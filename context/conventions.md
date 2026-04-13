# Strict Rules

## Database
- ORM String matching: Always use `str(Model.col)`.
- Sessions: Use `with SessionLocal() as db:` (context manager), not try/finally.
- Time: `datetime.now(UTC).replace(tzinfo=None)`. Never `utcnow()`.
- Exceptions: Catch specific types, no bare `except:`.
- Queries: Max 2 DB queries per page load (bulk).
- Currency: single account currency — no USD/EUR/RUB conversions.

## Architecture
- **Frontend isolation:** `frontend/` modules must NEVER import `SessionLocal`, ORM models,
  or query the DB directly. All data goes through `services/`. Violations break testability.
- **Cache write guard:** Pattern is always BUILD rows → check empty → DELETE → INSERT.
  An empty result is a no-op — never DELETE without rows to replace.
- **Signal labels:** `ratio_signal` / `momentum_signal` in `FactInvestmentSignal` are derived
  via `_get_ratio_label()` / `_get_momentum_label()` in `cache_writer.py`. Never store raw floats.
- **Correlation resampling:** Always call `_resample_pair()` before `_to_log_returns()`.
  Never compute log-returns on raw date-intersections.

## State Persistence
- All runtime state lives in Redis (no files, no SQLite outside of tests).
- Redis keys namespaced: `cs2:module:key`.
- `STEALTH_BLOCK_EXPIRES` is the shared 429-block key (no namespace — shared by client + tasks).

## Logging
- Production code: `structlog.get_logger()` with `structlog.info("event_name", key=val)`.
- Event names: `snake_case`, no spaces.
- Never log secret values (cookie, token). Mask or omit.

## Async
- `SteamMarketClient` is NOT an async context manager. Use: `client = SteamMarketClient()` then
  `await client.fetch_xxx(...)`. Do NOT use `async with SteamMarketClient() as client:`.
- `SteamInventoryClient` IS an async context manager. Use: `async with SteamInventoryClient() as c:`.
- In Celery tasks: wrap async code in `asyncio.new_event_loop() / loop.run_until_complete()`.
  Never reuse event loops across tasks (multi-process workers).

## Testing
- Pure functions only in unit tests. No network, no real DB.
- Use SQLite in-memory (`sqlite:///:memory:`) for DB-dependent unit tests.
- Use `FakeInventoryRepository` for DI testing.
- Use `AsyncMock` for async clients in sync test contexts.

## Prohibited
- USD/EUR/RUB/GBP conversions anywhere.
- Skinport API, EV/ROI skin calculators, StatTrak items.
- Hardcoded constants (use config.py: key_price, steam_fee_divisor, steam_fee_fixed).
- Direct DB access from `frontend/` modules.
- File-based state (everything goes to Redis or PostgreSQL).
