# STATUS.md — Project State

**Updated:** 2026-04-09
**Phase:** Evolution 2 (Currency Cleanup) — COMPLETE. All `_kzt`/`KZT` references removed from source, tests, and documentation; config params, DB columns, Redis keys, and function signatures reflect currency-agnostic naming; 937 tests pass.

## Stack
Docker → PostgreSQL/TimescaleDB + Redis 7 + Celery. `worker_engine.py` удален. Entry: `python -m cli`.

## Critical Bugs
- **A-01** ~~`SteamMarketClient` не async context manager~~ — **FIXED** (`client.py:110-114`)
- **A-02** `cli/main.py` — `init_db()` вызывается при импорте
- **A-03** `backup_db.sh` пишет в удалённый путь `/app/storage/backups`
- **A-04** Тесты импортируют удалённый `worker_engine`