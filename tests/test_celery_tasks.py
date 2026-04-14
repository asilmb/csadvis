"""
Unit tests for scheduler/tasks.py — Celery task coverage.

All tests are pure: no real Redis, no real DB, no real network.

Covers:
  _is_stealth_blocked:
    - returns False when key is absent in Redis
    - returns True when key exists in Redis
    - returns False (safe default) on Redis connection error

  _trigger_stealth_block:
    - calls setex with the correct 6-hour TTL

  fetch_steam_price:
    - returns None immediately when stealth-blocked (retry is triggered)
    - returns None when container_id not found in DB
    - returns None when container is blacklisted

  poll_container_prices_task:
    - returns skip summary when stealth-blocked (no self.retry)
    - calls fetch_steam_price.delay once per non-blacklisted container
    - returns queued count

  sync_inventory_task:
    - returns {"status": "skipped"} when settings.steam_id is empty (no self.retry)

  cleanup_old_history_task:
    - calls repo.downsample_old_prices and returns correct summary dict

  daily_backup_task:
    - returns ok dict on zero exit code
    - calls self.retry on non-zero exit code
    - calls self.retry when script is not found
"""

from __future__ import annotations

import subprocess
from unittest.mock import MagicMock, call, patch

import pytest


# ─── _is_stealth_blocked ──────────────────────────────────────────────────────


class TestIsStealthBlocked:
    def test_returns_false_when_key_absent(self):
        from scheduler.tasks import _is_stealth_blocked

        mock_r = MagicMock()
        mock_r.exists.return_value = 0

        with patch("scheduler.tasks._redis", return_value=mock_r):
            assert _is_stealth_blocked() is False

    def test_returns_true_when_key_present(self):
        from scheduler.tasks import _is_stealth_blocked

        mock_r = MagicMock()
        mock_r.exists.return_value = 1

        with patch("scheduler.tasks._redis", return_value=mock_r):
            assert _is_stealth_blocked() is True

    def test_returns_false_on_redis_error(self):
        """Safe default: if Redis is down, don't block the worker."""
        from scheduler.tasks import _is_stealth_blocked

        with patch("scheduler.tasks._redis", side_effect=Exception("connection refused")):
            assert _is_stealth_blocked() is False


# ─── _trigger_stealth_block ───────────────────────────────────────────────────


class TestTriggerStealthBlock:
    def test_sets_key_with_6h_ttl(self):
        from scheduler.tasks import _STEALTH_TTL, _trigger_stealth_block

        mock_r = MagicMock()
        with patch("scheduler.tasks._redis", return_value=mock_r):
            _trigger_stealth_block("test_reason")

        mock_r.setex.assert_called_once()
        _, ttl_arg, _ = mock_r.setex.call_args[0]
        assert ttl_arg == _STEALTH_TTL  # 6 hours

    def test_does_not_raise_on_redis_error(self):
        """Silently handles Redis write failures."""
        from scheduler.tasks import _trigger_stealth_block

        with patch("scheduler.tasks._redis", side_effect=Exception("redis down")):
            _trigger_stealth_block("reason")  # must not raise


# ─── fetch_steam_price ────────────────────────────────────────────────────────


class TestFetchSteamPrice:
    def test_stealth_blocked_calls_retry(self):
        """When stealth-blocked, task calls self.retry and returns."""
        from celery.exceptions import Retry

        from scheduler.tasks import fetch_steam_price

        mock_self = MagicMock()
        mock_self.retry.side_effect = Retry("blocked")

        with patch("scheduler.tasks._is_stealth_blocked", return_value=True):
            with pytest.raises(Retry):
                fetch_steam_price.run.__func__(mock_self, "some-id")

        mock_self.retry.assert_called_once()

    def test_container_not_found_returns_none(self):
        """Missing container_id → returns None without touching Steam."""
        from scheduler.tasks import fetch_steam_price

        mock_db = MagicMock()
        mock_db.get.return_value = None

        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_db)
        mock_session.__exit__ = MagicMock(return_value=False)

        with patch("scheduler.tasks._is_stealth_blocked", return_value=False), \
             patch("src.domain.connection.SessionLocal", return_value=mock_session):
            result = fetch_steam_price.run("nonexistent-uuid")

        assert result is None

    def test_blacklisted_container_returns_none(self):
        """Blacklisted container → returns None without touching Steam."""
        from scheduler.tasks import fetch_steam_price

        mock_container = MagicMock()
        mock_container.is_blacklisted = True

        mock_db = MagicMock()
        mock_db.get.return_value = mock_container

        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_db)
        mock_session.__exit__ = MagicMock(return_value=False)

        with patch("scheduler.tasks._is_stealth_blocked", return_value=False), \
             patch("src.domain.connection.SessionLocal", return_value=mock_session):
            result = fetch_steam_price.run("blacklisted-uuid")

        assert result is None


# ─── poll_container_prices_task ───────────────────────────────────────────────


class TestPollContainerPricesTask:
    def test_stealth_blocked_returns_skip_summary(self):
        """When stealth-blocked, returns a summary without queuing anything."""
        from scheduler.tasks import poll_container_prices_task

        with patch("scheduler.tasks._is_stealth_blocked", return_value=True):
            result = poll_container_prices_task.run()

        assert result["queued"] == 0
        assert result["skipped_block"] == 1

    def test_enqueues_for_each_non_blacklisted_container(self):
        """One delay() call per container returned from DB."""
        from scheduler.tasks import fetch_steam_price, poll_container_prices_task

        containers = [MagicMock(container_id=f"cid-{i}", container_name=f"Case {i}") for i in range(3)]

        mock_db = MagicMock()
        # First query: containers; second query (FactContainerPrice history): returns empty list
        mock_db.query.return_value.filter.return_value.all.return_value = containers
        mock_db.query.return_value.distinct.return_value.all.return_value = []

        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_db)
        mock_session.__exit__ = MagicMock(return_value=False)

        with patch("scheduler.tasks._is_stealth_blocked", return_value=False), \
             patch("src.domain.connection.SessionLocal", return_value=mock_session), \
             patch("scheduler.tasks.fetch_steam_price") as mock_task, \
             patch("scheduler.tasks.backfill_history_task") as mock_backfill:
            result = poll_container_prices_task.run()

        assert mock_task.delay.call_count == 3
        assert result["queued"] == 3
        # All 3 containers have no history → backfill triggered
        mock_backfill.delay.assert_called_once()

    def test_no_backfill_when_all_containers_have_history(self):
        """When every container has history, backfill_history_task is NOT triggered."""
        from scheduler.tasks import poll_container_prices_task

        containers = [MagicMock(container_id=f"cid-{i}", container_name=f"Case {i}") for i in range(2)]

        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.all.return_value = containers
        # All container IDs already have history
        mock_db.query.return_value.distinct.return_value.all.return_value = [
            (f"cid-{i}",) for i in range(2)
        ]

        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_db)
        mock_session.__exit__ = MagicMock(return_value=False)

        with patch("scheduler.tasks._is_stealth_blocked", return_value=False), \
             patch("src.domain.connection.SessionLocal", return_value=mock_session), \
             patch("scheduler.tasks.fetch_steam_price"), \
             patch("scheduler.tasks.backfill_history_task") as mock_backfill:
            result = poll_container_prices_task.run()

        mock_backfill.delay.assert_not_called()
        assert result["backfill_triggered"] == 0


# ─── sync_inventory_task ──────────────────────────────────────────────────────


class TestSyncInventoryTask:
    def test_empty_steam_id_returns_skipped(self):
        """If settings.steam_id is blank, return skipped without HTTP call."""
        from scheduler.tasks import sync_inventory_task

        mock_settings = MagicMock()
        mock_settings.steam_id = "   "  # whitespace-only → empty after strip

        with patch("scheduler.tasks._is_stealth_blocked", return_value=False), \
             patch("config.settings", mock_settings):
            result = sync_inventory_task.run()

        assert result["status"] == "skipped"
        assert "steam_id" in result.get("reason", "")


# ─── cleanup_old_history_task ────────────────────────────────────────────────


class TestCleanupOldHistoryTask:
    def test_returns_ok_summary(self):
        """Normal run: delegates to repo.downsample_old_prices, returns summary."""
        from scheduler.tasks import cleanup_old_history_task

        mock_repo = MagicMock()
        mock_repo.downsample_old_prices.return_value = (150, 30)

        mock_db = MagicMock()
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_db)
        mock_session.__exit__ = MagicMock(return_value=False)

        with patch("src.domain.connection.SessionLocal", return_value=mock_session), \
             patch("src.domain.postgres_repo.PostgresRepository", return_value=mock_repo):
            result = cleanup_old_history_task.run()

        assert result["status"] == "ok"
        assert result["rows_deleted"] == 150
        assert result["summaries_inserted"] == 30

    def test_calls_retry_on_db_exception(self):
        """DB failure → task calls self.retry."""
        from celery.exceptions import Retry

        from scheduler.tasks import cleanup_old_history_task

        mock_self = MagicMock()
        mock_self.retry.side_effect = Retry("db error")

        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(side_effect=RuntimeError("DB down"))
        mock_session.__exit__ = MagicMock(return_value=False)

        with patch("src.domain.connection.SessionLocal", return_value=mock_session):
            with pytest.raises(Retry):
                cleanup_old_history_task.run.__func__(mock_self)

        mock_self.retry.assert_called_once()


# ─── daily_backup_task ───────────────────────────────────────────────────────


class TestDailyBackupTask:
    def test_success_returns_ok_dict(self):
        """Script exits 0 → returns status=ok."""
        from scheduler.tasks import daily_backup_task

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "Backup complete\n"
        mock_result.stderr = ""

        with patch("scheduler.tasks.subprocess.run", return_value=mock_result):
            result = daily_backup_task.run()

        assert result["status"] == "ok"
        assert result["returncode"] == 0

    def test_nonzero_exit_calls_retry(self):
        """Script exits non-zero → task calls self.retry."""
        from celery.exceptions import Retry

        from scheduler.tasks import daily_backup_task

        mock_self = MagicMock()
        mock_self.retry.side_effect = Retry("script failed")

        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = ""
        mock_result.stderr = "error"

        with patch("scheduler.tasks.subprocess.run", return_value=mock_result):
            with pytest.raises(Retry):
                daily_backup_task.run.__func__(mock_self)

        mock_self.retry.assert_called_once()

    def test_script_not_found_calls_retry(self):
        """Missing script file → task calls self.retry."""
        from celery.exceptions import Retry

        from scheduler.tasks import daily_backup_task

        mock_self = MagicMock()
        mock_self.retry.side_effect = Retry("not found")

        with patch("scheduler.tasks.subprocess.run", side_effect=FileNotFoundError):
            with pytest.raises(Retry):
                daily_backup_task.run.__func__(mock_self)

        mock_self.retry.assert_called_once()

    def test_timeout_calls_retry(self):
        """Script timeout → task calls self.retry."""
        from celery.exceptions import Retry

        from scheduler.tasks import daily_backup_task

        mock_self = MagicMock()
        mock_self.retry.side_effect = Retry("timeout")

        with patch(
            "scheduler.tasks.subprocess.run",
            side_effect=subprocess.TimeoutExpired(cmd="bash", timeout=600),
        ):
            with pytest.raises(Retry):
                daily_backup_task.run.__func__(mock_self)

        mock_self.retry.assert_called_once()


# ─── backfill_history_task ────────────────────────────────────────────────────


class TestBackfillHistoryTask:
    def _make_session(self, containers=None, max_ts_rows=None):
        """Build a mock SessionLocal that returns provided containers and max_ts_rows."""
        containers = containers or []
        max_ts_rows = max_ts_rows or []

        mock_db = MagicMock()
        # containers query: .query(DimContainer).filter(...).all()
        mock_db.query.return_value.filter.return_value.all.return_value = containers
        # max_ts_rows query: .query(...).filter(...).group_by(...).all()
        mock_db.query.return_value.filter.return_value.group_by.return_value.all.return_value = max_ts_rows

        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_db)
        mock_session.__exit__ = MagicMock(return_value=False)
        return mock_session

    def test_stealth_blocked_calls_retry(self):
        """When stealth-blocked, task calls self.retry."""
        from celery.exceptions import Retry

        from scheduler.tasks import backfill_history_task

        mock_self = MagicMock()
        mock_self.retry.side_effect = Retry("blocked")

        with patch("scheduler.tasks._is_stealth_blocked", return_value=True):
            with pytest.raises(Retry):
                backfill_history_task.run.__func__(mock_self)

        mock_self.retry.assert_called_once()

    def test_no_cookie_returns_skipped(self):
        """SteamMarketClient() raises → task returns status=skipped."""
        from scheduler.tasks import backfill_history_task

        mock_container = MagicMock()
        mock_container.container_name = "Recoil Case"
        mock_container.container_id = "uuid-1"

        mock_session = self._make_session(containers=[mock_container])

        with patch("scheduler.tasks._is_stealth_blocked", return_value=False), \
             patch("src.domain.connection.SessionLocal", return_value=mock_session), \
             patch("scrapper.steam.client.SteamMarketClient", side_effect=RuntimeError("no cookie")):
            result = backfill_history_task.run()

        assert result["status"] == "skipped"
        assert result["reason"] == "no_cookie"

    def test_empty_container_list_returns_ok(self):
        """No containers in DB → returns ok immediately."""
        from scheduler.tasks import backfill_history_task

        mock_session = self._make_session(containers=[])

        with patch("scheduler.tasks._is_stealth_blocked", return_value=False), \
             patch("src.domain.connection.SessionLocal", return_value=mock_session):
            result = backfill_history_task.run()

        assert result["status"] == "ok"
        assert result["saved"] == 0

    def test_checkpoint_skips_recently_updated_container(self):
        """Container with recent price row (within 24h) is checkpoint-skipped."""
        from datetime import UTC, datetime, timedelta

        from scheduler.tasks import backfill_history_task

        mock_container = MagicMock()
        mock_container.container_name = "Fracture Case"
        mock_container.container_id = "uuid-2"

        # Timestamp within 24h checkpoint window
        recent_ts = datetime.now(UTC).replace(tzinfo=None) - timedelta(hours=1)
        mock_session = self._make_session(
            containers=[mock_container],
            max_ts_rows=[("uuid-2", recent_ts)],
        )

        mock_steam = MagicMock()

        with patch("scheduler.tasks._is_stealth_blocked", return_value=False), \
             patch("src.domain.connection.SessionLocal", return_value=mock_session), \
             patch("scrapper.steam.client.SteamMarketClient", return_value=mock_steam):
            result = backfill_history_task.run()

        assert result["skipped_checkpoint"] == 1
        assert result["saved"] == 0

    def test_429_sets_stealth_block_and_retries(self):
        """_run raises with '429' → stealth block set + self.retry."""
        from datetime import UTC, datetime, timedelta

        from celery.exceptions import Retry

        from scheduler.tasks import backfill_history_task

        mock_container = MagicMock()
        mock_container.container_name = "Horizon Case"
        mock_container.container_id = "uuid-3"

        # Stale timestamp (beyond 24h checkpoint)
        stale_ts = datetime.now(UTC).replace(tzinfo=None) - timedelta(hours=48)
        mock_session = self._make_session(
            containers=[mock_container],
            max_ts_rows=[("uuid-3", stale_ts)],
        )

        mock_self = MagicMock()
        mock_self.request.retries = 0
        mock_self.retry.side_effect = Retry("429")

        # Simulate 429 by making the event loop raise when run_until_complete is called.
        import asyncio as _asyncio
        mock_loop = MagicMock(spec=_asyncio.AbstractEventLoop)
        mock_loop.run_until_complete.side_effect = Exception("HTTP 429 Too Many Requests")
        mock_steam = MagicMock()

        with patch("scheduler.tasks._is_stealth_blocked", return_value=False), \
             patch("src.domain.connection.SessionLocal", return_value=mock_session), \
             patch("scrapper.steam.client.SteamMarketClient", return_value=mock_steam), \
             patch("scheduler.tasks._trigger_stealth_block") as mock_block, \
             patch("scrapper.steam.formatter.to_api_name", return_value="Horizon+Case"), \
             patch("asyncio.new_event_loop", return_value=mock_loop):
            with pytest.raises(Retry):
                backfill_history_task.run.__func__(mock_self)

        mock_block.assert_called_once()
        mock_self.retry.assert_called_once()
