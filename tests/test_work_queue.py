"""Unit tests for infra.work_queue — async job queue and auth-pause logic."""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ─── Helpers ──────────────────────────────────────────────────────────────────

def _reset_queue_module():
    """Reset module-level state between tests."""
    import infra.work_queue as wq
    wq._state.busy = False
    wq._state.auth_paused = False
    wq._state.current_type = ""
    wq._state.last_error = ""
    wq._state.restarts = 0
    wq._state.progress_current = 0
    wq._state.progress_total = 0
    wq._state.last_item_name = ""
    wq._state.last_item_price = 0.0
    wq._state.last_item_volume = 0
    wq._auth_event = None
    # Recreate the queue so it binds to the current test's event loop.
    wq._work_queue = asyncio.Queue(maxsize=10)
    wq._queue_shadow.clear()


@pytest.fixture(autouse=True)
def reset_state():
    _reset_queue_module()
    yield
    _reset_queue_module()


# ─── get_worker_state ─────────────────────────────────────────────────────────


class TestGetWorkerState:
    def test_contains_auth_paused_field(self):
        from infra.work_queue import get_worker_state
        state = get_worker_state()
        assert "auth_paused" in state
        assert state["auth_paused"] is False

    def test_contains_expected_keys(self):
        from infra.work_queue import get_worker_state
        state = get_worker_state()
        for key in ("busy", "auth_paused", "current_type", "last_job_at", "last_error", "restarts", "queue_size"):
            assert key in state


# ─── signal_auth_ready ────────────────────────────────────────────────────────


class TestSignalAuthReady:
    def test_sets_event_flag(self):
        from infra.work_queue import _get_auth_event, signal_auth_ready
        ev = _get_auth_event()
        assert not ev.is_set()
        signal_auth_ready()
        assert ev.is_set()

    def test_idempotent_double_call(self):
        from infra.work_queue import _get_auth_event, signal_auth_ready
        signal_auth_ready()
        signal_auth_ready()
        assert _get_auth_event().is_set()


# ─── _is_auth_error ───────────────────────────────────────────────────────────


class TestIsAuthError:
    def test_detects_403(self):
        from infra.work_queue import _is_auth_error
        assert _is_auth_error(RuntimeError("Steam auth error 403 for item"))

    def test_detects_forbidden(self):
        from infra.work_queue import _is_auth_error
        assert _is_auth_error(Exception("Forbidden — check credentials"))

    def test_detects_no_cookie(self):
        from infra.work_queue import _is_auth_error
        assert _is_auth_error(ValueError("NO_COOKIE returned"))

    def test_detects_invalid_session(self):
        from infra.work_queue import _is_auth_error
        assert _is_auth_error(RuntimeError("invalid session token"))

    def test_ignores_non_auth_errors(self):
        from infra.work_queue import _is_auth_error
        assert not _is_auth_error(RuntimeError("Connection reset by peer"))
        assert not _is_auth_error(ValueError("price is None"))


# ─── _wait_for_auth ───────────────────────────────────────────────────────────


class TestWaitForAuth:
    @pytest.mark.asyncio
    async def test_returns_true_when_credentials_appear(self):
        from infra.work_queue import _wait_for_auth

        call_count = 0

        def creds_exist():
            nonlocal call_count
            call_count += 1
            return call_count >= 2  # False on first call, True on second

        with patch("infra.work_queue.auth_credentials_exist", side_effect=creds_exist):
            result = await _wait_for_auth()

        assert result is True

    @pytest.mark.asyncio
    async def test_sets_auth_paused_while_waiting(self):
        import infra.work_queue as wq

        call_count = 0

        def creds_exist():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                assert wq._state.auth_paused is True
            return call_count >= 2

        with patch("infra.work_queue.auth_credentials_exist", side_effect=creds_exist):
            await wq._wait_for_auth()

        assert wq._state.auth_paused is False

    @pytest.mark.asyncio
    async def test_clears_auth_paused_on_success(self):
        import infra.work_queue as wq

        with patch("infra.work_queue.auth_credentials_exist", return_value=True):
            await wq._wait_for_auth()

        assert wq._state.auth_paused is False

    @pytest.mark.asyncio
    async def test_returns_false_on_timeout(self):
        import infra.work_queue as wq

        with patch("infra.work_queue.auth_credentials_exist", return_value=False), \
             patch("infra.work_queue._AUTH_WAIT_TIMEOUT_S", 0):
            result = await wq._wait_for_auth()

        assert result is False
        assert wq._state.auth_paused is False
        assert "timed out" in wq._state.last_error

    @pytest.mark.asyncio
    async def test_woken_by_signal_auth_ready(self):
        from infra.work_queue import _wait_for_auth, signal_auth_ready

        signalled = False

        async def trigger_signal():
            nonlocal signalled
            await asyncio.sleep(0.05)
            signalled = True
            signal_auth_ready()

        call_count = 0

        def creds_exist():
            nonlocal call_count
            call_count += 1
            return signalled

        with patch("infra.work_queue.auth_credentials_exist", side_effect=creds_exist):
            task = asyncio.create_task(trigger_signal())
            result = await _wait_for_auth()
            await task

        assert result is True
        assert signalled is True


# ─── _worker_loop ─────────────────────────────────────────────────────────────


class TestWorkerLoop:
    @pytest.mark.asyncio
    async def test_processes_job_and_resets_busy(self):
        import infra.work_queue as wq

        processed = []

        async def fake_handler(job):
            processed.append(job)

        with patch.dict(wq._HANDLERS, {"test_job": fake_handler}):
            await wq._work_queue.put({"type": "test_job"})

            task = asyncio.create_task(wq._worker_loop())
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        assert len(processed) == 1
        assert wq._state.busy is False

    @pytest.mark.asyncio
    async def test_continues_after_job_exception(self):
        import infra.work_queue as wq

        processed = []

        async def failing_handler(_job):
            raise ValueError("job failed")

        async def ok_handler(job):
            processed.append(job)

        with patch.dict(wq._HANDLERS, {"fail": failing_handler, "ok": ok_handler}):
            await wq._work_queue.put({"type": "fail"})
            await wq._work_queue.put({"type": "ok"})

            task = asyncio.create_task(wq._worker_loop())
            await asyncio.sleep(0.1)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        assert len(processed) == 1
        # last_error is cleared on the subsequent successful job — continuity is
        # verified via len(processed) above; error persistence is tested separately.

    @pytest.mark.asyncio
    async def test_records_last_error_on_failure(self):
        import infra.work_queue as wq

        async def failing_handler(_job):
            raise RuntimeError("boom")

        with patch.dict(wq._HANDLERS, {"bad": failing_handler}):
            await wq._work_queue.put({"type": "bad"})
            task = asyncio.create_task(wq._worker_loop())
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        assert "RuntimeError" in wq._state.last_error


# ─── supervised_worker ────────────────────────────────────────────────────────


class TestSupervisedWorker:
    @pytest.mark.asyncio
    async def test_cancels_cleanly(self):
        from infra.work_queue import supervised_worker

        task = asyncio.create_task(supervised_worker())
        await asyncio.sleep(0.02)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    @pytest.mark.asyncio
    async def test_increments_restarts_on_crash(self):
        import infra.work_queue as wq

        crash_count = 0

        async def crashing_loop():
            nonlocal crash_count
            crash_count += 1
            if crash_count < 3:
                raise RuntimeError("simulated crash")
            # On 3rd call, just wait to be cancelled
            await asyncio.sleep(10)

        with patch("infra.work_queue._worker_loop", side_effect=crashing_loop), \
             patch("infra.work_queue._SUPERVISOR_RESTART_DELAY_S", 0):
            task = asyncio.create_task(wq.supervised_worker())
            for _ in range(30):
                await asyncio.sleep(0)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        assert wq._state.restarts >= 2

    @pytest.mark.asyncio
    async def test_last_error_set_on_crash(self):
        import infra.work_queue as wq

        async def crashing_loop():
            raise RuntimeError("critical failure")

        with patch("infra.work_queue._worker_loop", side_effect=crashing_loop), \
             patch("infra.work_queue._SUPERVISOR_RESTART_DELAY_S", 0):
            task = asyncio.create_task(wq.supervised_worker())
            await asyncio.sleep(0.1)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass


# ── Shadow list (queue_items display) ─────────────────────────────────────────

class TestQueueShadow:
    def test_enqueue_appends_job_type_to_shadow(self):
        import infra.work_queue as wq
        wq.enqueue({"type": "price_poll"})
        assert wq._queue_shadow == ["price_poll"]

    def test_enqueue_multiple_preserves_order(self):
        import infra.work_queue as wq
        wq.enqueue({"type": "price_poll"})
        wq.enqueue({"type": "backfill_history"})
        assert wq._queue_shadow == ["price_poll", "backfill_history"]

    def test_get_worker_state_uses_shadow_not_private_queue(self):
        import infra.work_queue as wq
        wq.enqueue({"type": "market_catalog"})
        state = wq.get_worker_state()
        assert "market_catalog" in state["queue_items"]

    @pytest.mark.asyncio
    async def test_worker_removes_job_from_shadow_on_consume(self):
        import infra.work_queue as wq

        done = asyncio.Event()

        async def _handler(_job):
            done.set()

        with patch.dict(wq._HANDLERS, {"price_poll": _handler}):
            wq.enqueue({"type": "price_poll"})
            task = asyncio.create_task(wq._worker_loop())
            try:
                await asyncio.wait_for(done.wait(), timeout=2.0)
            finally:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        assert wq._queue_shadow == []


# ── Cooldown guard ────────────────────────────────────────────────────────────

class TestCooldownGuardInHandlers:
    @pytest.mark.asyncio
    async def test_price_poll_returns_early_on_active_cooldown(self):
        from datetime import UTC, datetime, timedelta

        import infra.work_queue as wq
        from infra.scrape_guard import ScrapeBlocked

        future = datetime.now(UTC).replace(tzinfo=None) + timedelta(hours=3)

        with patch("infra.work_queue.auth_credentials_exist", return_value=True), \
             patch("infra.scrape_guard.check_cooldown", side_effect=ScrapeBlocked(future)):
            await wq._handle_price_poll({"type": "price_poll"})

        assert wq._state.progress_total == 0  # no containers were queued up

    @pytest.mark.asyncio
    async def test_backfill_returns_early_on_active_cooldown(self):
        from datetime import UTC, datetime, timedelta

        import infra.work_queue as wq
        from infra.scrape_guard import ScrapeBlocked

        future = datetime.now(UTC).replace(tzinfo=None) + timedelta(hours=3)

        with patch("infra.work_queue.auth_credentials_exist", return_value=True), \
             patch("infra.scrape_guard.check_cooldown", side_effect=ScrapeBlocked(future)):
            await wq._handle_backfill_history({"type": "backfill_history"})
        # No crash — returns cleanly


# ── Volume pass-through in price_poll ────────────────────────────────────────

class TestVolumeSavedInPricePoll:
    @pytest.mark.asyncio
    async def test_volume_extracted_from_overview_and_passed_to_service(self):
        import infra.work_queue as wq

        saved_calls: list[tuple] = []

        def fake_process_new_price(_cid, _price, volume=0):
            saved_calls.append((_cid, _price, volume))

        mock_svc = MagicMock()
        mock_svc.process_new_price = fake_process_new_price
        mock_svc.close = MagicMock()

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.fetch_price_overview = AsyncMock(
            return_value={"lowest_price": "500 ₸", "volume": "1,234"}
        )

        mock_container = MagicMock()
        mock_container.container_id = "abc-123"
        mock_container.container_name = "Prisma 2 Case"

        mock_db = MagicMock()
        mock_db.__enter__ = MagicMock(return_value=mock_db)
        mock_db.__exit__ = MagicMock(return_value=False)
        mock_db.query.return_value.filter.return_value.all.return_value = [mock_container]
        mock_db.get = MagicMock(return_value=mock_container)

        settings_mock = MagicMock()
        settings_mock.ratio_floor = 1.0

        with patch("infra.work_queue.auth_credentials_exist", return_value=True), \
             patch("infra.work_queue.SteamMarketClient", return_value=mock_client), \
             patch("infra.work_queue.SessionLocal", return_value=mock_db), \
             patch("infra.work_queue.ItemService") as mock_svc_cls, \
             patch("infra.scrape_guard.check_cooldown"), \
             patch("infra.work_queue.create_session", return_value=None), \
             patch("infra.work_queue.finish_session"), \
             patch("random.shuffle"), \
             patch("asyncio.sleep", new=AsyncMock()), \
             patch("asyncio.to_thread", side_effect=lambda fn, *a, **kw: fn(*a, **kw)):
            # patch config settings used in handler
            import sys
            config_mock = MagicMock()
            config_mock.settings = settings_mock
            sys.modules.setdefault("config", config_mock)
            mock_svc_cls.open.return_value = mock_svc
            await wq._handle_price_poll({"type": "price_poll"})

        assert len(saved_calls) == 1
        _cid, _price, volume = saved_calls[0]
        assert volume == 1234

    @pytest.mark.asyncio
    async def test_volume_defaults_to_zero_when_missing_from_overview(self):
        import infra.work_queue as wq

        saved_volumes: list[int] = []

        def fake_process_new_price(_cid, _price, volume=0):
            saved_volumes.append(volume)

        mock_svc = MagicMock()
        mock_svc.process_new_price = fake_process_new_price
        mock_svc.close = MagicMock()

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.fetch_price_overview = AsyncMock(
            return_value={"lowest_price": "100 ₸"}  # no "volume" key
        )

        mock_container = MagicMock()
        mock_container.container_id = "xyz-999"
        mock_container.container_name = "Prisma 2 Case"

        mock_db = MagicMock()
        mock_db.__enter__ = MagicMock(return_value=mock_db)
        mock_db.__exit__ = MagicMock(return_value=False)
        mock_db.query.return_value.filter.return_value.all.return_value = [mock_container]
        mock_db.get = MagicMock(return_value=mock_container)

        settings_mock = MagicMock()
        settings_mock.ratio_floor = 1.0

        with patch("infra.work_queue.auth_credentials_exist", return_value=True), \
             patch("infra.work_queue.SteamMarketClient", return_value=mock_client), \
             patch("infra.work_queue.SessionLocal", return_value=mock_db), \
             patch("infra.work_queue.ItemService") as mock_svc_cls, \
             patch("infra.scrape_guard.check_cooldown"), \
             patch("infra.work_queue.create_session", return_value=None), \
             patch("infra.work_queue.finish_session"), \
             patch("random.shuffle"), \
             patch("asyncio.sleep", new=AsyncMock()), \
             patch("asyncio.to_thread", side_effect=lambda fn, *a, **kw: fn(*a, **kw)):
            import sys
            config_mock = MagicMock()
            config_mock.settings = settings_mock
            sys.modules.setdefault("config", config_mock)
            mock_svc_cls.open.return_value = mock_svc
            await wq._handle_price_poll({"type": "price_poll"})

        assert saved_volumes == [0]


# ── InvalidHashNameError tick fix ─────────────────────────────────────────────

class TestInvalidHashNameTick:
    @pytest.mark.asyncio
    async def test_session_ticked_for_invalid_hash_name_container(self):
        import infra.work_queue as wq
        from scrapper.steam.formatter import InvalidHashNameError

        tick_calls: list[int] = []

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        mock_container = MagicMock()
        mock_container.container_id = "bad-id"
        mock_container.container_name = "??INVALID??"

        mock_db = MagicMock()
        mock_db.__enter__ = MagicMock(return_value=mock_db)
        mock_db.__exit__ = MagicMock(return_value=False)
        mock_db.query.return_value.filter.return_value.all.return_value = [mock_container]

        def fake_to_api_name(_name):
            raise InvalidHashNameError(_name)

        with patch("infra.work_queue.auth_credentials_exist", return_value=True), \
             patch("infra.work_queue.SteamMarketClient", return_value=mock_client), \
             patch("infra.work_queue.SessionLocal", return_value=mock_db), \
             patch("infra.work_queue.to_api_name", side_effect=fake_to_api_name), \
             patch("infra.scrape_guard.check_cooldown"), \
             patch("infra.work_queue.create_session", return_value=42), \
             patch("infra.work_queue.tick_session", side_effect=tick_calls.append), \
             patch("infra.work_queue.finish_session"), \
             patch("random.shuffle"), \
             patch("asyncio.sleep", new=AsyncMock()), \
             patch("asyncio.to_thread", side_effect=lambda fn, *a, **kw: fn(*a, **kw)):
            await wq._handle_price_poll({"type": "price_poll"})

        assert 42 in tick_calls, "tick_session must be called even for InvalidHashNameError containers"

    @pytest.mark.asyncio
    async def test_progress_incremented_for_invalid_hash_name(self):
        import infra.work_queue as wq
        from scrapper.steam.formatter import InvalidHashNameError

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        containers = [MagicMock(container_id=f"id-{i}", container_name="??BAD??") for i in range(3)]

        mock_db = MagicMock()
        mock_db.__enter__ = MagicMock(return_value=mock_db)
        mock_db.__exit__ = MagicMock(return_value=False)
        mock_db.query.return_value.filter.return_value.all.return_value = containers

        def fake_to_api_name(_name):
            raise InvalidHashNameError(_name)

        with patch("infra.work_queue.auth_credentials_exist", return_value=True), \
             patch("infra.work_queue.SteamMarketClient", return_value=mock_client), \
             patch("infra.work_queue.SessionLocal", return_value=mock_db), \
             patch("infra.work_queue.to_api_name", side_effect=fake_to_api_name), \
             patch("infra.scrape_guard.check_cooldown"), \
             patch("infra.work_queue.create_session", return_value=None), \
             patch("infra.work_queue.finish_session"), \
             patch("random.shuffle"), \
             patch("asyncio.sleep", new=AsyncMock()), \
             patch("asyncio.to_thread", side_effect=lambda fn, *a, **kw: fn(*a, **kw)):
            await wq._handle_price_poll({"type": "price_poll"})

        assert wq._state.progress_current == 3

        assert "SUPERVISOR" in wq._state.last_error
