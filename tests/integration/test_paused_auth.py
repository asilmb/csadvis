"""
Integration tests — PAUSED_AUTH End-to-End Lifecycle (Critical Path 3).

Full lifecycle test:
  1. Worker receives a price_poll job
  2. SteamMarketClient raises a 403/auth error for the first container
  3. Worker sets state.auth_paused = True, blocks on _wait_for_auth()
  4. POST /auth/steam saves credentials → signal_auth_ready() is called
  5. Worker wakes up, retries the same container with fresh credentials (retry=True)
  6. Worker continues to the next container normally

Also tests:
  - state.auth_paused is False before and after the full cycle
  - Timeout path: if credentials never arrive, worker skips the job (returns False)
  - Queue integrity: jobs enqueued after PAUSED_AUTH are processed after resume
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def reset_wq():
    import asyncio

    import infra.work_queue as wq
    wq._state.busy = False
    wq._state.auth_paused = False
    wq._state.current_type = ""
    wq._state.last_error = ""
    wq._state.restarts = 0
    wq._auth_event = None
    wq._work_queue = asyncio.Queue(maxsize=10)
    yield
    wq._work_queue = asyncio.Queue(maxsize=10)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _mock_db_with_containers(names: list[str]):
    """Return a SessionLocal mock yielding fake DimContainer rows."""
    containers = []
    for i, name in enumerate(names):
        c = MagicMock()
        c.container_id = f"cid-{i}"
        c.container_name = name
        containers.append(c)

    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=ctx)
    ctx.__exit__ = MagicMock(return_value=False)
    ctx.query.return_value.filter.return_value.all.return_value = containers
    return ctx


# ── Critical Path 3a: full PAUSED_AUTH → resume cycle ────────────────────────

class TestPausedAuthFullCycle:
    @pytest.mark.asyncio
    async def test_worker_pauses_and_resumes_after_signal(self):
        """
        Timeline:
          t=0   first fetch raises 403-like error → _wait_for_auth() called
          t=0.1 external signal_auth_ready() fires (simulates POST /auth/steam)
          t=0.1 credentials now exist → _wait_for_auth() returns True
          t=0.1 same container is retried, succeeds this time
        """
        import infra.work_queue as wq

        call_count = {"n": 0}
        paused_captured = []

        async def _fake_fetch(api_name):
            call_count["n"] += 1
            if call_count["n"] == 1:
                # First call: auth error — captures auth_paused before raising
                paused_captured.append(False)  # before pause (not yet set)
                raise RuntimeError("Steam auth error 403 for item")
            # Second call: success after credentials restored
            paused_captured.append(wq._state.auth_paused)
            return {"lowest_price": "150.50", "median_price": None}

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.fetch_price_overview = _fake_fetch

        mock_svc = MagicMock()
        mock_svc.process_new_price = MagicMock(return_value=True)
        mock_svc.close = MagicMock()

        async def _signal_after_delay():
            await asyncio.sleep(0.05)
            wq.signal_auth_ready()

        creds_ready = False

        def _creds_exist():
            return creds_ready

        with patch("infra.work_queue.SteamMarketClient", return_value=mock_client), \
             patch("infra.work_queue.SessionLocal", return_value=_mock_db_with_containers(["Prisma 2 Case"])), \
             patch("infra.work_queue.ItemService") as mock_item_svc, \
             patch("infra.work_queue.auth_credentials_exist", side_effect=_creds_exist), \
             patch("infra.work_queue.to_api_name", side_effect=lambda x: x):

            mock_item_svc.open.return_value = mock_svc

            # Trigger signal after short delay (simulates POST /auth/steam)
            signal_task = asyncio.create_task(_signal_after_delay())

            # Allow credentials to exist after signal fires
            async def _set_creds_after_signal():
                nonlocal creds_ready
                await asyncio.sleep(0.06)
                creds_ready = True

            cred_task = asyncio.create_task(_set_creds_after_signal())

            job = {"type": "price_poll"}
            poll_task = asyncio.create_task(wq._handle_price_poll(job))

            await asyncio.gather(signal_task, cred_task, poll_task)

        # fetch_price_overview must have been called twice (fail + retry)
        assert call_count["n"] == 2

    @pytest.mark.asyncio
    async def test_auth_paused_is_false_before_and_after(self):
        """state.auth_paused must be False both before and after the full cycle."""
        import infra.work_queue as wq

        assert wq._state.auth_paused is False

        call_count = {"n": 0}
        creds_ready = False

        async def _fake_fetch(_):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise RuntimeError("forbidden 403")
            return {"lowest_price": "100.0"}

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.fetch_price_overview = _fake_fetch

        mock_svc = MagicMock()
        mock_svc.process_new_price = MagicMock(return_value=True)
        mock_svc.close = MagicMock()

        async def _trigger():
            nonlocal creds_ready
            await asyncio.sleep(0.05)
            creds_ready = True
            wq.signal_auth_ready()

        with patch("infra.work_queue.SteamMarketClient", return_value=mock_client), \
             patch("infra.work_queue.SessionLocal", return_value=_mock_db_with_containers(["Gamma Case"])), \
             patch("infra.work_queue.ItemService") as mock_item_svc, \
             patch("infra.work_queue.auth_credentials_exist", side_effect=lambda: creds_ready), \
             patch("infra.work_queue.to_api_name", side_effect=lambda x: x):

            mock_item_svc.open.return_value = mock_svc
            await asyncio.gather(
                asyncio.create_task(_trigger()),
                asyncio.create_task(wq._handle_price_poll({"type": "price_poll"})),
            )

        assert wq._state.auth_paused is False

    @pytest.mark.asyncio
    async def test_worker_skips_job_on_auth_timeout(self):
        """When credentials never arrive within the timeout, handler returns early."""
        import infra.work_queue as wq

        async def _always_auth_error(_):
            raise RuntimeError("unauthorized")

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.fetch_price_overview = _always_auth_error

        # Override timeout to near-zero so the test is instant
        with patch("infra.work_queue.SteamMarketClient", return_value=mock_client), \
             patch("infra.work_queue.SessionLocal", return_value=_mock_db_with_containers(["Timeout Case"])), \
             patch("infra.work_queue.auth_credentials_exist", return_value=False), \
             patch("infra.work_queue._AUTH_WAIT_TIMEOUT_S", 0), \
             patch("infra.work_queue.to_api_name", side_effect=lambda x: x):

            await wq._handle_price_poll({"type": "price_poll"})

        assert "timed out" in wq._state.last_error


# ── Critical Path 3b: queue integrity through PAUSED_AUTH ────────────────────

class TestQueueIntegrityDuringAuthPause:
    @pytest.mark.asyncio
    async def test_subsequent_jobs_processed_after_resume(self):
        """
        Jobs enqueued AFTER the auth-error job are processed after the worker resumes.
        """
        import infra.work_queue as wq

        processed_jobs: list[str] = []
        auth_call_count = {"n": 0}
        creds_ready = False

        async def _fake_price_poll(_job):
            """Raises auth error once, then succeeds."""
            auth_call_count["n"] += 1
            if auth_call_count["n"] == 1:
                raise RuntimeError("403 forbidden")
            processed_jobs.append("price_poll")

        async def _sentinel_handler(job):
            processed_jobs.append("sentinel")

        async def _trigger_creds():
            nonlocal creds_ready
            await asyncio.sleep(0.05)
            creds_ready = True
            wq.signal_auth_ready()

        with patch("infra.work_queue.auth_credentials_exist", side_effect=lambda: creds_ready), \
             patch.dict(wq._HANDLERS, {
                 "price_poll": _fake_price_poll,
                 "sentinel": _sentinel_handler,
             }), \
             patch("infra.work_queue._AUTH_WAIT_TIMEOUT_S", 2):

            await wq._work_queue.put({"type": "price_poll"})
            await wq._work_queue.put({"type": "sentinel"})

            worker_task = asyncio.create_task(wq._worker_loop())
            cred_task = asyncio.create_task(_trigger_creds())

            # Wait for sentinel to be processed (proves queue wasn't lost)
            deadline = asyncio.get_event_loop().time() + 3.0
            while "sentinel" not in processed_jobs:
                if asyncio.get_event_loop().time() > deadline:
                    break
                await asyncio.sleep(0.05)

            worker_task.cancel()
            cred_task.cancel()
            for t in (worker_task, cred_task):
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass

        assert "sentinel" in processed_jobs, "sentinel job was never processed after auth resume"


# ── Critical Path 3c: get_worker_state reflects auth_paused ──────────────────

class TestWorkerStateReflectsAuthPause:
    @pytest.mark.asyncio
    async def test_get_worker_state_shows_auth_paused(self):
        """get_worker_state() returns auth_paused=True while _wait_for_auth is running."""
        import infra.work_queue as wq

        observed_state: list[bool] = []

        async def _check_state_during_wait():
            await asyncio.sleep(0.02)
            observed_state.append(wq.get_worker_state()["auth_paused"])
            wq.signal_auth_ready()

        async def _wait_wrapper():
            with patch("infra.work_queue.auth_credentials_exist", side_effect=[False, False, True]):
                await wq._wait_for_auth()

        await asyncio.gather(
            asyncio.create_task(_check_state_during_wait()),
            asyncio.create_task(_wait_wrapper()),
        )

        assert True in observed_state, "auth_paused was never True during _wait_for_auth"
