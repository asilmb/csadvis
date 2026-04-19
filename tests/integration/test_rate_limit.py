"""
Integration tests — Rate Limit Handling (Critical Path 2).

Verifies that when Steam returns HTTP 429:
  - _trigger_emergency_stop() writes STEALTH_BLOCK_EXPIRES to Redis
  - The client returns an empty result (no exception propagated to the worker)
  - _is_emergency_blocked() gates subsequent requests correctly
  - The job in the asyncio.Queue is not lost — the queue drains to zero
  - Exponential backoff: repeat 429 hits grow the TTL (capped at 6 h)

All Redis interactions are mocked via a fake in-memory store so no Redis
instance is required.
"""
from __future__ import annotations

import asyncio
from contextlib import ExitStack
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ── Fake Redis ────────────────────────────────────────────────────────────────

class _FakeRedis:
    """Minimal in-memory Redis stub used across rate-limit tests."""

    def __init__(self):
        self._store: dict[str, tuple[str, int | None]] = {}  # key → (value, ttl_seconds)
        self._set_calls: list[dict] = []

    def set(self, key, value, *, nx=False, ex=None):
        if nx and key in self._store:
            return False
        self._store[key] = (str(value), ex)
        self._set_calls.append({"key": key, "value": value, "nx": nx, "ex": ex})
        return True

    def get(self, key):
        entry = self._store.get(key)
        return entry[0] if entry else None

    def exists(self, key):
        return 1 if key in self._store else 0

    def expire(self, key, seconds):
        if key in self._store:
            val, _ = self._store[key]
            self._store[key] = (val, seconds)

    def ttl(self, key):
        entry = self._store.get(key)
        if entry is None:
            return -2
        return entry[1] if entry[1] is not None else -1

    def delete(self, *keys):
        for k in keys:
            self._store.pop(k, None)

    def pipeline(self):
        pipe = MagicMock()
        executed = []
        pipe.set.side_effect = lambda k, v: executed.append(("set", k, v))
        pipe.execute.side_effect = lambda: executed
        return pipe

    def ping(self):
        return True


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def reset_wq_state():
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


@pytest.fixture
def fake_redis():
    return _FakeRedis()


# ── Critical Path 2a: _trigger_emergency_stop writes to Redis ─────────────────

class TestTriggerEmergencyStop:
    def test_writes_stealth_key(self, fake_redis):
        from scrapper.steam.client import _STEALTH_KEY, _trigger_emergency_stop
        with patch("scrapper.steam.client.get_redis", return_value=fake_redis):
            ttl = _trigger_emergency_stop("Prisma 2 Case", attempt=0)
        assert fake_redis.exists(_STEALTH_KEY) == 1
        assert ttl == 120  # attempt=0 → base TTL 120s

    def test_exponential_backoff_grows_ttl(self, fake_redis):
        from scrapper.steam.client import _trigger_emergency_stop
        with patch("scrapper.steam.client.get_redis", return_value=fake_redis):
            ttl0 = _trigger_emergency_stop("Item A", attempt=0)
            # Clear and retry with attempt=1
            fake_redis.delete("STEALTH_BLOCK_EXPIRES")
            ttl1 = _trigger_emergency_stop("Item A", attempt=1)
        assert ttl1 == ttl0 * 2  # 240s

    def test_nx_flag_prevents_shortening_existing_block(self, fake_redis):
        from scrapper.steam.client import _STEALTH_KEY, _trigger_emergency_stop
        # Pre-seed a long block
        fake_redis.set(_STEALTH_KEY, "existing", ex=7200)
        with patch("scrapper.steam.client.get_redis", return_value=fake_redis):
            _trigger_emergency_stop("Item B", attempt=0)
        # Original long TTL must be preserved
        assert fake_redis.ttl(_STEALTH_KEY) == 7200

    def test_ttl_capped_at_emergency_block_duration(self, fake_redis):
        from scrapper.steam.client import _BACKOFF_CAP_SECONDS, _trigger_emergency_stop
        with patch("scrapper.steam.client.get_redis", return_value=fake_redis):
            ttl = _trigger_emergency_stop("Item C", attempt=99)
        assert ttl == _BACKOFF_CAP_SECONDS


# ── Critical Path 2b: _is_emergency_blocked gates requests ───────────────────

class TestIsEmergencyBlocked:
    def test_returns_false_when_no_key(self, fake_redis):
        from scrapper.steam.client import _is_emergency_blocked
        with patch("scrapper.steam.client.get_redis", return_value=fake_redis):
            assert _is_emergency_blocked() is False

    def test_returns_true_when_key_present(self, fake_redis):
        from scrapper.steam.client import _STEALTH_KEY, _is_emergency_blocked
        fake_redis.set(_STEALTH_KEY, "blocker", ex=3600)
        with patch("scrapper.steam.client.get_redis", return_value=fake_redis):
            assert _is_emergency_blocked() is True

    def test_returns_false_on_redis_exception(self):
        from scrapper.steam.client import _is_emergency_blocked
        broken_redis = MagicMock()
        broken_redis.exists.side_effect = ConnectionError("Redis down")
        with patch("scrapper.steam.client.get_redis", return_value=broken_redis):
            assert _is_emergency_blocked() is False


# ── Critical Path 2c: fetch_price_overview returns {} on 429 ─────────────────

class TestFetchPriceOverviewOn429:
    @pytest.mark.asyncio
    async def test_returns_empty_dict_on_429(self, fake_redis):
        from scrapper.steam.client import SteamMarketClient

        mock_resp = MagicMock()
        mock_resp.status_code = 429

        with patch("scrapper.steam.client._cred_manager") as mock_cred, \
             patch("scrapper.steam.client.get_redis", return_value=fake_redis):
            mock_cred.return_value.credentials_exist.return_value = True
            mock_cred.return_value.get_credentials.return_value = ("ls_val", "si_val")

            client = SteamMarketClient.__new__(SteamMarketClient)
            client._attempt = 0
            client._session = MagicMock()
            client._session.get = AsyncMock(return_value=mock_resp)

            result = await client.fetch_price_overview("Prisma 2 Case")

        assert result == {}
        from scrapper.steam.client import _STEALTH_KEY
        assert fake_redis.exists(_STEALTH_KEY) == 1

    @pytest.mark.asyncio
    async def test_fetch_history_returns_empty_on_429(self, fake_redis):
        from scrapper.steam.client import SteamMarketClient

        mock_resp = MagicMock()
        mock_resp.status_code = 429

        with patch("scrapper.steam.client._cred_manager") as mock_cred, \
             patch("scrapper.steam.client.get_redis", return_value=fake_redis):
            mock_cred.return_value.credentials_exist.return_value = True
            mock_cred.return_value.get_credentials.return_value = ("ls_val", "si_val")

            client = SteamMarketClient.__new__(SteamMarketClient)
            client._attempt = 0
            client._session = MagicMock()
            client._session.get = AsyncMock(return_value=mock_resp)

            result = await client.fetch_history("Prisma 2 Case")

        assert result == []


# ── Critical Path 2d: job not lost from queue after 429 ──────────────────────

class TestQueueIntegrityAfter429:
    @pytest.mark.asyncio
    async def test_job_not_lost_when_client_returns_empty(self):
        """
        Even when SteamMarketClient returns {} on every call (simulating 429),
        the worker completes _handle_price_poll without dropping subsequent jobs.
        """
        import infra.work_queue as wq

        # Enqueue two jobs: price_poll then a sentinel custom job
        sentinel_processed = asyncio.Event()

        async def _sentinel_handler(_job):
            sentinel_processed.set()

        # Patch SteamMarketClient to always return {}
        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.fetch_price_overview = AsyncMock(return_value={})

        mock_container = MagicMock()
        mock_container.container_id = "cid-1"
        mock_container.container_name = "Prisma 2 Case"

        mock_db_ctx = MagicMock()
        mock_db_ctx.__enter__ = MagicMock(return_value=mock_db_ctx)
        mock_db_ctx.__exit__ = MagicMock(return_value=False)
        mock_db_ctx.query.return_value.filter.return_value.all.return_value = [mock_container]

        with patch("infra.work_queue.SteamMarketClient", return_value=mock_client), \
             patch("infra.work_queue.SessionLocal", return_value=mock_db_ctx), \
             patch("random.uniform", return_value=0), \
             patch.dict(wq._HANDLERS, {"sentinel": _sentinel_handler}):

            await wq._work_queue.put({"type": "price_poll"})
            await wq._work_queue.put({"type": "sentinel"})

            task = asyncio.create_task(wq._worker_loop())
            try:
                await asyncio.wait_for(sentinel_processed.wait(), timeout=3.0)
            finally:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        assert sentinel_processed.is_set(), "sentinel job was lost — queue not drained after 429"

    @pytest.mark.asyncio
    async def test_emergency_block_skips_all_fetches(self, fake_redis):
        """When STEALTH_BLOCK_EXPIRES is set, fetch_price_overview returns {} immediately."""
        from scrapper.steam.client import _STEALTH_KEY, SteamMarketClient
        fake_redis.set(_STEALTH_KEY, "block", ex=3600)

        with patch("scrapper.steam.client._cred_manager") as mock_cred, \
             patch("scrapper.steam.client.get_redis", return_value=fake_redis):
            mock_cred.return_value.credentials_exist.return_value = True
            mock_cred.return_value.get_credentials.return_value = ("ls_val", "si_val")

            client = SteamMarketClient.__new__(SteamMarketClient)
            client._attempt = 0
            client._session = MagicMock()
            client._session.get = AsyncMock()  # should NOT be called

            result = await client.fetch_price_overview("Prisma 2 Case")

        assert result == {}
        client._session.get.assert_not_called()


# ── Critical Path 2e: finish_session not called when 429 aborts a running job ──

class TestPricePollAbortsOn429:
    """price_poll must preserve the session (not call finish_session) when 429 fires mid-job."""

    def _make_db_mock(self, containers):
        mock_db = MagicMock()
        mock_db.__enter__ = MagicMock(return_value=mock_db)
        mock_db.__exit__ = MagicMock(return_value=False)
        # Support arbitrary .filter().filter()...filter().all() chains:
        # the handler applies 1 filter normally, 2 when container_ids is set.
        q = MagicMock()
        q.all.return_value = containers
        q.filter.return_value = q  # self-referential so chained .filter() stays on q
        mock_db.query.return_value.filter.return_value = q
        return mock_db

    def _make_client_mock(self, responses: list):
        """Mock Steam client whose fetch_price_overview returns responses in sequence."""
        idx = 0

        async def fake_fetch(_name):
            nonlocal idx
            r = responses[min(idx, len(responses) - 1)]
            idx += 1
            return r

        mock = MagicMock()
        mock.__aenter__ = AsyncMock(return_value=mock)
        mock.__aexit__ = AsyncMock(return_value=False)
        mock.fetch_price_overview = fake_fetch
        return mock

    def _base_patches(self, containers, client_mock, blocked_seq, session_id=99):
        settings_mock = MagicMock()
        settings_mock.ratio_floor = 1.0
        import sys
        sys.modules.setdefault("config", MagicMock(settings=settings_mock))

        return [
            patch("infra.work_queue.auth_credentials_exist", return_value=True),
            patch("infra.work_queue.SteamMarketClient", return_value=client_mock),
            patch("infra.work_queue.SessionLocal", return_value=self._make_db_mock(containers)),
            patch("infra.work_queue.ItemService"),
            patch("infra.scrape_guard.check_cooldown"),
            patch("infra.scrape_guard.create_session", return_value=session_id),
            patch("scrapper.steam.client._is_emergency_blocked", side_effect=blocked_seq),
            patch("random.shuffle"),
            patch("asyncio.sleep", new=AsyncMock()),
            patch("asyncio.to_thread", side_effect=lambda fn, *a, **kw: fn(*a, **kw)),
        ]

    @pytest.mark.asyncio
    async def test_finish_session_not_called_when_429_mid_job(self):
        """finish_session must NOT be called when 429 fires during a running price_poll."""
        import infra.work_queue as wq

        finish_calls: list = []
        tick_calls: list = []

        containers = [
            MagicMock(container_id=f"cid-{i}", container_name="Prisma 2 Case")
            for i in range(3)
        ]
        client_mock = self._make_client_mock([
            {"lowest_price": "500 ₸", "volume": "100"},  # item 0: success
            {},                                            # item 1: 429 response
        ])
        # _is_emergency_blocked: False after item 0, True after item 1
        blocked = iter([False, True])

        patches = self._base_patches(containers, client_mock, blocked)
        patches.append(patch("infra.scrape_guard.tick_session", side_effect=tick_calls.append))
        patches.append(patch("infra.scrape_guard.finish_session", side_effect=finish_calls.append))

        with ExitStack() as stack:
            mocks = [stack.enter_context(p) for p in patches]
            mock_svc = MagicMock(process_new_price=MagicMock(), close=MagicMock())
            mocks[3].open.return_value = mock_svc  # ItemService
            await wq._handle_price_poll({"type": "price_poll"})

        assert finish_calls == [], "finish_session must NOT be called — session must stay open for resume"
        assert tick_calls.count(99) == 1, "only item 0 should be ticked before the 429"

    @pytest.mark.asyncio
    async def test_429_item_itself_is_not_ticked(self):
        """The item whose fetch triggered 429 must not be counted as processed."""
        import infra.work_queue as wq

        tick_calls: list = []

        containers = [
            MagicMock(container_id=f"cid-{i}", container_name="Prisma 2 Case")
            for i in range(4)
        ]
        client_mock = self._make_client_mock([
            {"lowest_price": "500 ₸", "volume": "100"},  # item 0
            {"lowest_price": "600 ₸", "volume": "200"},  # item 1
            {},                                            # item 2: 429
            {},                                            # item 3: never reached
        ])
        # False × 2 for items 0+1, True for item 2
        blocked = iter([False, False, True])

        patches = self._base_patches(containers, client_mock, blocked, session_id=77)
        patches.append(patch("infra.scrape_guard.tick_session", side_effect=tick_calls.append))
        patches.append(patch("infra.scrape_guard.finish_session"))

        with ExitStack() as stack:
            mocks = [stack.enter_context(p) for p in patches]
            mock_svc = MagicMock(process_new_price=MagicMock(), close=MagicMock())
            mocks[3].open.return_value = mock_svc
            await wq._handle_price_poll({"type": "price_poll"})

        assert tick_calls.count(77) == 2, "items 0+1 ticked, items 2+3 must NOT be ticked"


# ── Critical Path 2f: session resume after 429 abort ─────────────────────────

class TestResumeAfterAbort:
    """After a 429 abort, remaining_ids is correct and a resumed job finishes the session."""

    @pytest.fixture(autouse=True)
    def sqlite_scrape_guard(self, monkeypatch):
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        from src.domain.models import Base
        import src.domain.connection as _conn

        engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)

        class _Ctx:
            def __init__(self):
                self._s = Session()
            def __enter__(self):
                return self._s
            def __exit__(self, *_):
                self._s.close()

        # scrape_guard functions do `from src.domain.connection import SessionLocal` locally,
        # so we patch at the source module — not on scrape_guard itself.
        monkeypatch.setattr(_conn, "SessionLocal", _Ctx)
        self.engine = engine
        self.Session = Session

    def _make_db_mock(self, containers):
        mock_db = MagicMock()
        mock_db.__enter__ = MagicMock(return_value=mock_db)
        mock_db.__exit__ = MagicMock(return_value=False)
        q = MagicMock()
        q.all.return_value = containers
        q.filter.return_value = q
        mock_db.query.return_value.filter.return_value = q
        return mock_db

    def _common_patches(self, containers, fetch_response, blocked_value):
        settings_mock = MagicMock()
        settings_mock.ratio_floor = 1.0
        import sys
        sys.modules.setdefault("config", MagicMock(settings=settings_mock))

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.fetch_price_overview = fetch_response

        return mock_client, [
            patch("infra.work_queue.auth_credentials_exist", return_value=True),
            patch("infra.work_queue.SteamMarketClient", return_value=mock_client),
            patch("infra.work_queue.SessionLocal", return_value=self._make_db_mock(containers)),
            patch("infra.work_queue.ItemService"),
            patch("infra.scrape_guard.check_cooldown"),
            patch("scrapper.steam.client._is_emergency_blocked", side_effect=blocked_value),
            patch("random.shuffle"),
            patch("asyncio.sleep", new=AsyncMock()),
            patch("asyncio.to_thread", side_effect=lambda fn, *a, **kw: fn(*a, **kw)),
        ]

    @pytest.mark.asyncio
    async def test_remaining_ids_correct_after_abort(self):
        """After price_poll aborts at item K, remaining_ids returns items K..N-1."""
        import infra.work_queue as wq
        from infra.scrape_guard import remaining_ids
        from src.domain.models import ScrapeSession

        cids = ["cid-A", "cid-B", "cid-C", "cid-D"]
        containers = [
            MagicMock(container_id=cid, container_name="Prisma 2 Case")
            for cid in cids
        ]

        call_count = 0
        async def fetch(name):
            nonlocal call_count
            call_count += 1
            return {"lowest_price": "500 ₸", "volume": "100"} if call_count == 1 else {}

        # False for item 0 (processed ok), True for item 1 (429 hit)
        blocked = iter([False, True])

        mock_client, patches = self._common_patches(containers, fetch, blocked)

        with ExitStack() as stack:
            mocks = [stack.enter_context(p) for p in patches]
            mock_svc = MagicMock(process_new_price=MagicMock(), close=MagicMock())
            mocks[3].open.return_value = mock_svc
            await wq._handle_price_poll({"type": "price_poll"})

        with self.Session() as s:
            sessions = s.query(ScrapeSession).all()
        assert len(sessions) == 1, "session must NOT be deleted after 429 abort"
        sess = sessions[0]
        assert sess.processed_count == 1, "only item 0 was processed before 429"
        assert remaining_ids(sess.id) == cids[1:], "cid-B, C, D must remain for resume"

    @pytest.mark.asyncio
    async def test_resumed_job_calls_finish_session(self):
        """A resumed job (session_id + remaining container_ids) calls finish_session."""
        import infra.work_queue as wq
        from infra.scrape_guard import create_session, remaining_ids
        from infra.scrape_guard import tick_session as real_tick

        # Simulate: first run processed cid-A only
        cids = ["cid-A", "cid-B", "cid-C"]
        session_id = create_session("price_poll", cids)
        real_tick(session_id)  # processed_count → 1

        remaining = remaining_ids(session_id)
        assert remaining == ["cid-B", "cid-C"]

        containers = [
            MagicMock(container_id=cid, container_name="Prisma 2 Case")
            for cid in remaining
        ]
        finish_calls: list = []

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.fetch_price_overview = AsyncMock(
            return_value={"lowest_price": "500 ₸", "volume": "100"}
        )

        import sys
        sys.modules.setdefault("config", MagicMock(settings=MagicMock(ratio_floor=1.0)))

        with patch("infra.work_queue.auth_credentials_exist", return_value=True), \
             patch("infra.work_queue.SteamMarketClient", return_value=mock_client), \
             patch("infra.work_queue.SessionLocal", return_value=self._make_db_mock(containers)), \
             patch("infra.work_queue.ItemService") as mock_isvc, \
             patch("infra.scrape_guard.check_cooldown"), \
             patch("infra.scrape_guard.tick_session"), \
             patch("infra.scrape_guard.finish_session", side_effect=finish_calls.append), \
             patch("scrapper.steam.client._is_emergency_blocked", return_value=False), \
             patch("random.shuffle"), \
             patch("asyncio.sleep", new=AsyncMock()), \
             patch("asyncio.to_thread", side_effect=lambda fn, *a, **kw: fn(*a, **kw)):

            mock_svc = MagicMock(process_new_price=MagicMock(), close=MagicMock())
            mock_isvc.open.return_value = mock_svc

            await wq._handle_price_poll({
                "type": "price_poll",
                "container_ids": remaining,
                "session_id": session_id,
            })

        assert finish_calls == [session_id], \
            "finish_session must be called with the correct session_id on successful resume"
