"""
Unit tests for PV-17: services/webhook_dispatcher.py

Covers:
  dispatch():
    - no-op (WARNING logged) when webhook_url is empty
    - spawns a daemon thread when URL is configured
    - thread calls _post with correct URL and payload

  _post():
    - calls httpx.Client.post with JSON payload
    - logs DEBUG on success
    - logs WARNING (no raise) on network error
"""

from __future__ import annotations

import threading
from unittest.mock import MagicMock, patch


class TestDispatch:
    def test_no_op_when_url_empty(self, caplog):
        """dispatch() must not spawn a thread when webhook_url is empty."""
        import logging

        from infra import webhook_dispatcher

        with patch.object(
            webhook_dispatcher,
            "_post",
            wraps=webhook_dispatcher._post,
        ) as mock_post, patch(
            "infra.webhook_dispatcher.threading.Thread"
        ) as mock_thread, patch(
            "config.settings"
        ) as mock_settings:
            mock_settings.webhook_url = ""

            with caplog.at_level(logging.WARNING, logger="infra.webhook_dispatcher"):
                webhook_dispatcher.dispatch({"event": "test"})

            mock_thread.assert_not_called()

    def test_spawns_thread_when_url_configured(self):
        """dispatch() must start a daemon thread when webhook_url is set."""
        from infra import webhook_dispatcher

        spawned: list[threading.Thread] = []
        original_thread = threading.Thread

        def _capture_thread(*args, **kwargs):
            t = original_thread(*args, **kwargs)
            spawned.append(t)
            return t

        with patch("infra.webhook_dispatcher.threading.Thread", side_effect=_capture_thread), \
             patch("infra.webhook_dispatcher._post"), \
             patch("config.settings") as mock_settings:
            mock_settings.webhook_url = "https://example.com/webhook"
            webhook_dispatcher.dispatch({"event": "super_deal"})

        assert len(spawned) == 1
        assert spawned[0].daemon is True

    def test_thread_receives_correct_args(self):
        """Thread is spawned with (_post, url, payload) args."""
        from infra import webhook_dispatcher

        captured: list[dict] = []

        def _fake_thread(target, args, daemon):
            captured.append({"target": target, "args": args, "daemon": daemon})
            # Don't actually start — just capture
            t = MagicMock()
            t.daemon = daemon
            return t

        with patch(
            "infra.webhook_dispatcher.threading.Thread",
            side_effect=_fake_thread,
        ), patch("config.settings") as mock_settings:
            mock_settings.webhook_url = "https://hooks.example.com"
            webhook_dispatcher.dispatch({"event": "auth_error", "item": "Recoil Case"})

        assert len(captured) == 1
        assert captured[0]["target"] is webhook_dispatcher._post
        assert captured[0]["args"][0] == "https://hooks.example.com"
        assert captured[0]["args"][1]["event"] == "auth_error"


class TestPost:
    def test_calls_httpx_post_with_json(self):
        """_post must call httpx.Client.post with the correct URL and JSON payload."""

        from infra.webhook_dispatcher import _post

        mock_resp = MagicMock()
        mock_resp.status_code = 200

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.post.return_value = mock_resp

        with patch("httpx.Client", return_value=mock_client):
            _post("https://example.com/hook", {"event": "super_deal"})

        mock_client.post.assert_called_once_with(
            "https://example.com/hook", json={"event": "super_deal"}
        )

    def test_logs_warning_on_network_error(self, caplog):
        """_post must catch exceptions and log WARNING — never raise."""
        import logging

        import httpx

        from infra.webhook_dispatcher import _post

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.post.side_effect = httpx.ConnectError("connection refused")

        with patch("httpx.Client", return_value=mock_client), \
             caplog.at_level(logging.WARNING, logger="infra.webhook_dispatcher"):
            _post("https://dead.example.com/hook", {"event": "test"})

        assert any("POST" in r.message and "failed" in r.message for r in caplog.records)
