"""Integration tests for POST /api/v1/auth/steam endpoint."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client():
    from src.api.app import create_app
    app = create_app()
    return TestClient(app, raise_server_exceptions=False)


# ─── POST /api/v1/auth/steam ──────────────────────────────────────────────────


class TestSetSteamAuth:
    def test_success(self, client):
        mock_mgr = MagicMock()
        with patch("infra.steam_credentials._get_manager", return_value=mock_mgr), \
             patch("infra.work_queue.signal_auth_ready") as mock_signal:
            resp = client.post(
                "/api/v1/auth/steam",
                json={"steamLoginSecure": "valid_token", "session_id": "valid_sess"},
            )
        assert resp.status_code == 200
        assert resp.json()["ok"] is True
        mock_mgr.set_credentials.assert_called_once_with("valid_token", "valid_sess")
        mock_signal.assert_called_once()

    def test_empty_login_secure_returns_422(self, client):
        resp = client.post(
            "/api/v1/auth/steam",
            json={"steamLoginSecure": "", "session_id": "valid_sess"},
        )
        assert resp.status_code == 422

    def test_empty_session_id_returns_422(self, client):
        resp = client.post(
            "/api/v1/auth/steam",
            json={"steamLoginSecure": "valid_token", "session_id": "   "},
        )
        assert resp.status_code == 422

    def test_missing_fields_returns_422(self, client):
        resp = client.post("/api/v1/auth/steam", json={})
        assert resp.status_code == 422

    def test_credential_storage_error_returns_500(self, client):
        mock_mgr = MagicMock()
        mock_mgr.set_credentials.side_effect = Exception("Redis connection refused")
        with patch("infra.steam_credentials._get_manager", return_value=mock_mgr):
            resp = client.post(
                "/api/v1/auth/steam",
                json={"steamLoginSecure": "token", "session_id": "sess"},
            )
        assert resp.status_code == 500
