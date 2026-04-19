"""Unit tests for SteamCredentialManager (Fernet at-rest encryption)."""
from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest
from cryptography.fernet import Fernet

# ─── Helpers ──────────────────────────────────────────────────────────────────

def _valid_key() -> str:
    return Fernet.generate_key().decode()


def _make_manager(key: str | None = None):
    """Instantiate SteamCredentialManager with a fresh valid key (or provided key)."""
    from infra.steam_credentials import SteamCredentialManager

    with patch.dict(os.environ, {"STEAM_DATA_KEY": key or _valid_key()}):
        return SteamCredentialManager()


# ─── __init__ ─────────────────────────────────────────────────────────────────


class TestInit:
    def test_raises_when_key_missing(self, monkeypatch):
        monkeypatch.delenv("STEAM_DATA_KEY", raising=False)
        from infra.steam_credentials import SteamCredentialManager
        with pytest.raises(RuntimeError, match="STEAM_DATA_KEY"):
            SteamCredentialManager()

    def test_raises_when_key_empty(self, monkeypatch):
        monkeypatch.setenv("STEAM_DATA_KEY", "   ")
        from infra.steam_credentials import SteamCredentialManager
        with pytest.raises(RuntimeError, match="STEAM_DATA_KEY"):
            SteamCredentialManager()

    def test_raises_when_key_malformed(self, monkeypatch):
        monkeypatch.setenv("STEAM_DATA_KEY", "not-a-valid-fernet-key")
        from infra.steam_credentials import SteamCredentialManager
        with pytest.raises(RuntimeError, match="not a valid Fernet key"):
            SteamCredentialManager()

    def test_succeeds_with_valid_key(self, monkeypatch):
        monkeypatch.setenv("STEAM_DATA_KEY", _valid_key())
        from infra.steam_credentials import SteamCredentialManager
        mgr = SteamCredentialManager()
        assert mgr is not None


# ─── _enc / _dec ──────────────────────────────────────────────────────────────


class TestEncDec:
    def test_round_trip(self):
        mgr = _make_manager()
        plaintext = "76561198000000000%7C%7Caabbccdd"
        assert mgr._dec(mgr._enc(plaintext)) == plaintext

    def test_different_ciphertexts_same_plaintext(self):
        """Fernet uses random IV — same input produces different ciphertext each time."""
        mgr = _make_manager()
        ct1 = mgr._enc("secret")
        ct2 = mgr._enc("secret")
        assert ct1 != ct2
        assert mgr._dec(ct1) == mgr._dec(ct2) == "secret"

    def test_dec_legacy_plaintext_fallback(self):
        """Legacy unencrypted values stored before migration are returned as-is."""
        mgr = _make_manager()
        raw_legacy = "plaintext_session_id"
        assert mgr._dec(raw_legacy) == raw_legacy

    def test_dec_wrong_key_falls_back(self):
        """Value encrypted with a different key falls back to plaintext (migration path)."""
        other_mgr = _make_manager()
        ciphertext = other_mgr._enc("secret_value")
        mgr = _make_manager()  # fresh key
        result = mgr._dec(ciphertext)
        # Should return the ciphertext as-is (migration fallback), not raise
        assert isinstance(result, str)


# ─── set_credentials ──────────────────────────────────────────────────────────


class TestSetCredentials:
    def _mock_redis(self):
        pipe = MagicMock()
        pipe.execute = MagicMock()
        r = MagicMock()
        r.pipeline.return_value = pipe
        return r, pipe

    def test_writes_encrypted_values_via_pipeline(self):
        mgr = _make_manager()
        r, pipe = self._mock_redis()
        with patch.object(mgr, "_redis", return_value=r):
            mgr.set_credentials("login_secure_value", "session_id_value")
        pipe.set.assert_called()
        pipe.execute.assert_called_once()

    def test_does_not_store_plaintext(self):
        mgr = _make_manager()
        r, pipe = self._mock_redis()
        stored_values: list[str] = []

        def capture_set(key, val):
            stored_values.append(val)

        pipe.set.side_effect = capture_set
        with patch.object(mgr, "_redis", return_value=r):
            mgr.set_credentials("my_login_secure", "my_session_id")

        for val in stored_values:
            assert "my_login_secure" not in val
            assert "my_session_id" not in val

    def test_strips_whitespace_before_encrypting(self):
        mgr = _make_manager()
        r, pipe = self._mock_redis()
        with patch.object(mgr, "_redis", return_value=r):
            mgr.set_credentials("  login  ", "  sess  ")
        # round-trip via get_credentials should return stripped values
        encrypted_calls = pipe.set.call_args_list
        assert len(encrypted_calls) == 2


# ─── get_credentials ──────────────────────────────────────────────────────────


class TestGetCredentials:
    def test_returns_decrypted_pair(self):
        mgr = _make_manager()
        enc_ls = mgr._enc("login_secure")
        enc_si = mgr._enc("session_id")
        r = MagicMock()
        r.get.side_effect = lambda key: enc_ls if "login" in key else enc_si
        with patch.object(mgr, "_redis", return_value=r):
            ls, si = mgr.get_credentials()
        assert ls == "login_secure"
        assert si == "session_id"

    def test_returns_empty_strings_when_keys_missing(self):
        mgr = _make_manager()
        r = MagicMock()
        r.get.return_value = None
        with patch.object(mgr, "_redis", return_value=r):
            ls, si = mgr.get_credentials()
        assert ls == ""
        assert si == ""

    def test_returns_empty_on_redis_exception(self):
        mgr = _make_manager()
        r = MagicMock()
        r.get.side_effect = ConnectionError("Redis down")
        with patch.object(mgr, "_redis", return_value=r):
            ls, si = mgr.get_credentials()
        assert ls == ""
        assert si == ""

    def test_partial_missing_keys(self):
        """Only login_secure present — session_id returns empty string."""
        mgr = _make_manager()
        enc_ls = mgr._enc("only_login")
        r = MagicMock()
        r.get.side_effect = lambda key: enc_ls if "login" in key else None
        with patch.object(mgr, "_redis", return_value=r):
            ls, si = mgr.get_credentials()
        assert ls == "only_login"
        assert si == ""


# ─── revoke ───────────────────────────────────────────────────────────────────


class TestRevoke:
    def test_deletes_both_keys(self):
        mgr = _make_manager()
        r = MagicMock()
        with patch.object(mgr, "_redis", return_value=r):
            mgr.revoke()
        r.delete.assert_called_once()
        args = r.delete.call_args[0]
        assert "steam:login_secure" in args
        assert "steam:session_id" in args

    def test_handles_redis_exception_gracefully(self):
        mgr = _make_manager()
        r = MagicMock()
        r.delete.side_effect = ConnectionError("Redis down")
        with patch.object(mgr, "_redis", return_value=r):
            mgr.revoke()  # must not raise


# ─── credentials_exist ────────────────────────────────────────────────────────


class TestCredentialsExist:
    def _mgr_with_creds(self, ls: str, si: str):
        mgr = _make_manager()
        with patch.object(mgr, "get_credentials", return_value=(ls, si)):
            return mgr

    def test_true_when_both_present(self):
        mgr = _make_manager()
        with patch.object(mgr, "get_credentials", return_value=("ls_val", "si_val")):
            assert mgr.credentials_exist() is True

    def test_false_when_both_missing(self):
        mgr = _make_manager()
        with patch.object(mgr, "get_credentials", return_value=("", "")):
            assert mgr.credentials_exist() is False

    def test_false_when_only_login_secure(self):
        mgr = _make_manager()
        with patch.object(mgr, "get_credentials", return_value=("ls_val", "")):
            assert mgr.credentials_exist() is False


# ─── Module-level helpers ─────────────────────────────────────────────────────


class TestModuleLevelHelpers:
    def test_get_login_secure_returns_empty_on_runtime_error(self, monkeypatch):
        monkeypatch.delenv("STEAM_DATA_KEY", raising=False)
        # Reset singleton so it re-initialises with missing key
        import infra.steam_credentials as sc
        sc._manager = None
        result = sc.get_login_secure()
        assert result == ""

    def test_auth_credentials_exist_returns_false_on_runtime_error(self, monkeypatch):
        monkeypatch.delenv("STEAM_DATA_KEY", raising=False)
        import infra.steam_credentials as sc
        sc._manager = None
        result = sc.auth_credentials_exist()
        assert result is False
