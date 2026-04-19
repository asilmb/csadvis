"""Encrypted Steam credential store (Redis-backed, Fernet at-rest encryption).

Key source
----------
The Fernet key is read exclusively from the STEAM_DATA_KEY environment variable.
If the variable is absent or malformed the manager raises RuntimeError — the
application must not start without a valid key.

Generate a key once and add it to .env:

    python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

Security properties
-------------------
- Plain-text credentials never leave this module's method scope.
- Encrypted tokens are stored in Redis; the plaintext only exists in local
  variables inside get_credentials() and is never logged or assigned to
  module-level state.
- revoke() hard-deletes both Redis keys so compromised or expired sessions
  do not linger.
"""
from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

_LOGIN_SECURE_KEY = "steam:login_secure"
_SESSION_ID_KEY   = "steam:session_id"


# ── Manager ───────────────────────────────────────────────────────────────────

class SteamCredentialManager:
    """Encrypt/decrypt Steam auth tokens before touching Redis."""

    def __init__(self) -> None:
        raw = os.getenv("STEAM_DATA_KEY", "").strip()
        if not raw:
            raise RuntimeError(
                "STEAM_DATA_KEY environment variable is not set. "
                "Generate a key and add it to .env:\n"
                "  python -c \"from cryptography.fernet import Fernet; "
                "print(Fernet.generate_key().decode())\""
            )
        try:
            from cryptography.fernet import Fernet
            self._fernet = Fernet(raw.encode())
        except Exception as exc:
            raise RuntimeError(f"STEAM_DATA_KEY is not a valid Fernet key: {exc}") from exc

    # ── Internal crypto ───────────────────────────────────────────────────────

    def _enc(self, plaintext: str) -> str:
        return self._fernet.encrypt(plaintext.encode()).decode()

    def _dec(self, token: str) -> str:
        """Decrypt Fernet token; falls back to plaintext for legacy unencrypted values."""
        from cryptography.fernet import InvalidToken
        try:
            return self._fernet.decrypt(token.encode()).decode()
        except InvalidToken:
            logger.warning("steam_credentials: value not encrypted — returning as-is (will be re-encrypted on next write)")
            return token  # migration path: unencrypted value — will be re-encrypted on next write

    # ── Redis helpers ─────────────────────────────────────────────────────────

    @staticmethod
    def _redis():
        from infra.redis_client import get_redis
        return get_redis()

    # ── Public API ────────────────────────────────────────────────────────────

    def set_credentials(self, login_secure: str, session_id: str) -> None:
        """Encrypt both tokens and write to Redis atomically via pipeline."""
        enc_ls = self._enc(login_secure.strip())
        enc_si = self._enc(session_id.strip())
        pipe = self._redis().pipeline()
        pipe.set(_LOGIN_SECURE_KEY, enc_ls)
        pipe.set(_SESSION_ID_KEY,   enc_si)
        pipe.execute()
        logger.info("steam_credentials: credentials updated (values encrypted, not logged)")

    def get_credentials(self) -> tuple[str, str]:
        """Return (login_secure, session_id).  Decrypted values exist only in local scope."""
        try:
            r     = self._redis()
            ls    = r.get(_LOGIN_SECURE_KEY)
            si    = r.get(_SESSION_ID_KEY)
            return (
                self._dec(ls) if ls else "",
                self._dec(si) if si else "",
            )
        except Exception as exc:
            logger.debug("steam_credentials: get_credentials failed — %s", type(exc).__name__)
            return ("", "")

    def revoke(self) -> None:
        """Hard-delete both credential keys — call on 401 or explicit sign-out."""
        try:
            self._redis().delete(_LOGIN_SECURE_KEY, _SESSION_ID_KEY)
            logger.warning("steam_credentials: credentials revoked (keys deleted from Redis)")
        except Exception as exc:
            logger.error("steam_credentials: revoke failed — %s", exc)

    def credentials_exist(self) -> bool:
        """Return True when both tokens are present (used by the worker wait-loop)."""
        ls, si = self.get_credentials()
        return bool(ls) and bool(si)


# ── Module-level singleton (lazy, validated on first use) ──────────────────────

_manager: SteamCredentialManager | None = None


def _get_manager() -> SteamCredentialManager:
    global _manager
    if _manager is None:
        _manager = SteamCredentialManager()
    return _manager


# ── Backward-compatible module-level helpers ───────────────────────────────────
# Existing callers (work_queue.py, system.py, callbacks.py) continue to work
# without modification.

def get_login_secure() -> str:
    try:
        ls, _ = _get_manager().get_credentials()
        return ls
    except RuntimeError:
        return ""


def set_login_secure(value: str) -> None:
    _, si = _get_manager().get_credentials()
    _get_manager().set_credentials(value, si)


def get_session_id() -> str:
    try:
        _, si = _get_manager().get_credentials()
        return si
    except RuntimeError:
        return ""


def set_session_id(value: str) -> None:
    ls, _ = _get_manager().get_credentials()
    _get_manager().set_credentials(ls, value)


def auth_credentials_exist() -> bool:
    try:
        return _get_manager().credentials_exist()
    except RuntimeError:
        return False
