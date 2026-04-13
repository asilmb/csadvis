"""
Setup commands: cmd_init, cmd_cookie, cmd_clean.
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent  # cli/setup.py → cs2/


def cmd_init(args) -> None:
    """
    First-run setup: validate .env → create DB → seed → test Steam cookie.
    """
    import httpx

    from config import settings

    ok = True

    # ── .env presence ────────────────────────────────────────────────────────
    env_path = _PROJECT_ROOT / ".env"
    _s = "OK" if env_path.exists() else "MISSING — copy .env.example to .env and fill credentials"
    print(f"[INIT] .env ............ {_s}")
    if not env_path.exists():
        ok = False

    # ── Required vars ────────────────────────────────────────────────────────
    _s = "OK (set)" if settings.steam_login_secure else "NOT SET — add STEAM_LOGIN_SECURE= to .env"
    print(f"[INIT] STEAM_LOGIN_SECURE {_s}")
    if not settings.steam_login_secure:
        ok = False

    _s = f"OK ({settings.steam_id})" if settings.steam_id else "NOT SET — add STEAM_ID= to .env"
    print(f"[INIT] STEAM_ID ........ {_s}")
    if not settings.steam_id:
        ok = False

    if not ok:
        print("[INIT] Fix the above issues and re-run: cs2 init")
        sys.exit(1)

    # ── DB init + seed ────────────────────────────────────────────────────────
    from domain.connection import SessionLocal, init_db
    from seed.data import seed_database

    print("[INIT] init_db ......... ", end="", flush=True)
    init_db()
    print("OK")

    from sqlalchemy import text as _text

    with SessionLocal() as db:
        db.execute(_text("DELETE FROM worker_registry"))
        db.commit()
    print("[init] WorkerRegistry cleared")

    print("[INIT] seed_database ... ", end="", flush=True)
    with SessionLocal() as db:
        seed_database(db)
    print("OK")

    # ── Steam cookie validation ───────────────────────────────────────────────
    print("[INIT] Steam cookie .... ", end="", flush=True)
    try:
        resp = httpx.get(
            "https://steamcommunity.com/market/priceoverview/",
            params={"appid": 730, "currency": 37, "market_hash_name": "Revolution Case"},
            headers={
                "Cookie": f"steamLoginSecure={settings.steam_login_secure}",
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                ),
            },
            timeout=10.0,
            follow_redirects=True,
        )
        if resp.status_code == 200 and resp.json().get("success"):
            print("OK (valid)")
        elif resp.status_code in (401, 403):
            print("EXPIRED — refresh with: cs2 cookie")
        else:
            print(f"WARNING — HTTP {resp.status_code} (network issue?)")
    except Exception as exc:
        print(f"ERROR — {exc}")

    print("[INIT] Done. Run: cs2 up")


def cmd_cookie(args) -> None:
    """Save Steam login cookie to .env (auto-reads from Chrome or guides manually)."""
    import webbrowser

    env_path = _PROJECT_ROOT / ".env"

    def _save_to_env(cookie_value: str) -> None:
        """Write or update STEAM_LOGIN_SECURE= line in .env."""
        if env_path.exists():
            lines = env_path.read_text(encoding="utf-8").splitlines()
            updated = False
            for i, line in enumerate(lines):
                if line.startswith("STEAM_LOGIN_SECURE="):
                    lines[i] = f"STEAM_LOGIN_SECURE={cookie_value}"
                    updated = True
                    break
            if not updated:
                lines.append(f"STEAM_LOGIN_SECURE={cookie_value}")
            env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        else:
            env_path.write_text(f"STEAM_LOGIN_SECURE={cookie_value}\n", encoding="utf-8")

    # ── Try auto-read from Chrome (only works when Chrome is closed) ──────────
    print("\n  Trying to read cookie from Chrome automatically...")
    try:
        from scrapper.steam_wallet import read_cookie_from_chrome

        cookie = read_cookie_from_chrome()
        if cookie:
            _save_to_env(cookie)
            print(f"  Done! steamLoginSecure saved to .env  (length: {len(cookie)})")
            print("  Restart cs2 dashboard and cs2 start to apply.\n")
            return
        print("  Auto-read failed (Chrome is open or no Steam session found).")
    except Exception as exc:
        print(f"  Auto-read failed: {exc}")

    # ── Fallback: guided manual extraction ────────────────────────────────────
    url = "https://steamcommunity.com/market/"
    print(f"\n  Opening {url} in your browser...")
    webbrowser.open(url)

    print("""
  3 steps to copy the cookie:

  1. Press F12  (DevTools opens)
  2. Go to:  Application -> Cookies -> https://steamcommunity.com
  3. Click on  steamLoginSecure  -> double-click the Value column -> Ctrl+A -> Ctrl+C
""")

    try:
        value = input("  Paste steamLoginSecure here and press Enter: ").strip()
    except (KeyboardInterrupt, EOFError):
        print("\n  Aborted.\n")
        return

    if not value:
        print("  Nothing entered. Aborted.\n")
        return

    _save_to_env(value)
    print(f"\n  Saved to .env  (length: {len(value)})")
    print("  Restart cs2 dashboard and cs2 start to apply.\n")


def cmd_clean(args) -> None:
    """
    Project hygiene: remove build artefacts, stale logs, temp DBs, and phantom workers.

    Deleted:
      * __pycache__ directories (recursively, skipping .venv / venv)
      * .pytest_cache / .ruff_cache / .mypy_cache
      * logs/*.log  (logs/.gitkeep is preserved)
      * *.db-wal / *.db-shm (SQLite WAL side-cars)
      * test_*.db   (left over by pytest tmp_path leaks)

    Never deleted:
      * .env  (live credentials)
      * config.py
      * Any *.db file whose name does not start with "test_"

    DB-side:
      * WorkerRegistry rows are cleared (phantom workers from past sessions).
    """
    import shutil

    root = _PROJECT_ROOT
    removed_dirs: list[str] = []
    removed_files: list[str] = []
    skip_dirs = {".venv", "venv", ".git", "node_modules"}

    # ── Cache directories ─────────────────────────────────────────────────────
    for pattern in ("__pycache__", ".pytest_cache", ".ruff_cache", ".mypy_cache"):
        for p in root.rglob(pattern):
            # Skip virtual-env trees
            if any(s in p.parts for s in skip_dirs):
                continue
            if p.is_dir():
                shutil.rmtree(p, ignore_errors=True)
                removed_dirs.append(str(p.relative_to(root)))

    # ── Log files ─────────────────────────────────────────────────────────────
    for logs_dir in (root / "storage" / "logs", root / "logs"):
        if logs_dir.is_dir():
            for f in logs_dir.glob("*.log"):
                f.unlink(missing_ok=True)
                removed_files.append(str(f.relative_to(root)))

    # ── SQLite WAL side-cars ──────────────────────────────────────────────────
    for pattern in ("*.db-wal", "*.db-shm"):
        for f in root.rglob(pattern):
            if any(s in f.parts for s in skip_dirs):
                continue
            f.unlink(missing_ok=True)
            removed_files.append(str(f.relative_to(root)))

    # ── Stray test databases ──────────────────────────────────────────────────
    for f in root.rglob("test_*.db"):
        if any(s in f.parts for s in skip_dirs):
            continue
        f.unlink(missing_ok=True)
        removed_files.append(str(f.relative_to(root)))

    # ── WorkerRegistry reset ──────────────────────────────────────────────────
    workers_reset = 0
    try:
        from domain.connection import SessionLocal, init_db

        init_db()
        from domain.models import WorkerRegistry

        with SessionLocal() as db:
            workers_reset = db.query(WorkerRegistry).delete(synchronize_session=False)
            db.commit()
    except Exception as exc:
        logger.warning("clean: could not reset WorkerRegistry: %s", exc)

    # ── Report ────────────────────────────────────────────────────────────────
    print(f"[CLEAN] Cache dirs removed:   {len(removed_dirs)}")
    print(f"[CLEAN] Files removed:        {len(removed_files)}")
    print(f"[CLEAN] WorkerRegistry rows:  {workers_reset} cleared")
    if removed_dirs:
        for d in removed_dirs[:10]:
            print(f"         dir  {d}")
        if len(removed_dirs) > 10:
            print(f"         ... and {len(removed_dirs) - 10} more")
    if removed_files:
        for f in removed_files[:10]:
            print(f"         file {f}")
        if len(removed_files) > 10:
            print(f"         ... and {len(removed_files) - 10} more")
    print("[CLEAN] Done.")
