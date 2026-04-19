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
    from infra.steam_credentials import get_login_secure
    _s = "OK (set)" if get_login_secure() else "NOT SET — enter it via the dashboard cookie form"
    print(f"[INIT] STEAM_LOGIN_SECURE {_s}")
    if not get_login_secure():
        ok = False

    _s = f"OK ({settings.steam_id})" if settings.steam_id else "NOT SET — add STEAM_ID= to .env"
    print(f"[INIT] STEAM_ID ........ {_s}")
    if not settings.steam_id:
        ok = False

    if not ok:
        print("[INIT] Fix the above issues and re-run: cs2 init")
        sys.exit(1)

    # ── DB init + seed ────────────────────────────────────────────────────────
    from seed.data import seed_database
    from src.domain.connection import SessionLocal, init_db

    print("[INIT] init_db ......... ", end="", flush=True)
    init_db()
    print("OK")

    print("[INIT] seed_database ... ", end="", flush=True)
    with SessionLocal() as db:
        seed_database(db)
    print("OK")

    # ── Steam cookie validation ───────────────────────────────────────────────
    print("[INIT] Steam cookie .... ", end="", flush=True)
    try:
        from infra.steam_credentials import get_login_secure as _get_cookie
        resp = httpx.get(
            "https://steamcommunity.com/market/priceoverview/",
            params={"appid": 730, "currency": 37, "market_hash_name": "Revolution Case"},
            headers={
                "Cookie": f"steamLoginSecure={_get_cookie()}",
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
    """Save Steam login cookie to Redis (auto-reads from Chrome or guides manually)."""
    import webbrowser

    from infra.steam_credentials import set_login_secure

    # ── Try auto-read from Chrome (only works when Chrome is closed) ──────────
    print("\n  Trying to read cookie from Chrome automatically...")
    try:
        from scrapper.steam_wallet import read_cookie_from_chrome

        cookie = read_cookie_from_chrome()
        if cookie:
            set_login_secure(cookie)
            print(f"  Done! steamLoginSecure saved to Redis  (length: {len(cookie)})")
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

    set_login_secure(value)
    print(f"\n  Saved to Redis  (length: {len(value)})\n")


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

    # ── Report ────────────────────────────────────────────────────────────────
    print(f"[CLEAN] Cache dirs removed:   {len(removed_dirs)}")
    print(f"[CLEAN] Files removed:        {len(removed_files)}")
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
