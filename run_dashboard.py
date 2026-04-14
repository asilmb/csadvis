"""
DEV-ONLY: Starts the Dash analytics dashboard as a standalone process.

Run in a separate terminal after starting the main stack (`python -m cli`):
    python run_dashboard.py

NOT intended to be imported — guarded by a hard RuntimeError below.
In production the dashboard is served by the FastAPI/Uvicorn process
defined in cli/main.py.
"""

import sys

if __name__ != "__main__":
    raise RuntimeError(
        "run_dashboard.py is a dev-only entry point and must not be imported. "
        "Use `python run_dashboard.py` to start the standalone dashboard."
    )

import logging
import os
from pathlib import Path

from config import settings
from src.domain.connection import SessionLocal, init_db
from ui.app import create_dash_app
from seed.data import seed_database

_log_dir = Path(os.getenv("LOG_DIR", "logs"))
_log_dir.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(str(_log_dir / "app.log"), encoding="utf-8"),
    ],
)

# Ensure DB and seed data exist (safe to call even if already initialised)
init_db()
with SessionLocal() as db:
    seed_database(db)

app = create_dash_app()

print(f"\n  Dashboard: http://localhost:{settings.dashboard_port}\n")
app.run(
    host="0.0.0.0",
    port=settings.dashboard_port,
    debug=False,
)
