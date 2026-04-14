"""
Entry point — CLI dispatcher.

Usage:
    python src/main.py api        Start FastAPI server (uvicorn)
    python src/main.py ui         Start Dash analytics dashboard
    python src/main.py scrapper   Run one market-sync scrape pass
    python src/main.py worker     Start Celery background worker
"""

from __future__ import annotations

import sys


def _cmd_api() -> None:
    """Start the FastAPI / uvicorn server."""
    import os
    import signal
    from collections.abc import AsyncGenerator
    from contextlib import asynccontextmanager
    from pathlib import Path

    import structlog
    import uvicorn
    from fastapi import FastAPI

    from config import settings
    from infra.logger import configure_logging
    from seed.data import seed_database
    from src.api.app import create_app
    from src.domain.connection import SessionLocal, engine, init_db

    configure_logging(
        log_level=settings.log_level,
        log_dir=Path(os.getenv("LOG_DIR", "/app/logs")),
    )
    logger = structlog.get_logger()

    def _handle_sigterm(signum: int, frame: object) -> None:
        logger.info("sigterm_received", service="api")
        engine.dispose()
        raise KeyboardInterrupt

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        logger.info("db_init", service="api")
        init_db()
        with SessionLocal() as db:
            seed_database(db)
        logger.info("api_ready", service="api", host=settings.api_host, port=settings.api_port)
        try:
            yield
        finally:
            engine.dispose()
            logger.info("db_pool_closed", service="api")

    try:
        signal.signal(signal.SIGTERM, _handle_sigterm)
        signal.signal(signal.SIGINT, _handle_sigterm)
    except ValueError:
        pass

    app = create_app(lifespan=lifespan)
    uvicorn.run(
        app,
        host=settings.api_host,
        port=settings.api_port,
        reload=False,
        log_level=settings.log_level.lower(),
    )


def _cmd_ui() -> None:
    """Start the Dash analytics dashboard."""
    import logging
    import os
    import sys
    from pathlib import Path

    from config import settings
    from seed.data import seed_database
    from src.domain.connection import SessionLocal, init_db
    from src.ui.app import create_dash_app

    log_dir = Path(os.getenv("LOG_DIR", "logs"))
    log_dir.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(str(log_dir / "app.log"), encoding="utf-8"),
        ],
    )

    init_db()
    with SessionLocal() as db:
        seed_database(db)

    app = create_dash_app()
    print(f"\n  Dashboard: http://localhost:{settings.dashboard_port}\n")
    app.run(host="0.0.0.0", port=settings.dashboard_port, debug=False)


def _cmd_scrapper() -> None:
    """Run one full market-sync scrape pass (blocking)."""
    import asyncio
    import logging

    from src.domain.connection import SessionLocal, init_db
    from src.scrapper.db_writer import write_new_containers
    from src.scrapper.state import mark_done
    from src.scrapper.steam_market_scraper import scrape_all_containers

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    )
    logger = logging.getLogger("scrapper")

    init_db()

    logger.info("scrapper: starting market sync")
    containers = asyncio.run(scrape_all_containers())

    if not containers:
        logger.warning("scrapper: no containers returned")
        return

    with SessionLocal() as db:
        inserted = write_new_containers(db, containers)

    mark_done()
    logger.info("scrapper: done — scraped=%d inserted=%d", len(containers), inserted)


def _cmd_worker() -> None:
    """Start the Celery background worker."""
    from scheduler.celery_app import app as celery_app

    celery_app.worker_main(
        argv=["worker", "--loglevel=info", "--concurrency=2", "-Q", "celery"]
    )


# ─── Dispatch ────────────────────────────────────────────────────────────────

_COMMANDS: dict[str, tuple[str, object]] = {
    "api":      ("Start FastAPI server",               _cmd_api),
    "ui":       ("Start Dash analytics dashboard",     _cmd_ui),
    "scrapper": ("Run one market-sync scrape pass",    _cmd_scrapper),
    "worker":   ("Start Celery background worker",     _cmd_worker),
}

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in _COMMANDS:
        print("Usage: python src/main.py <command>\n")
        print("Commands:")
        for name, (desc, _) in _COMMANDS.items():
            print(f"  {name:<12}{desc}")
        sys.exit(1)

    _, fn = _COMMANDS[sys.argv[1]]
    fn()  # type: ignore[operator]
