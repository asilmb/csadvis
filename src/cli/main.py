"""
Точка входа — запускает бэкенд FastAPI + интерфейс Dash.
Исправлена ошибка регистрации сигналов в неосновном потоке.
Исправлена ошибка AttributeError: 'str' object has no attribute 'mkdir'.
"""

import signal
import sys
import os
from pathlib import Path
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import structlog
import uvicorn
from fastapi import FastAPI

from src.api.app import create_app
from config import settings
from src.domain.connection import SessionLocal, engine, init_db
from infra.logger import configure_logging
from seed.data import seed_database

LOG_PATH = Path(os.getenv("LOG_DIR", "/app/logs"))

# Настройка UTF-8 для Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# Настройка логирования
configure_logging(
    log_level=settings.log_level,
    log_dir=LOG_PATH,
)

logger = structlog.get_logger()

# ─── Обработчик сигналов ──────────────────────────────────────────────────────

def _handle_sigterm(signum: int, frame: object) -> None:
    """Завершение работы при получении SIGTERM."""
    logger.info("sigterm_received", service="main")
    engine.dispose()
    raise KeyboardInterrupt

# ─── Lifespan FastAPI ────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Управление циклом жизни приложения."""
    logger.info("db_init", service="main")
    init_db()
    with SessionLocal() as db:
        seed_database(db)

    logger.info(
        "api_ready",
        service="main",
        host=settings.api_host,
        port=settings.api_port
    )

    try:
        yield
    finally:
        engine.dispose()
        logger.info("db_pool_closed", service="main")

# ─── Запуск приложения ────────────────────────────────────────────────────────

app = create_app(lifespan=lifespan)

if __name__ == "__main__":
    # Регистрация сигналов только при прямом запуске (Main Thread)
    try:
        signal.signal(signal.SIGTERM, _handle_sigterm)
        signal.signal(signal.SIGINT, _handle_sigterm)
    except ValueError:
        # Игнорируем, если запуск не в основном потоке (например, под дебаггером)
        pass

    uvicorn.run(
        "cli.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=False,
        log_level=settings.log_level.lower(),
    )