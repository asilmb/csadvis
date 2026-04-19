"""
Драйвер подключения к PostgreSQL для платформы аналитики CS2.

Заменяет устаревшее соединение SQLite/WAL. URL подключения строится на основе
переменных окружения POSTGRES_* / DB_HOST (те же переменные, что и в Celery).

Публичный API:
  DATABASE_URL  — строка подключения (экспортируется для отладки)
  SessionLocal  — фабрика сессий, привязанная к engine
  get_db()      — контекст-менеджер для работы с сессией (commit/rollback)
  get_db_dep()  — генератор зависимости для FastAPI
  init_db()     — идемпотентное создание таблиц и типов данных
"""

import logging
import os
from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError, InternalError
from sqlalchemy.orm import Session, sessionmaker

logger = logging.getLogger(__name__)

# ── Параметры подключения ──────────────────────────────────────────────────────
# Используем те же переменные, что и в docker-compose / celery_app.py
_pg_user = os.getenv("POSTGRES_USER", "cs2user")
_pg_pass = os.getenv("POSTGRES_PASSWORD", "cs2pass")
_pg_host = os.getenv("DB_HOST", "db")          # Имя сервиса в Docker сети
_pg_port = os.getenv("DB_PORT", "5432")
_pg_db   = os.getenv("POSTGRES_DB", "cs2")

# Приоритет отдается прямой переменной DATABASE_URL, если она задана
DATABASE_URL: str = os.getenv(
    "DATABASE_URL",
    f"postgresql+psycopg2://{_pg_user}:{_pg_pass}@{_pg_host}:{_pg_port}/{_pg_db}",
)

# ── Создание Engine ────────────────────────────────────────────────────────────
engine = create_engine(
    DATABASE_URL,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,  # Проверка живого соединения перед выдачей из пула
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


# ── Помощники для работы с сессиями ─────────────────────────────────────────────

@contextmanager
def get_db() -> Generator[Session, None, None]:
    """Контекстный менеджер для синхронного кода."""
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def get_db_dep() -> Generator[Session, None, None]:
    """Зависимость (Dependency) для эндпоинтов FastAPI."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ── Инициализация схемы ────────────────────────────────────────────────────────

def init_db() -> None:
    """
    Создает все таблицы, объявленные в database.models.
    
    Включает защиту от ошибок 'already exists' при создании ENUM типов PostgreSQL,
    которые могут возникать при одновременном запуске нескольких контейнеров.
    """
    from src.domain.models import Base

    try:
        # checkfirst=True корректно обрабатывает таблицы, но для ENUM типов
        # в PostgreSQL может потребоваться перехват ошибок UniqueViolation.
        Base.metadata.create_all(bind=engine, checkfirst=True)
        logger.info("БД готова (PostgreSQL @ %s:%s/%s)", _pg_host, _pg_port, _pg_db)
    except (IntegrityError, InternalError) as e:
        error_msg = str(e).lower()
        if "already exists" in error_msg or "duplicate key" in error_msg:
            logger.warning("Схема или типы данных уже существуют. Пропуск инициализации.")
        else:
            logger.error("Критическая ошибка инициализации БД: %s", e)
            raise e
