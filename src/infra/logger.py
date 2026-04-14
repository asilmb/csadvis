"""
Structured logging configuration (PV-07).

Sets up structlog with two render targets:
  - Console (stdout) : ColourConsoleRenderer — human-readable, dev-friendly.
  - File             : JSONRenderer          — machine-readable, production.

Mandatory context fields surfaced in every log record:
  event, service, level, timestamp
Optional: item_id, duration_ms (added by callers via keyword arguments).

Usage
-----
Call configure_logging() once at application startup (main.py / celery_app.py).
Modules get a logger with:
    import structlog
    logger = structlog.get_logger()

Stdlib loggers (third-party libs, legacy code) are routed through the same
pipeline via ProcessorFormatter — no double-configuration needed.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import structlog


def configure_logging(log_level: str = "INFO", log_dir: Path | None = None) -> None:
    """
    Configure structlog + stdlib root logger.

    Parameters
    ----------
    log_level:
        Minimum log level string, e.g. "DEBUG", "INFO", "WARNING".
    log_dir:
        Optional directory for JSON log file.  When None only the console
        handler is attached (useful in tests).
    """
    level = getattr(logging, log_level.upper(), logging.INFO)

    # ── Processors shared between structlog and stdlib ────────────────────────
    shared_processors: list = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
    ]

    # ── structlog core configuration ──────────────────────────────────────────
    structlog.configure(
        processors=shared_processors
        + [
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # ── Handlers ──────────────────────────────────────────────────────────────
    console_formatter = structlog.stdlib.ProcessorFormatter(
        processor=structlog.dev.ConsoleRenderer(colors=True),
        foreign_pre_chain=shared_processors,
    )
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(level)

    handlers: list[logging.Handler] = [console_handler]

    if log_dir is not None:
        log_dir.mkdir(parents=True, exist_ok=True)
        json_formatter = structlog.stdlib.ProcessorFormatter(
            processor=structlog.processors.JSONRenderer(),
            foreign_pre_chain=shared_processors,
        )
        file_handler = logging.FileHandler(
            str(log_dir / "app.log"), encoding="utf-8"
        )
        file_handler.setFormatter(json_formatter)
        file_handler.setLevel(level)
        handlers.append(file_handler)

    # ── Root stdlib logger ────────────────────────────────────────────────────
    root = logging.getLogger()
    root.handlers.clear()
    for h in handlers:
        root.addHandler(h)
    root.setLevel(level)

    # Silence noisy third-party loggers
    for noisy in ("uvicorn.access", "httpx", "httpcore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
