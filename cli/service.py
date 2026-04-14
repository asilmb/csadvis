"""
Логика запуска сервисов.
"""
import os
import threading
import logging
from pathlib import Path

from infra.logger import configure_logging

logger = logging.getLogger(__name__)

# ФИКС: Обязательно Path объект для логов
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOG_PATH = Path(os.getenv("LOG_DIR", str(_PROJECT_ROOT / "storage" / "logs")))

def cmd_start(args):
    """Запуск API и Dashboard."""
    import uvicorn
    from src.domain.connection import init_db
    from ui.app import create_dash_app
    
    configure_logging(log_level="INFO", log_dir=LOG_PATH)
    init_db()
    
    dash = create_dash_app()
    
    # API в фоне
    config = uvicorn.Config("cli.main:app", host="0.0.0.0", port=8000, log_level="info")
    server = uvicorn.Server(config)
    threading.Thread(target=server.run, daemon=True).start()
    
    # Dash (блокирует)
    dash.run(host="0.0.0.0", port=8050, debug=False)

def cmd_worker(args):
    """Запуск Celery."""
    from scheduler.celery_app import app as celery_app

    configure_logging(log_level="INFO", log_dir=LOG_PATH)
    worker_count = getattr(args, "workers", 2)

    # Celery процесс
    use_beat = getattr(args, "beat", False)
    celery_argv = ["worker", f"--concurrency={worker_count}", "-E"]
    if use_beat:
        celery_argv += ["--beat"]
    celery_app.worker_main(argv=celery_argv)

def cmd_dashboard(args):
    """Только Dashboard."""
    from ui.app import create_dash_app
    configure_logging(log_level="INFO", log_dir=LOG_PATH)
    app = create_dash_app()
    app.run(host="0.0.0.0", port=8050, debug=False)