from src.api.routes.auth import router as auth_router
from src.api.routes.containers import router as containers_router
from src.api.routes.items import router as items_router
from src.api.routes.metrics_route import router as metrics_router
from src.api.routes.stats import router as stats_router
from src.api.routes.sync import router as sync_router
from src.api.routes.system import router as system_router

__all__ = [
    "auth_router",
    "containers_router",
    "items_router",
    "metrics_router",
    "stats_router",
    "sync_router",
    "system_router",
]
