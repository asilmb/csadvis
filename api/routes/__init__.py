from .auth import router as auth_router
from .containers import router as containers_router
from .items import router as items_router
from .metrics_route import router as metrics_router
from .stats import router as stats_router
from .sync import router as sync_router
from .system import router as system_router

__all__ = [
    "auth_router",
    "containers_router",
    "items_router",
    "metrics_router",
    "stats_router",
    "sync_router",
    "system_router",
]
