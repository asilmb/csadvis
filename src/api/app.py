"""FastAPI application factory (PV-08)."""

import time
from typing import Any

import structlog
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from src.api.routes import (
    auth_router,
    containers_router,
    items_router,
    metrics_router,
    positions_router,
    scrape_sessions_router,
    stats_router,
    sync_router,
    system_router,
)

logger = structlog.get_logger()

# Origins allowed to call the API.
# Includes the Dash UI (8050) and the React dev server (5173 — PV-08).
_ALLOWED_ORIGINS = [
    "http://localhost:8050",
    "http://127.0.0.1:8050",
    "http://localhost:5173",
    "http://127.0.0.1:5173",
]


def create_app(lifespan: Any = None) -> FastAPI:
    app = FastAPI(
        title="CS2 Market Analytics Platform",
        description=(
            "Container ROI, EV, and Risk Assessment Engine.\n\n"
            "### Key endpoints\n"
            "- **`/api/v1/items`** — market overview with tier filtering\n"
            "- **`/api/v1/items/{id}/history`** — full price history\n"
            "- **`/api/v1/stats`** — portfolio ROI summary\n"
        ),
        version="1.0.0",
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )

    # ── CORS ──────────────────────────────────────────────────────────────────
    app.add_middleware(
        CORSMiddleware,
        allow_origins=_ALLOWED_ORIGINS,
        allow_credentials=False,
        allow_methods=["GET", "POST", "PATCH", "DELETE"],
        allow_headers=["Content-Type"],
    )

    # ── Request timing middleware ─────────────────────────────────────────────
    @app.middleware("http")
    async def log_request_timing(request: Request, call_next):
        t0 = time.monotonic()
        response = None
        try:
            response = await call_next(request)
            return response
        finally:
            duration_ms = round((time.monotonic() - t0) * 1000)
            status_code = response.status_code if response is not None else 500
            logger.info(
                "http_request",
                method=request.method,
                path=request.url.path,
                status_code=status_code,
                duration_ms=duration_ms,
            )
            if response is not None:
                response.headers["X-Response-Time-Ms"] = str(duration_ms)

    # ── Routers ───────────────────────────────────────────────────────────────
    app.include_router(auth_router, prefix="/api/v1")
    app.include_router(containers_router, prefix="/api/v1")
    app.include_router(items_router, prefix="/api/v1")
    app.include_router(positions_router, prefix="/api/v1")
    app.include_router(stats_router, prefix="/api/v1")
    app.include_router(sync_router, prefix="/api/v1")
    app.include_router(system_router, prefix="/api/v1")
    app.include_router(scrape_sessions_router, prefix="/api/v1")
    app.include_router(metrics_router)  # /metrics — no prefix

    @app.get("/health", tags=["health"], summary="Health check")
    def health() -> dict:
        """Returns `{"status": "ok"}` when the API process is alive."""
        return {"status": "ok"}

    return app
