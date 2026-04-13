"""/metrics endpoint — returns empty document (observability stack removed)."""

from fastapi import APIRouter
from fastapi.responses import Response

from infra.metrics import collect_metrics

router = APIRouter(tags=["observability"])

_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"


@router.get("/metrics", include_in_schema=False)
def metrics() -> Response:
    """Return empty metrics document (no Prometheus scraper configured)."""
    return Response(content=collect_metrics(), media_type=_CONTENT_TYPE)
