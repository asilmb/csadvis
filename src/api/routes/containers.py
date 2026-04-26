"""
Container endpoints — investment signals.

GET /containers       — list all containers with investment verdicts
GET /containers/{id}  — full investment signal + price history for one container

Prices come from the local DB (Steam Market history).
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from src.api.schemas import SyncDispatchResponse
from src.domain.connection import get_db_dep
from src.domain.investment import compute_all_investment_signals
from src.domain.models import DimContainer, FactContainerPrice
from src.domain.portfolio import get_portfolio_data

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/containers", tags=["containers"])

_VERDICT_ORDER = {"BUY": 0, "LEAN BUY": 1, "HOLD": 2, "LEAN SELL": 3, "SELL": 4, "NO DATA": 5}

get_db = get_db_dep  # canonical FastAPI dependency from database/connection.py


@router.get("/", response_model=list[dict])
def list_containers(db: Session = Depends(get_db)) -> list[dict]:
    """List all containers sorted by investment verdict (BUY first)."""
    containers = db.query(DimContainer).all()
    price_data = get_portfolio_data()  # delegates to services.portfolio (bulk query, no N+1)
    signals = compute_all_investment_signals(containers, price_data)

    result = [
        {
            "container_id": c.container_id,
            "container_name": c.container_name,
            "container_type": c.container_type.value,
            "base_cost": c.base_cost,
            **signals.get(str(c.container_id), {"verdict": "NO DATA"}),
        }
        for c in containers
    ]
    result.sort(key=lambda x: _VERDICT_ORDER.get(x.get("verdict", "NO DATA"), 5))
    return result


@router.get("/{container_id}", response_model=dict)
def get_container(container_id: str, db: Session = Depends(get_db)) -> dict:
    """Full investment signal + price history for a single container."""
    c = db.query(DimContainer).filter(DimContainer.container_id == container_id).first()
    if not c:
        raise HTTPException(status_code=404, detail="Container not found")

    price_data = get_portfolio_data()  # delegates to services.portfolio (bulk query, no N+1)
    signals = compute_all_investment_signals([c], price_data)
    sig = signals.get(str(c.container_id), {"verdict": "NO DATA"})

    history = (
        db.query(FactContainerPrice)
        .filter(FactContainerPrice.container_id == container_id)
        .order_by(FactContainerPrice.timestamp.asc())
        .all()
    )

    return {
        "container_id": c.container_id,
        "container_name": c.container_name,
        "container_type": c.container_type.value,
        "base_cost": c.base_cost,
        **sig,
        "price_history": [
            {
                "timestamp": r.timestamp.isoformat(),
                "price": r.price,
                "mean_price": r.mean_price,
                "volume_7d": r.volume_7d,
            }
            for r in history
        ],
    }


@router.get("/blacklisted", response_model=list[dict])
def list_blacklisted(db: Session = Depends(get_db)) -> list[dict]:
    """Return all blacklisted containers."""
    rows = db.query(DimContainer).filter(DimContainer.is_blacklisted == 1).all()
    return [
        {
            "container_id": str(c.container_id),
            "container_name": c.container_name,
            "container_type": c.container_type.value,
        }
        for c in rows
    ]


@router.post("/{container_id}/blacklist", response_model=dict)
def blacklist_container(container_id: str, db: Session = Depends(get_db)) -> dict:
    """Set is_blacklisted=1 for a container."""
    c = db.query(DimContainer).filter(DimContainer.container_id == container_id).first()
    if not c:
        raise HTTPException(status_code=404, detail="Container not found")
    c.is_blacklisted = 1
    db.commit()
    return {"ok": True, "container_id": container_id}


@router.delete("/{container_id}/blacklist", response_model=dict)
def unblacklist_container(container_id: str, db: Session = Depends(get_db)) -> dict:
    """Set is_blacklisted=0 for a container (unblock scraping)."""
    c = db.query(DimContainer).filter(DimContainer.container_id == container_id).first()
    if not c:
        raise HTTPException(status_code=404, detail="Container not found")
    c.is_blacklisted = 0
    db.commit()
    return {"ok": True, "container_id": container_id}


@router.post("/{container_id}/sync-price", response_model=SyncDispatchResponse)
def sync_container_price(container_id: str, db: Session = Depends(get_db)) -> SyncDispatchResponse:
    """Enqueue a single-container price fetch job."""
    import asyncio
    c = db.query(DimContainer).filter(DimContainer.container_id == container_id).first()
    if not c:
        raise HTTPException(status_code=404, detail="Container not found")
    if c.is_blacklisted:
        return SyncDispatchResponse(ok=False, already_running=False, message="Container is blacklisted.")
    try:
        from infra.work_queue import enqueue
        enqueue({"type": "backfill_history", "names": [str(c.container_name)]})
        enqueue({"type": "price_poll", "container_id": container_id})
        enqueue({"type": "analyze_lifecycle", "container_ids": [container_id], "apply_prune": False})
        return SyncDispatchResponse(ok=True, already_running=False, task_id=container_id, message="History + price + lifecycle enqueued.")
    except asyncio.QueueFull:
        return SyncDispatchResponse(ok=False, already_running=True, message="Queue full — try again shortly.")
    except Exception as exc:
        logger.error("sync_container_price: enqueue failed for %s: %s", container_id, exc)
        return SyncDispatchResponse(ok=False, already_running=False, message=f"Enqueue error: {exc}")
