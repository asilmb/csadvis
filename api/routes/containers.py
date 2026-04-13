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

from database.connection import get_db_dep
from database.models import DimContainer, FactContainerPrice
from engine.investment import compute_all_investment_signals
from services.portfolio import get_portfolio_data

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
