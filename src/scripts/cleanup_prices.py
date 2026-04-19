"""
One-time script: strip accidental currency symbols from price columns.

Cleans dim_containers.base_cost_kzt and fact_container_prices columns
(price_kzt, mean_kzt, lowest_price_kzt) that may contain string values
with symbols like "$", "₸", "RUB", spaces, or commas.

Uses SQLAlchemy (PostgreSQL) — not sqlite3.
"""

import re
import sys
from pathlib import Path

# Allow running from any working directory
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.domain.connection import SessionLocal, init_db
from src.domain.models import DimContainer, FactContainerPrice


def _to_float(value) -> tuple[float | None, bool]:
    """Return (cleaned_float, was_dirty). Returns (None, False) for NULL."""
    if value is None:
        return None, False
    if isinstance(value, (int, float)):
        return float(value), False
    s = str(value)
    cleaned = re.sub(r"[^\d.]", "", s.replace(",", "."))
    if not cleaned:
        return None, False
    try:
        result = float(cleaned)
        return result, (result != value)
    except ValueError:
        return None, False


def main() -> None:
    init_db()

    with SessionLocal() as db:
        # ── dim_containers.base_cost_kzt ────────────────────────────────────
        containers = db.query(DimContainer).all()
        n1 = 0
        for row in containers:
            cleaned, dirty = _to_float(row.base_cost_kzt)
            if dirty:
                row.base_cost_kzt = cleaned
                n1 += 1
        if n1:
            db.commit()
        print(f"dim_containers: {n1} rows updated")

        # ── fact_container_prices ────────────────────────────────────────────
        prices = db.query(FactContainerPrice).all()
        n2 = 0
        for row in prices:
            dirty = False
            for col in ("price_kzt", "mean_kzt", "lowest_price_kzt"):
                cleaned, was_dirty = _to_float(getattr(row, col))
                if was_dirty:
                    setattr(row, col, cleaned)
                    dirty = True
            if dirty:
                n2 += 1
        if n2:
            db.commit()
        print(f"fact_container_prices: {n2} rows updated")

        print(f"Total rows updated: {n1 + n2}")


if __name__ == "__main__":
    main()
