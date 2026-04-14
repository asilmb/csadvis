"""
Trade Ledger & Position Manager (PV-31).

Tracks individual Steam asset purchases as OPEN positions and computes
unrealized P&L using Steam's effective fee multiplier.

P&L formula (per unit):
    net_proceeds = current_price * STEAM_NET_MULTIPLIER
    pnl_per_unit = net_proceeds - buy_price
    pnl_total    = pnl_per_unit * quantity
    roi          = pnl_per_unit / buy_price          ← ratio, not percent

STEAM_NET_MULTIPLIER = 0.869 accounts for Steam's ~13.1% effective take-rate
(15% fee on top → seller receives 1/1.15 ≈ 0.8696 of the listed price).

All P&L values are plain float — rounding happens only at the Dash display boundary.

Usage:
    from src.domain.trade_ledger import TradeService

    svc = TradeService()
    pos = svc.record_purchase(asset_id=76561..., market_hash_name="AK-47 | ...",
                              buy_price=5000.0, quantity=1)
    pnl = svc.calculate_pnl(pos.buy_price, current_price=6000.0, quantity=pos.quantity)
    summary = svc.get_portfolio_summary(price_map={"AK-47 | ...": 6000.0})
"""

from __future__ import annotations

from src.domain.abstract_repo import PositionDTO, PositionRepository
from src.domain.sql_repositories import SqlAlchemyPositionRepository

__all__ = ["STEAM_NET_MULTIPLIER", "PositionDTO", "TradeService"]

#: Effective fraction of the listed price the seller receives after Steam fees.
STEAM_NET_MULTIPLIER: float = 0.869


class TradeService:
    """
    Stateless service — safe to instantiate per-call or as a singleton.
    Each public method that touches the DB opens its own Session and commits.
    """

    # ── Pure calculations (no DB) ──────────────────────────────────────────────

    @staticmethod
    def calculate_pnl(
        buy_price: float,
        current_price: float,
        quantity: int = 1,
    ) -> float:
        """
        Compute unrealized P&L in KZT.

        Parameters
        ----------
        buy_price:      Price paid per unit (KZT).
        current_price:  Current Steam Market median price per unit (KZT).
        quantity:       Number of units held.

        Returns
        -------
        Float P&L — positive = profit, negative = loss.
        No rounding applied (display layer is responsible).
        """
        net_per_unit = current_price * STEAM_NET_MULTIPLIER - buy_price
        return net_per_unit * quantity

    @staticmethod
    def calculate_roi(buy_price: float, current_price: float) -> float:
        """
        Compute ROI as a ratio (0.05 = 5 %).

        Returns 0.0 when buy_price is zero (guard against division by zero).
        """
        if buy_price <= 0:
            return 0.0
        return (current_price * STEAM_NET_MULTIPLIER - buy_price) / buy_price

    # ── DB-backed operations ───────────────────────────────────────────────────

    def record_purchase(
        self,
        asset_id: int,
        market_hash_name: str,
        buy_price: float,
        quantity: int = 1,
    ) -> PositionDTO:
        """
        Create a new OPEN position in the ledger.

        Parameters
        ----------
        asset_id:          Steam 64-bit asset ID (BigInteger-compatible).
        market_hash_name:  Steam market item name.
        buy_price:         Price paid per unit (KZT).
        quantity:          Units purchased (default 1).
        """
        from src.domain.connection import SessionLocal

        with SessionLocal() as db:
            repo = SqlAlchemyPositionRepository(db)
            dto = repo.add_position(
                asset_id=asset_id,
                market_hash_name=market_hash_name,
                buy_price=buy_price,
                quantity=quantity,
            )
            db.commit()
            return dto

    def close_position(self, asset_id: int, sell_price: float) -> dict | None:
        """
        Mark the OPEN position for asset_id as CLOSED.

        Returns a dict with the realised P&L, or None if no OPEN position found.

        Parameters
        ----------
        asset_id:    Steam 64-bit asset ID of the item being sold.
        sell_price:  Listed sell price (KZT) — used to compute realised P&L.
        """
        from src.domain.connection import SessionLocal

        with SessionLocal() as db:
            repo = SqlAlchemyPositionRepository(db)
            dto = repo.close_position(asset_id)
            db.commit()

        if dto is None:
            return None

        realised_pnl = self.calculate_pnl(dto.buy_price, sell_price, dto.quantity)
        realised_roi = self.calculate_roi(dto.buy_price, sell_price)
        return {
            "position": dto,
            "sell_price": sell_price,
            "realised_pnl": realised_pnl,
            "realised_roi": realised_roi,
        }

    def get_portfolio_summary(
        self,
        price_map: dict[str, float] | None = None,
    ) -> dict:
        """
        Aggregate all OPEN positions into a portfolio summary.

        Parameters
        ----------
        price_map:  Optional ``{market_hash_name: current_price_kzt}`` mapping.
                    Positions without a matching price_map entry show P&L as None.

        Returns
        -------
        dict with keys:
            positions     — list[dict]: per-position detail (pnl, roi, current_price)
            open_count    — int: number of OPEN positions
            total_invested — float: sum of buy_price * quantity across open positions
            total_pnl     — float | None: total unrealized P&L (None if any price missing)
            avg_roi       — float | None: mean ROI across positions with known prices
        """
        from src.domain.connection import SessionLocal

        price_map = price_map or {}

        with SessionLocal() as db:
            repo = SqlAlchemyPositionRepository(db)
            open_positions = repo.get_open_positions()

        rows = []
        pnl_values: list[float] = []
        roi_values: list[float] = []
        total_invested = 0.0

        for pos in open_positions:
            total_invested += pos.buy_price * pos.quantity
            current_price = price_map.get(pos.market_hash_name)

            if current_price is not None:
                pnl = self.calculate_pnl(pos.buy_price, current_price, pos.quantity)
                roi = self.calculate_roi(pos.buy_price, current_price)
                pnl_values.append(pnl)
                roi_values.append(roi)
            else:
                pnl = None
                roi = None

            rows.append(
                {
                    "id": pos.id,
                    "asset_id": pos.asset_id,
                    "market_hash_name": pos.market_hash_name,
                    "buy_price": pos.buy_price,
                    "quantity": pos.quantity,
                    "opened_at": pos.opened_at,
                    "current_price": current_price,
                    "pnl": pnl,
                    "roi": roi,
                }
            )

        if not open_positions:
            total_pnl = None
        elif len(pnl_values) == len(open_positions):
            total_pnl = sum(pnl_values)
        else:
            total_pnl = None
        avg_roi = sum(roi_values) / len(roi_values) if roi_values else None

        return {
            "positions": rows,
            "open_count": len(open_positions),
            "total_invested": total_invested,
            "total_pnl": total_pnl,
            "avg_roi": avg_roi,
        }
