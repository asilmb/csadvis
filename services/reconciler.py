"""
Position Reconciler (PV-33).

Matches Steam inventory items against open DB positions via a 3-step pipeline:
  Step 1 — Direct:  asset_id exact match.
  Step 2 — Listing: market_id match (item listed on Steam Market).
  Step 3 — FIFO:    classid match, oldest OPEN position first.

For steps 2 and 3, the position's asset_id is updated in DB so future runs
resolve via Step 1.  No positions are created or closed here — that is the
trade ledger's responsibility.

Unmatched inventory items (no open position exists) and unmatched positions
(item no longer in inventory) are counted but not acted upon.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class ReconcileResult:
    matched_direct: int = 0
    matched_listing: int = 0
    matched_fifo: int = 0
    unmatched_inventory: int = 0   # items with no open position
    unmatched_positions: int = 0   # open positions with no inventory item


class PositionReconciler:
    """
    Stateless reconciler.  Session lifecycle is owned by the caller.

    Usage:
        with SessionLocal() as db:
            repo = SqlAlchemyPositionRepository(db)
            result = PositionReconciler().sync(inventory_items, repo)
            db.commit()
    """

    def sync(
        self,
        inventory_items: list[dict],
        repo,  # SqlAlchemyPositionRepository — avoid circular import
    ) -> ReconcileResult:
        """
        Run the full 3-step reconciliation pipeline.

        inventory_items: list of dicts from SteamInventoryClient.fetch().
          Required keys: asset_id (int), classid (str), market_hash_name (str).
          Optional keys: market_id (str|None).

        The repo's session is flushed after each match but NOT committed —
        caller owns the transaction.
        """
        result = ReconcileResult()

        # Index inventory by asset_id (int) for O(1) Step-1 lookup.
        inv_by_asset: dict[int, dict] = {int(it["asset_id"]): it for it in inventory_items}
        # Track which asset_ids are consumed (matched to a position).
        consumed_asset_ids: set[int] = set()

        open_positions = repo.get_open_positions()
        matched_position_ids: set[str] = set()

        # ── Step 1: Direct match ──────────────────────────────────────────────
        for pos in open_positions:
            inv_item = inv_by_asset.get(pos.asset_id)
            if inv_item is None:
                continue
            # Update classid/market_id if now known.
            classid = str(inv_item.get("classid") or "")
            market_id = inv_item.get("market_id") or None
            if classid and (pos.classid != classid or pos.market_id != market_id):
                repo.update_asset_identity(
                    pos.id,
                    new_asset_id=pos.asset_id,
                    new_classid=classid or None,
                    new_market_id=market_id,
                    is_on_market=bool(market_id),
                )
            matched_position_ids.add(pos.id)
            consumed_asset_ids.add(pos.asset_id)
            result.matched_direct += 1

        unmatched_positions = [p for p in open_positions if p.id not in matched_position_ids]

        # Build lookup: market_id → inventory item (for Step 2)
        inv_by_market: dict[str, dict] = {
            str(it["market_id"]): it
            for it in inventory_items
            if it.get("market_id") and int(it["asset_id"]) not in consumed_asset_ids
        }

        # ── Step 2: Listing match (market_id) ────────────────────────────────
        still_unmatched: list = []
        for pos in unmatched_positions:
            if pos.market_id and pos.market_id in inv_by_market:
                inv_item = inv_by_market[pos.market_id]
                new_asset_id = int(inv_item["asset_id"])
                repo.update_asset_identity(
                    pos.id,
                    new_asset_id=new_asset_id,
                    new_classid=str(inv_item.get("classid") or "") or None,
                    new_market_id=pos.market_id,
                    is_on_market=True,
                )
                matched_position_ids.add(pos.id)
                consumed_asset_ids.add(new_asset_id)
                result.matched_listing += 1
                logger.debug(
                    "reconciler: listing match — pos %s asset %d→%d via market_id=%s",
                    pos.id, pos.asset_id, new_asset_id, pos.market_id,
                )
            else:
                still_unmatched.append(pos)

        # ── Step 3: FIFO match (classid) ──────────────────────────────────────
        # Group unmatched inventory items by classid.
        inv_by_classid: dict[str, list[dict]] = {}
        for it in inventory_items:
            aid = int(it["asset_id"])
            if aid in consumed_asset_ids:
                continue
            cid = str(it.get("classid") or "")
            if not cid:
                continue
            inv_by_classid.setdefault(cid, []).append(it)

        for pos in still_unmatched:
            classid = pos.classid
            if not classid or classid not in inv_by_classid:
                result.unmatched_positions += 1
                logger.debug(
                    "reconciler: no FIFO match for pos %s (asset_id=%d classid=%s)",
                    pos.id, pos.asset_id, classid,
                )
                continue

            # Take oldest available inventory item for this classid (list order
            # from inv_by_classid is insertion order — API order, not sorted).
            # We fetch the DB-ordered FIFO list for classid and take the first
            # position slot that still needs matching.  Here pos IS the oldest
            # unmatched position (still_unmatched preserves get_open_positions order
            # reversed — but we operate one-at-a-time so we just pop the first inv item).
            candidates = inv_by_classid[classid]
            inv_item = candidates.pop(0)
            if not candidates:
                del inv_by_classid[classid]

            new_asset_id = int(inv_item["asset_id"])
            repo.update_asset_identity(
                pos.id,
                new_asset_id=new_asset_id,
                new_classid=classid,
                new_market_id=inv_item.get("market_id") or None,
                is_on_market=bool(inv_item.get("market_id")),
            )
            consumed_asset_ids.add(new_asset_id)
            result.matched_fifo += 1
            logger.debug(
                "reconciler: FIFO match — pos %s asset %d→%d classid=%s",
                pos.id, pos.asset_id, new_asset_id, classid,
            )

        # Count unmatched inventory items (no open position holds them).
        result.unmatched_inventory = sum(
            1 for it in inventory_items if int(it["asset_id"]) not in consumed_asset_ids
        )

        total = result.matched_direct + result.matched_listing + result.matched_fifo
        logger.info(
            "reconciler: sync done — direct=%d listing=%d fifo=%d "
            "unmatched_inv=%d unmatched_pos=%d",
            result.matched_direct,
            result.matched_listing,
            result.matched_fifo,
            result.unmatched_inventory,
            result.unmatched_positions,
        )
        return result
