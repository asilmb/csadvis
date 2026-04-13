"""
Writes scraped container data into the database.

Only inserts containers that do not already exist (by name).
Item-level data is not stored — the investment model only tracks container prices.

Steam Market listing policy for capsules:
  - Group capsules ARE listed: "Event Challengers/Legends/Contenders/Champions Autograph/Sticker Capsule"
  - Individual team capsules are NOT listed: "Event TeamName Autograph Capsule"
  We filter out individual team capsules to avoid 400 errors on backfill.
"""

from __future__ import annotations

import logging
import uuid

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from config import settings
from domain.models import ContainerType, DimContainer
from scrapper.steam_market_scraper import ScrapedContainer

logger = logging.getLogger(__name__)

# Group subdivision keywords that appear in Steam-listable event capsule names.
# Steam only lists capsules at the group level (Challengers/Legends/Contenders/Champions),
# NOT at the individual team level (e.g. "NaVi Autograph Capsule").
# The word "autograph" or "sticker" alone is NOT sufficient — it appears in
# every capsule name including individual team capsules.
_STEAM_CAPSULE_GROUP_WORDS = frozenset({"challengers", "legends", "contenders", "champions"})


def _is_steam_listed(sc: ScrapedContainer) -> bool:
    """Return True if this container is expected to have a Steam Market listing.

    Individual team capsules (e.g. "Copenhagen 2024 NaVi Autograph Capsule") are
    sold in-game packs but are NOT individually listed on Steam Community Market.
    Only group capsules (Challengers / Legends / Contenders / Champions) are listed.
    """
    ctype = sc.container_type
    # Weapon cases, souvenir packages, and terminals are always listed.
    if ctype not in ("Autograph Capsule", "Event Capsule", "Sticker Capsule"):
        return True

    # Event-linked capsule types (Autograph Capsule, Event Capsule, Sticker Capsule):
    # must contain a group subdivision keyword to be listed on Steam Market.
    name_lower = sc.name.lower()
    return any(kw in name_lower for kw in _STEAM_CAPSULE_GROUP_WORDS)


_CTYPE_ENUM: dict[str, ContainerType] = {
    "Weapon Case": ContainerType.Weapon_Case,
    "Souvenir Package": ContainerType.Souvenir_Package,
    "Sealed Terminal": ContainerType.Sealed_Terminal,
    "Sticker Capsule": ContainerType.Sticker_Capsule,
    "Autograph Capsule": ContainerType.Autograph_Capsule,
    "Event Capsule": ContainerType.Event_Capsule,
}

_DEFAULT_COST: dict[str, float] = {
    # Default base costs (~481₸/$ reference)
    "Weapon Case": 1445,
    "Souvenir Package": 1200,
    "Sealed Terminal": 1200,
    "Sticker Capsule": 480,
    "Autograph Capsule": 960,
    "Event Capsule": 480,
}


def write_new_containers(db: Session, containers: list[ScrapedContainer]) -> int:
    """
    Insert scraped containers that don't already exist in the DB.
    Returns the count of newly inserted containers.
    """
    existing = {str(c.container_name) for c in db.query(DimContainer).all()}
    inserted = 0

    for sc in containers:
        if sc.name in existing:
            continue

        if not _is_steam_listed(sc):
            logger.debug("Scraper DB: skipping individual team capsule (not on SCM): %s", sc.name)
            continue

        ctype_enum = _CTYPE_ENUM.get(sc.container_type)
        if not ctype_enum:
            logger.warning(
                "Scraper DB: unknown container type %r for %s", sc.container_type, sc.name
            )
            continue

        cost = _DEFAULT_COST.get(sc.container_type, 1200)

        try:
            db.add(
                DimContainer(
                    container_id=str(uuid.uuid4()),
                    container_name=sc.name,
                    container_type=ctype_enum,
                    base_cost=cost,
                )
            )
            db.flush()
            db.commit()
            existing.add(sc.name)
            inserted += 1
            logger.info("Scraper DB: inserted container %s (cost %.0f%s)", sc.name, cost, settings.currency_symbol)
        except IntegrityError:
            db.rollback()
            existing.add(sc.name)  # keep cache consistent
            logger.debug("Scraper DB: container already exists (concurrent insert): %s", sc.name)

    return inserted
