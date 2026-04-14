"""
Seed containers only — no item permutations needed for investment model.
Safe to call on every startup (idempotent).

Container names match Steam Community Market exactly (verified).
Group capsules (Challengers/Legends/Contenders/Champions) are used
instead of individual team capsules — those are not listed on SCM.

Base costs are in KZT (MSRP at release, rounded to nearest 10₸).
Conversion reference: ~481₸/$ used for initial seed values.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any

from sqlalchemy.orm import Session

from src.domain.models import ContainerType, DimContainer

logger = logging.getLogger(__name__)

_CONTAINERS: list[dict[str, Any]] = [
    # ── Weapon Cases ──────────────────────────────────────────────────────────
    {"name": "Kilowatt Case", "type": ContainerType.Weapon_Case, "cost": 1920},
    {"name": "Gallery Case", "type": ContainerType.Weapon_Case, "cost": 1680},
    {"name": "Revolution Case", "type": ContainerType.Weapon_Case, "cost": 1580},
    {"name": "Recoil Case", "type": ContainerType.Weapon_Case, "cost": 1540},
    {"name": "Dreams & Nightmares Case", "type": ContainerType.Weapon_Case, "cost": 1580},
    {"name": "Fracture Case", "type": ContainerType.Weapon_Case, "cost": 1490},
    {"name": "Snakebite Case", "type": ContainerType.Weapon_Case, "cost": 1490},
    {"name": "Prisma 2 Case", "type": ContainerType.Weapon_Case, "cost": 1490},
    {"name": "CS20 Case", "type": ContainerType.Weapon_Case, "cost": 1490},
    {"name": "Danger Zone Case", "type": ContainerType.Weapon_Case, "cost": 1490},
    # ── Souvenir Packages ─────────────────────────────────────────────────────
    {
        "name": "PGL CS2 Major Copenhagen 2024 Souvenir Package",
        "type": ContainerType.Souvenir_Package,
        "cost": 1200,
    },
    # ── Armory Terminals ──────────────────────────────────────────────────────
    {"name": "Sealed Genesis Terminal", "type": ContainerType.Sealed_Terminal, "cost": 1200},
    {"name": "Sealed Dead Hand Terminal", "type": ContainerType.Sealed_Terminal, "cost": 1200},
    # ── Budapest 2025 Capsules ────────────────────────────────────────────────
    {
        "name": "Budapest 2025 Challengers Autograph Capsule",
        "type": ContainerType.Autograph_Capsule,
        "cost": 290,
    },
    {
        "name": "Budapest 2025 Legends Autograph Capsule",
        "type": ContainerType.Autograph_Capsule,
        "cost": 310,
    },
    {
        "name": "Budapest 2025 Contenders Autograph Capsule",
        "type": ContainerType.Autograph_Capsule,
        "cost": 260,
    },
    {
        "name": "Budapest 2025 Challengers Sticker Capsule",
        "type": ContainerType.Event_Capsule,
        "cost": 240,
    },
    {
        "name": "Budapest 2025 Legends Sticker Capsule",
        "type": ContainerType.Event_Capsule,
        "cost": 240,
    },
    {
        "name": "Budapest 2025 Contenders Sticker Capsule",
        "type": ContainerType.Event_Capsule,
        "cost": 240,
    },
    # ── Copenhagen 2024 Capsules ──────────────────────────────────────────────
    {
        "name": "Copenhagen 2024 Challengers Autograph Capsule",
        "type": ContainerType.Autograph_Capsule,
        "cost": 720,
    },
    {
        "name": "Copenhagen 2024 Legends Autograph Capsule",
        "type": ContainerType.Autograph_Capsule,
        "cost": 870,
    },
    {
        "name": "Copenhagen 2024 Contenders Autograph Capsule",
        "type": ContainerType.Autograph_Capsule,
        "cost": 720,
    },
    {
        "name": "Copenhagen 2024 Champions Autograph Capsule",
        "type": ContainerType.Autograph_Capsule,
        "cost": 960,
    },
    {
        "name": "Copenhagen 2024 Legends Sticker Capsule",
        "type": ContainerType.Event_Capsule,
        "cost": 480,
    },
    {
        "name": "Copenhagen 2024 Challengers Sticker Capsule",
        "type": ContainerType.Event_Capsule,
        "cost": 480,
    },
    {
        "name": "Copenhagen 2024 Contenders Sticker Capsule",
        "type": ContainerType.Event_Capsule,
        "cost": 480,
    },
    # ── Paris 2023 Capsules ───────────────────────────────────────────────────
    {
        "name": "Paris 2023 Challengers Autograph Capsule",
        "type": ContainerType.Autograph_Capsule,
        "cost": 480,
    },
    {
        "name": "Paris 2023 Legends Autograph Capsule",
        "type": ContainerType.Autograph_Capsule,
        "cost": 480,
    },
    {
        "name": "Paris 2023 Contenders Autograph Capsule",
        "type": ContainerType.Autograph_Capsule,
        "cost": 480,
    },
    {
        "name": "Paris 2023 Champions Autograph Capsule",
        "type": ContainerType.Autograph_Capsule,
        "cost": 580,
    },
    {
        "name": "Paris 2023 Legends Sticker Capsule",
        "type": ContainerType.Event_Capsule,
        "cost": 240,
    },
    {
        "name": "Paris 2023 Challengers Sticker Capsule",
        "type": ContainerType.Event_Capsule,
        "cost": 240,
    },
    {
        "name": "Paris 2023 Contenders Sticker Capsule",
        "type": ContainerType.Event_Capsule,
        "cost": 240,
    },
    # ── Antwerp 2022 Capsules ─────────────────────────────────────────────────
    {
        "name": "Antwerp 2022 Challengers Autograph Capsule",
        "type": ContainerType.Autograph_Capsule,
        "cost": 480,
    },
    {
        "name": "Antwerp 2022 Legends Autograph Capsule",
        "type": ContainerType.Autograph_Capsule,
        "cost": 480,
    },
    {
        "name": "Antwerp 2022 Contenders Autograph Capsule",
        "type": ContainerType.Autograph_Capsule,
        "cost": 480,
    },
    {
        "name": "Antwerp 2022 Challengers Sticker Capsule",
        "type": ContainerType.Event_Capsule,
        "cost": 480,
    },
    {
        "name": "Antwerp 2022 Legends Sticker Capsule",
        "type": ContainerType.Event_Capsule,
        "cost": 480,
    },
    {
        "name": "Antwerp 2022 Contenders Sticker Capsule",
        "type": ContainerType.Event_Capsule,
        "cost": 480,
    },
]


def seed_database(db: Session) -> None:
    existing = {c.container_name for c in db.query(DimContainer).all()}
    added = 0
    for cdata in _CONTAINERS:
        if cdata["name"] in existing:
            continue
        db.add(
            DimContainer(
                container_id=str(uuid.uuid4()),
                container_name=cdata["name"],
                container_type=cdata["type"],
                base_cost=cdata["cost"],
            )
        )
        added += 1
    if added:
        db.commit()
        logger.info("Seeded %d new containers.", added)
