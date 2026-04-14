from __future__ import annotations

from sqlalchemy.orm import Session

from src.domain.abstract_repo import AbstractRepository


def get_repository(db: Session) -> AbstractRepository:
    from src.domain.postgres_repo import PostgresRepository
    return PostgresRepository(db)
