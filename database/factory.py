from __future__ import annotations

from sqlalchemy.orm import Session

from database.abstract_repo import AbstractRepository


def get_repository(db: Session) -> AbstractRepository:
    from database.postgres_repo import PostgresRepository
    return PostgresRepository(db)
