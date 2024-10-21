from __future__ import annotations

import sqlite3
from typing import Annotated, Optional

from fastapi import Depends
from minmodkg.api.models.user import User, UserBase, UserCreate, UserPublic, UserUpdate
from minmodkg.config import DBFILE
from sqlmodel import Session, SQLModel, create_engine

dbconn = f"sqlite:///{DBFILE}"
connect_args = {"check_same_thread": False}
engine = create_engine(dbconn, connect_args=connect_args)


def create_db_and_tables():
    SQLModel.metadata.create_all(engine)


def get_session():
    with Session(engine) as session:
        yield session


SessionDep = Annotated[Session, Depends(get_session)]

__all__ = [
    "User",
    "UserBase",
    "UserCreate",
    "UserPublic",
    "UserUpdate",
    "SessionDep",
    "create_db_and_tables",
    "get_session",
]
