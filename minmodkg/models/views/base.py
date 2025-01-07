from __future__ import annotations

from minmodkg.config import MINMOD_DEBUG, MINMOD_KGREL_DB
from minmodkg.models.views.custom_types import ComputedLocation, DataclassType, Event
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Session


class Base(DeclarativeBase):
    type_annotation_map = {
        ComputedLocation: DataclassType(ComputedLocation),
        Event: DataclassType(Event),
    }


dbconn = MINMOD_KGREL_DB
if dbconn.startswith("sqlite"):
    connect_args = {"check_same_thread": False}
else:
    connect_args = {}
engine = create_engine(dbconn, connect_args=connect_args, echo=MINMOD_DEBUG)


def create_db_and_tables():
    Base.metadata.create_all(engine)


def get_session():
    with Session(engine) as session:
        yield session
