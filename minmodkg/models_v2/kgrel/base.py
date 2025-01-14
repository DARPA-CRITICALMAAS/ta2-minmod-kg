from __future__ import annotations

from contextlib import contextmanager

from minmodkg.config import MINMOD_DEBUG, MINMOD_KGREL_DB
from minmodkg.models_v2.inputs.candidate_entity import CandidateEntity
from minmodkg.models_v2.inputs.mineral_inventory import MineralInventory
from minmodkg.models_v2.inputs.reference import Reference
from minmodkg.models_v2.kgrel.custom_types import (
    DataclassType,
    DedupMineralSiteDepositType,
    ListDataclassType,
    Location,
    LocationView,
    RefGeoCoordinate,
    RefListID,
    RefValue,
    SiteAndScore,
)
from sqlalchemy import create_engine, update
from sqlalchemy.orm import DeclarativeBase, Session


class Base(DeclarativeBase):
    type_annotation_map = {
        CandidateEntity: DataclassType(CandidateEntity),
        Location: DataclassType(Location),
        LocationView: DataclassType(LocationView),
        RefValue: DataclassType(RefValue),
        RefValue[str]: DataclassType(RefValue[str]),
        RefGeoCoordinate: DataclassType(RefGeoCoordinate),
        RefListID: DataclassType(RefListID),
        SiteAndScore: DataclassType(SiteAndScore),
        list[SiteAndScore]: ListDataclassType(SiteAndScore),
        list[DedupMineralSiteDepositType]: ListDataclassType(
            DedupMineralSiteDepositType
        ),
        list[CandidateEntity]: ListDataclassType(CandidateEntity),
        list[MineralInventory]: ListDataclassType(MineralInventory),
        list[Reference]: ListDataclassType(Reference),
    }

    def get_update_query(self):
        q = update(self.__class__)
        args = {}
        for col in self.__table__.columns:
            val = getattr(self, col.name)
            if col.primary_key:
                q = q.where(getattr(self.__class__, col.name) == val)
            args[col.name] = val

        return q.values(**args)

    def get_update_args(self):
        return {col.name: getattr(self, col.name) for col in self.__table__.columns}


dbconn = MINMOD_KGREL_DB
if dbconn.startswith("sqlite"):
    connect_args = {"check_same_thread": False}
else:
    connect_args = {}
engine = create_engine(dbconn, connect_args=connect_args, echo=MINMOD_DEBUG)


def create_db_and_tables():
    Base.metadata.create_all(engine)


@contextmanager
def get_rel_session():
    with Session(engine) as session:
        yield session
