from __future__ import annotations

from typing import Optional

from minmodkg.models.kg.entities.commodity_form import CommodityForm as KGCommodityForm
from minmodkg.models.kg.entities.crs import CRS as KGCRS
from minmodkg.models.kgrel.base import engine
from minmodkg.models.kgrel.entities.commodity import Commodity
from minmodkg.models.kgrel.entities.commodity_form import CommodityForm
from minmodkg.models.kgrel.entities.country import Country
from minmodkg.models.kgrel.entities.crs import CRS
from minmodkg.models.kgrel.entities.deposit_type import DepositType
from minmodkg.models.kgrel.entities.state_or_province import StateOrProvince
from minmodkg.models.kgrel.entities.unit import Unit
from minmodkg.models.kgrel.source import Source
from minmodkg.typing import IRI, InternalID
from sqlalchemy import Engine, select
from sqlalchemy.orm import Session


class EntityService:
    instance = None

    def __init__(self, _engine: Optional[Engine] = None):
        self.engine = _engine or engine

        self.units: Optional[list[Unit]] = None
        self.commodities: Optional[list[Commodity]] = None
        self.deposit_types: Optional[list[DepositType]] = None
        self.countries: Optional[list[Country]] = None
        self.state_or_provinces: Optional[list[StateOrProvince]] = None
        self.commodity_forms: Optional[list[CommodityForm]] = None
        self.sources: Optional[list[Source]] = None

        self.commodity_form_conversion: Optional[dict[IRI, float]] = None
        self.source_score: Optional[dict[IRI, float]] = None
        self.crs_name: Optional[dict[IRI, str]] = None

    @staticmethod
    def get_instance():
        if EntityService.instance is None:
            EntityService.instance = EntityService()
        return EntityService.instance

    def get_commodity_form_conversion(self) -> dict[IRI, float]:
        if self.commodity_form_conversion is None:
            if self.commodity_forms is None:
                with Session(self.engine, expire_on_commit=False) as session:
                    self.commodity_form_conversion = {
                        KGCommodityForm.__subj__.key_ns.uristr(row[0]): row[1]
                        for row in session.execute(
                            select(CommodityForm.id, CommodityForm.conversion)
                        ).all()
                    }
            else:
                self.commodity_form_conversion = {
                    comm_form.uri: comm_form.conversion
                    for comm_form in self.commodity_forms
                }
        return self.commodity_form_conversion

    def get_source_score(self) -> dict[IRI, float]:
        if self.source_score is None:
            with Session(self.engine, expire_on_commit=False) as session:
                self.source_score = {
                    row[0]: row[1]
                    for row in session.execute(select(Source.id, Source.score)).all()
                }
        return self.source_score

    def get_crs_name(self) -> dict[IRI, str]:
        if self.crs_name is None:
            with Session(self.engine, expire_on_commit=False) as session:
                self.crs_name = {
                    KGCRS.__subj__.key_ns.uristr(row[0]): row[1]
                    for row in session.execute(select(CRS.id, CRS.name)).all()
                }
        return self.crs_name

    def get_units(self) -> list[Unit]:
        if self.units is None:
            with Session(self.engine, expire_on_commit=False) as session:
                self.units = list(session.execute(select(Unit)).scalars())
        return self.units

    def get_commodities(self) -> list[Commodity]:
        if self.commodities is None:
            with Session(self.engine, expire_on_commit=False) as session:
                self.commodities = list(session.execute(select(Commodity)).scalars())
        return self.commodities

    def get_deposit_types(self) -> list[DepositType]:
        if self.deposit_types is None:
            with Session(self.engine, expire_on_commit=False) as session:
                self.deposit_types = list(
                    session.execute(select(DepositType)).scalars()
                )
        return self.deposit_types

    def get_countries(self) -> list[Country]:
        if self.countries is None:
            with Session(self.engine, expire_on_commit=False) as session:
                self.countries = list(session.execute(select(Country)).scalars())
        return self.countries

    def get_state_or_provinces(self) -> list[StateOrProvince]:
        if self.state_or_provinces is None:
            with Session(self.engine, expire_on_commit=False) as session:
                self.state_or_provinces = list(
                    session.execute(select(StateOrProvince)).scalars()
                )
        return self.state_or_provinces

    def get_commodity_forms(self) -> list[CommodityForm]:
        if self.commodity_forms is None:
            with Session(self.engine, expire_on_commit=False) as session:
                self.commodity_forms = list(
                    session.execute(select(CommodityForm)).scalars()
                )
        return self.commodity_forms

    def get_sources(self) -> list[Source]:
        if self.sources is None:
            with Session(self.engine, expire_on_commit=False) as session:
                self.sources = list(session.execute(select(Source)).scalars())
        return self.sources
