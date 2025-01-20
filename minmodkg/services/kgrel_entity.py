from __future__ import annotations

from pathlib import Path
from typing import Optional, TypeVar

import serde.json
from minmodkg.models.kg.entities.commodity_form import CommodityForm as KGCommodityForm
from minmodkg.models.kg.entities.crs import CRS as KGCRS
from minmodkg.models.kgrel.base import Base, engine
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

T = TypeVar("T", bound=Base)


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
        self.crs: Optional[list[CRS]] = None

        self.commodity_form_conversion: Optional[dict[IRI, float]] = None
        self.source_score: Optional[dict[IRI, float | None]] = None
        self.crs_name: Optional[dict[IRI, str]] = None

    @staticmethod
    def get_instance():
        if EntityService.instance is None:
            EntityService.instance = EntityService()
        return EntityService.instance

    def get_commodity_form_conversion(self) -> dict[IRI, float]:
        if self.commodity_form_conversion is None:
            self.commodity_form_conversion = {
                comm_form.uri: comm_form.conversion
                for comm_form in self.get_commodity_forms()
            }
        return self.commodity_form_conversion

    def get_source_score(self) -> dict[IRI, float | None]:
        if self.source_score is None:
            self.source_score = {
                source.id: source.score for source in self.get_sources()
            }
        return self.source_score

    def get_crs_name(self) -> dict[IRI, str]:
        if self.crs_name is None:
            self.crs_name = {crs.uri: crs.name for crs in self.get_crs()}
        return self.crs_name

    def get_units(self) -> list[Unit]:
        if self.units is None:
            self.units = self._select_all(Unit)
        return self.units

    def get_commodities(self) -> list[Commodity]:
        if self.commodities is None:
            self.commodities = self._select_all(Commodity)
        return self.commodities

    def get_deposit_types(self) -> list[DepositType]:
        if self.deposit_types is None:
            self.deposit_types = self._select_all(DepositType)
        return self.deposit_types

    def get_countries(self) -> list[Country]:
        if self.countries is None:
            self.countries = self._select_all(Country)
        return self.countries

    def get_state_or_provinces(self) -> list[StateOrProvince]:
        if self.state_or_provinces is None:
            self.state_or_provinces = self._select_all(StateOrProvince)
        return self.state_or_provinces

    def get_commodity_forms(self) -> list[CommodityForm]:
        if self.commodity_forms is None:
            self.commodity_forms = self._select_all(CommodityForm)
        return self.commodity_forms

    def get_sources(self) -> list[Source]:
        if self.sources is None:
            self.sources = self._select_all(Source)
        return self.sources

    def get_crs(self) -> list[CRS]:
        if self.crs is None:
            self.crs = self._select_all(CRS)
        return self.crs

    def _select_all(self, cls: type[T]) -> list[T]:
        with Session(self.engine, expire_on_commit=False) as session:
            return list(session.execute(select(cls)).scalars())


class FileEntityService(EntityService):
    def __init__(self, entity_dir: Path):
        super().__init__()
        self.entity_dir = entity_dir

    def _select_all(self, cls: type[T]) -> list[T]:
        records = serde.json.deser(self.entity_dir / f"{cls.__tablename__}.json")[
            cls.__name__
        ]
        return [cls.from_dict(record) for record in records]
