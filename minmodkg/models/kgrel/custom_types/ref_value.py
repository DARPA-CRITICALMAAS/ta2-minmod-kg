from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Generic, Optional

from minmodkg.models.kg.geology_info import GeologyInfo, RockType
from minmodkg.models.kgrel.custom_types import DedupMineralSiteDepositType
from minmodkg.models.kgrel.custom_types.location import GeoCoordinate
from minmodkg.typing import InternalID, T

if TYPE_CHECKING:
    from minmodkg.models.kgrel.dedup_mineral_site import DedupMineralSite
    from minmodkg.models.kgrel.mineral_site import MineralSite


@dataclass
class RefValue(Generic[T]):
    value: T
    refid: InternalID

    @classmethod
    def from_sites(
        cls, sorted_sites: list[MineralSite], attr: Callable[[MineralSite], T | None]
    ):
        for site in sorted_sites:
            value = attr(site)
            if value is not None:
                return cls(value, refid=site.site_id)
        return None

    def to_dict(self):
        return {"value": self.value, "refid": self.refid}

    @classmethod
    def from_dict(cls, d):
        return cls(d["value"], d["refid"])

    @classmethod
    def as_composite(cls, value: T | None, refid: InternalID | None):
        if refid is None or value is None:
            return None
        return cls(value, refid)

    def __composite_values__(self):
        return (self.value, self.refid)


@dataclass
class RefListID(RefValue[list[InternalID]]):
    # repeat the type hint because SQLAlchemy can't handle generics yet
    value: list[InternalID]
    refid: InternalID


@dataclass
class RefListStr(RefValue[list[str]]):
    # repeat the type hint because SQLAlchemy can't handle generics yet
    value: list[str]
    refid: InternalID


@dataclass
class RefGeoCoordinate(RefValue[GeoCoordinate]):
    def to_dict(self):
        return {"value": self.value.to_dict(), "refid": self.refid}

    @classmethod
    def from_dict(cls, d):
        return cls(GeoCoordinate.from_dict(d["value"]), d["refid"])


@dataclass
class RefDepositType(RefValue[DedupMineralSiteDepositType]):
    def to_dict(self):
        return {"value": self.value.to_dict(), "refid": self.refid}

    @classmethod
    def from_dict(cls, d):
        return cls(DedupMineralSiteDepositType.from_dict(d["value"]), d["refid"])


@dataclass
class RefRockType:
    unit: Optional[RefValue[str]] = None
    type: Optional[RefValue[str]] = None

    @classmethod
    def from_sites(
        cls,
        sorted_sites: list[MineralSite],
        attr: Callable[[MineralSite], RockType | None],
    ):
        out = cls(
            unit=RefValue.from_sites(
                sorted_sites,
                lambda s: p.unit if (p := attr(s)) is not None else None,
            ),
            type=RefValue.from_sites(
                sorted_sites,
                lambda s: p.type if (p := attr(s)) is not None else None,
            ),
        )
        if out.unit is None and out.type is None:
            return None
        return out

    def to_rock_type(self) -> Optional[RockType]:
        if self.unit is None and self.type is None:
            return None
        return RockType(
            unit=self.unit.value if self.unit else None,
            type=self.type.value if self.type else None,
        )

    def to_dict(self):
        return {
            "unit": self.unit.to_dict() if self.unit else None,
            "type": self.type.to_dict() if self.type else None,
        }

    @classmethod
    def from_dict(cls, d):
        return cls(
            unit=RefValue.from_dict(d["unit"]) if d.get("unit") else None,
            type=RefValue.from_dict(d["type"]) if d.get("type") else None,
        )


@dataclass
class RefGeologyInfo:
    alteration: Optional[RefValue[str]] = None
    concentration_process: Optional[RefValue[str]] = None
    ore_control: Optional[RefValue[str]] = None
    host_rock: Optional[RefRockType] = None
    associated_rock: Optional[RefRockType] = None
    structure: Optional[RefValue[str]] = None
    tectonic: Optional[RefValue[str]] = None

    def is_empty(self):
        return (
            self.alteration is None
            and self.concentration_process is None
            and self.ore_control is None
            and self.host_rock is None
            and self.associated_rock is None
            and self.structure is None
            and self.tectonic is None
        )

    def to_geology_info(self) -> Optional[GeologyInfo]:
        if self.is_empty():
            return None
        return GeologyInfo(
            alteration=self.alteration.value if self.alteration else None,
            concentration_process=(
                self.concentration_process.value if self.concentration_process else None
            ),
            ore_control=self.ore_control.value if self.ore_control else None,
            host_rock=self.host_rock.to_rock_type() if self.host_rock else None,
            associated_rock=(
                self.associated_rock.to_rock_type() if self.associated_rock else None
            ),
            structure=self.structure.value if self.structure else None,
            tectonic=self.tectonic.value if self.tectonic else None,
        )

    @classmethod
    def from_sites(cls, sorted_sites: list[MineralSite]):
        alteration = RefValue.from_sites(
            sorted_sites,
            lambda s: s.geology_info.alteration if s.geology_info is not None else None,
        )
        concentration_process = RefValue.from_sites(
            sorted_sites,
            lambda s: (
                s.geology_info.concentration_process
                if s.geology_info is not None
                else None
            ),
        )
        ore_control = RefValue.from_sites(
            sorted_sites,
            lambda s: (
                s.geology_info.ore_control if s.geology_info is not None else None
            ),
        )
        host_rock = RefRockType.from_sites(
            sorted_sites,
            lambda s: s.geology_info.host_rock if s.geology_info is not None else None,
        )
        associated_rock = RefRockType.from_sites(
            sorted_sites,
            lambda s: (
                s.geology_info.associated_rock if s.geology_info is not None else None
            ),
        )
        structure = RefValue.from_sites(
            sorted_sites,
            lambda s: s.geology_info.structure if s.geology_info is not None else None,
        )
        tectonic = RefValue.from_sites(
            sorted_sites,
            lambda s: s.geology_info.tectonic if s.geology_info is not None else None,
        )

        return cls(
            alteration=alteration,
            concentration_process=concentration_process,
            ore_control=ore_control,
            host_rock=host_rock,
            associated_rock=associated_rock,
            structure=structure,
            tectonic=tectonic,
        )

    @classmethod
    def from_dedup_sites(cls, sorted_dedup_sites: list[DedupMineralSite]):
        alteration = next(
            (
                dms.geology_info.alteration
                for dms in sorted_dedup_sites
                if dms.geology_info.alteration is not None
            ),
            sorted_dedup_sites[0].geology_info.alteration,
        )
        concentration_process = next(
            (
                dms.geology_info.concentration_process
                for dms in sorted_dedup_sites
                if dms.geology_info.concentration_process is not None
            ),
            sorted_dedup_sites[0].geology_info.concentration_process,
        )
        ore_control = next(
            (
                dms.geology_info.ore_control
                for dms in sorted_dedup_sites
                if dms.geology_info.ore_control is not None
            ),
            sorted_dedup_sites[0].geology_info.ore_control,
        )
        host_rock = next(
            (
                dms.geology_info.host_rock
                for dms in sorted_dedup_sites
                if dms.geology_info.host_rock is not None
            ),
            sorted_dedup_sites[0].geology_info.host_rock,
        )
        associated_rock = next(
            (
                dms.geology_info.associated_rock
                for dms in sorted_dedup_sites
                if dms.geology_info.associated_rock is not None
            ),
            sorted_dedup_sites[0].geology_info.associated_rock,
        )
        structure = next(
            (
                dms.geology_info.structure
                for dms in sorted_dedup_sites
                if dms.geology_info.structure is not None
            ),
            sorted_dedup_sites[0].geology_info.structure,
        )
        tectonic = next(
            (
                dms.geology_info.tectonic
                for dms in sorted_dedup_sites
                if dms.geology_info.tectonic is not None
            ),
            sorted_dedup_sites[0].geology_info.tectonic,
        )
        return cls(
            alteration=alteration,
            concentration_process=concentration_process,
            ore_control=ore_control,
            host_rock=host_rock,
            associated_rock=associated_rock,
            structure=structure,
            tectonic=tectonic,
        )

    def to_dict(self):
        return {
            "alteration": self.alteration.to_dict() if self.alteration else None,
            "concentration_process": (
                self.concentration_process.to_dict()
                if self.concentration_process
                else None
            ),
            "ore_control": self.ore_control.to_dict() if self.ore_control else None,
            "host_rock": self.host_rock.to_dict() if self.host_rock else None,
            "associated_rock": (
                self.associated_rock.to_dict() if self.associated_rock else None
            ),
            "structure": (self.structure.to_dict() if self.structure else None),
            "tectonic": (self.tectonic.to_dict() if self.tectonic else None),
        }

    @classmethod
    def from_dict(cls, d):
        return cls(
            alteration=(
                RefValue.from_dict(d["alteration"])
                if d["alteration"] is not None
                else None
            ),
            concentration_process=(
                RefValue.from_dict(d["concentration_process"])
                if d["concentration_process"] is not None
                else None
            ),
            ore_control=(
                RefValue.from_dict(d["ore_control"])
                if d["ore_control"] is not None
                else None
            ),
            host_rock=(
                RefRockType.from_dict(d["host_rock"])
                if d["host_rock"] is not None
                else None
            ),
            associated_rock=(
                RefRockType.from_dict(d["associated_rock"])
                if d["associated_rock"] is not None
                else None
            ),
            structure=(
                RefValue.from_dict(d["structure"])
                if d["structure"] is not None
                else None
            ),
            tectonic=(
                RefValue.from_dict(d["tectonic"]) if d["tectonic"] is not None else None
            ),
        )
