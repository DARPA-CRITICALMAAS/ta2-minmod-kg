from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Generic

from minmodkg.models_v2.kgrel.custom_types.location import GeoCoordinate
from minmodkg.typing import InternalID, T

if TYPE_CHECKING:
    from minmodkg.models_v2.kgrel.mineral_site import MineralSite


@dataclass
class RefValue(Generic[T]):
    value: T
    refid: InternalID

    @classmethod
    def from_sites(
        cls, sorted_sites: list[MineralSite], attr: Callable[[MineralSite], T]
    ):
        for site in sorted_sites:
            value = attr(site)
            if value:
                return cls(value, refid=site.site_id)
        return None

    def to_dict(self):
        return {"value": self.value, "refid": self.refid}

    @classmethod
    def from_dict(cls, d):
        return cls(d["value"], d["refid"])


@dataclass
class RefListID(RefValue[list[InternalID]]): ...


@dataclass
class RefGeoCoordinate(RefValue[GeoCoordinate]):
    def to_dict(self):
        return {"value": self.value.to_dict(), "refid": self.refid}

    @classmethod
    def from_dict(cls, d):
        return cls(GeoCoordinate.from_dict(d["value"]), d["refid"])
