from __future__ import annotations

from dataclasses import dataclass, field
from typing import Annotated, Optional

import shapely.wkt
from minmodkg.misc.geo import reproject_geometry
from minmodkg.misc.utils import extend_unique, makedict
from minmodkg.models.base import MINMOD_NS
from minmodkg.models_v2.inputs.candidate_entity import CandidateEntity
from minmodkg.typing import InternalID


@dataclass
class Location:
    country: list[CandidateEntity] = field(default_factory=list)
    state_or_province: list[CandidateEntity] = field(default_factory=list)
    crs: Optional[CandidateEntity] = None
    coordinates: Annotated[Optional[str], "WKT string"] = None

    def to_dict(self):
        return makedict.without_none_or_empty_list(
            (
                ("country", [ent.to_dict() for ent in self.country]),
                (
                    "state_or_province",
                    [ent.to_dict() for ent in self.state_or_province],
                ),
                ("crs", self.crs.to_dict() if self.crs else None),
                ("coordinates", self.coordinates),
            )
        )

    @classmethod
    def from_dict(cls, d):
        return cls(
            country=[CandidateEntity.from_dict(ent) for ent in d.get("country", [])],
            state_or_province=[
                CandidateEntity.from_dict(ent) for ent in d.get("state_or_province", [])
            ],
            crs=CandidateEntity.from_dict(d["crs"]) if d.get("crs") else None,
            coordinates=d.get("coordinates"),
        )


@dataclass
class GeoCoordinate:
    lat: Optional[Annotated[float, "Latitude"]] = None
    lon: Optional[Annotated[float, "Longitude"]] = None

    def to_dict(self):
        return makedict.without_none((("lat", self.lat), ("lon", self.lon)))

    @classmethod
    def from_dict(cls, d: dict):
        return GeoCoordinate(lat=d.get("lat"), lon=d.get("lon"))


@dataclass
class LocationView(GeoCoordinate):
    country: list[InternalID] = field(default_factory=list)
    state_or_province: list[InternalID] = field(default_factory=list)

    def is_empty(self):
        return (
            self.lat is None
            and self.lon is None
            and len(self.country) == 0
            and len(self.state_or_province) == 0
        )

    def to_dict(self):
        return makedict.without_none_or_empty_list(
            (
                ("lat", self.lat),
                ("lon", self.lon),
                ("country", self.country),
                ("state_or_province", self.state_or_province),
            )
        )

    @classmethod
    def from_dict(cls, d):
        return cls(
            lat=d.get("lat"),
            lon=d.get("lon"),
            country=d.get("country", []),
            state_or_province=d.get("state_or_province", []),
        )

    def combine(self, other: Optional[LocationView]):
        if other is None:
            return self

        if self.lat is None and other.lat is not None:
            self.lat = other.lat
        if self.lon is None and other.lon is not None:
            self.lon = other.lon

        self.country = extend_unique(self.country, other.country)
        self.state_or_province = extend_unique(
            self.state_or_province, other.state_or_province
        )
        return self

    @staticmethod
    def from_location(location: Location, crss: dict[str, str]) -> LocationView:
        view = LocationView()
        if location.coordinates is not None:
            if location.crs is None or location.crs.normalized_uri is None:
                crs = "EPSG:4326"
            else:
                crs = crss[location.crs.normalized_uri]

            # TODO: fix this nan
            if "nan" in location.coordinates.lower():
                centroid = None
            else:
                try:
                    assert crs.startswith("EPSG:")
                    geometry = shapely.wkt.loads(location.coordinates)
                    centroid = shapely.centroid(geometry)
                    centroid = reproject_geometry(centroid, crs, "EPSG:4326")
                except shapely.errors.GEOSException:
                    centroid = None

            if centroid is not None:
                view.lat = centroid.y
                view.lon = centroid.x

        view.country = [
            MINMOD_NS.mr.id(ent.normalized_uri)
            for ent in location.country
            if ent.normalized_uri is not None
        ]
        view.state_or_province = [
            MINMOD_NS.mr.id(ent.normalized_uri)
            for ent in location.state_or_province
            if ent.normalized_uri is not None
        ]
        return view
