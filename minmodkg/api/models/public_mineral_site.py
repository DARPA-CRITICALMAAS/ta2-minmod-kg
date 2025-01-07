from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Annotated, Optional, Union

from minmodkg.misc.utils import makedict
from minmodkg.models.base import MINMOD_NS
from minmodkg.models_v2.inputs.candidate_entity import CandidateEntity
from minmodkg.models_v2.inputs.location_info import LocationInfo
from minmodkg.models_v2.inputs.mineral_inventory import MineralInventory
from minmodkg.models_v2.inputs.mineral_site import MineralSite as InputMineralSite
from minmodkg.models_v2.inputs.reference import Reference
from minmodkg.models_v2.kgrel.mineral_site import MineralSite
from minmodkg.typing import IRI, InternalID
from pydantic import BaseModel, Field


class GradeTonnage(BaseModel):
    commodity: InternalID
    total_contained_metal: Optional[float] = None
    total_tonnage: Optional[float] = None
    total_grade: Optional[float] = None
    date: Optional[str] = None


class Coordinates(BaseModel):
    lat: Annotated[float, "Latitude"]
    lon: Annotated[float, "Longitude"]


class PublicMineralSite(BaseModel):
    id: InternalID
    source_id: str
    record_id: Union[str, int]
    dedup_site_uri: Optional[IRI]
    name: Optional[str]
    created_by: list[IRI]
    aliases: list[str]
    site_rank: Optional[str] = None
    site_type: Optional[str] = None
    location_info: Optional[LocationInfo] = None
    deposit_type_candidate: list[CandidateEntity] = Field(default_factory=list)
    mineral_inventory: list[MineralInventory] = Field(default_factory=list)
    reference: list[Reference] = Field(default_factory=list)
    modified_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
    )

    coordinates: Optional[Coordinates] = None
    grade_tonnage: list[GradeTonnage] = Field(default_factory=list)

    @staticmethod
    def from_kgrel(site: MineralSite):
        return PublicMineralSite(
            id=site.site_id,
            source_id=site.source_id,
            record_id=site.record_id,
            dedup_site_uri=MINMOD_NS.md.uristr(site.dedup_site_id),
            name=site.name,
            created_by=site.created_by,
            aliases=site.aliases,
            site_rank=site.rank,
            site_type=site.type,
            location_info=(
                LocationInfo(
                    country=site.location.country,
                    state_or_province=site.location.state_or_province,
                    crs=site.location.crs,
                    location=site.location.coordinates,
                )
                if site.location is not None
                else None
            ),
            deposit_type_candidate=site.deposit_type_candidates,
            mineral_inventory=site.inventories,
            reference=site.reference,
            modified_at=site.modified_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
            coordinates=(
                Coordinates(
                    lat=site.location_view.lat,
                    lon=site.location_view.lon,
                )
                if site.location_view.lat is not None
                and site.location_view.lon is not None
                else None
            ),
            grade_tonnage=[
                GradeTonnage(
                    commodity=gt.commodity,
                    total_contained_metal=gt.contained_metal,
                    total_tonnage=gt.tonnage,
                    total_grade=gt.grade,
                    date=gt.date,
                )
                for gt in site.inventory_views
            ],
        )

    def to_dict(self):
        return makedict.without_none_or_empty_list(
            (
                ("id", self.id),
                ("source_id", self.source_id),
                ("record_id", self.record_id),
                ("dedup_site_uri", self.dedup_site_uri),
                ("name", self.name),
                ("created_by", self.created_by),
                ("aliases", self.aliases),
                ("site_rank", self.site_rank),
                ("site_type", self.site_type),
                (
                    "location_info",
                    (
                        self.location_info.to_dict()
                        if self.location_info is not None
                        else None
                    ),
                ),
                (
                    "deposit_type_candidate",
                    [c.to_dict() for c in self.deposit_type_candidate],
                ),
                ("mineral_inventory", [v.to_dict() for v in self.mineral_inventory]),
                ("reference", [r.to_dict() for r in self.reference]),
                ("modified_at", self.modified_at),
                (
                    "coordinates",
                    self.coordinates if self.coordinates is not None else None,
                ),
                (
                    "grade_tonnage",
                    [gt.model_dump(exclude_none=True) for gt in self.grade_tonnage],
                ),
            )
        )


@dataclass
class InputPublicMineralSite(InputMineralSite):
    dedup_site_uri: Optional[IRI] = None

    def to_kgrel(
        self,
        material_form: dict[str, float],
        crs_names: dict[str, str],
    ) -> MineralSite:
        site = MineralSite.from_raw_site(
            self,
            material_form=material_form,
            crs_names=crs_names,
        )
        if self.dedup_site_uri is not None:
            site.dedup_site_id = MINMOD_NS.md.id(self.dedup_site_uri)
        return site

    def to_dict(self):
        d = super().to_dict()
        if self.dedup_site_uri is not None:
            d["dedup_site_uri"] = self.dedup_site_uri
        return d


class UpdateDedupLink(BaseModel):
    """A class represents the latest dedup links"""

    sites: list[InternalID]
