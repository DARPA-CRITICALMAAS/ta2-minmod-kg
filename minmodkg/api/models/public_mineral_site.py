from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Annotated, Optional, Union

from fastapi import APIRouter, Body, HTTPException, Query, Response, status
from minmodkg.misc.utils import format_datetime, format_nanoseconds, makedict
from minmodkg.models.base import MINMOD_NS
from minmodkg.models_v2.inputs.candidate_entity import CandidateEntity
from minmodkg.models_v2.inputs.location_info import LocationInfo
from minmodkg.models_v2.inputs.mineral_inventory import MineralInventory
from minmodkg.models_v2.inputs.mineral_site import MineralSite as InputMineralSite
from minmodkg.models_v2.inputs.reference import Reference
from minmodkg.models_v2.kgrel.dedup_mineral_site import DedupMineralSite
from minmodkg.models_v2.kgrel.mineral_site import MineralSite, MineralSiteAndInventory
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


class OutputPublicMineralSite(BaseModel):
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
        default_factory=lambda: format_datetime(datetime.now(timezone.utc))
    )

    coordinates: Optional[Coordinates] = None
    grade_tonnage: list[GradeTonnage] = Field(default_factory=list)
    snapshot_id: str = ""

    @staticmethod
    def from_kgrel(site: MineralSiteAndInventory):
        ms = site.ms
        return OutputPublicMineralSite(
            id=ms.site_id,
            source_id=ms.source_id,
            record_id=ms.record_id,
            dedup_site_uri=MINMOD_NS.md.uristr(ms.dedup_site_id),
            name=ms.name,
            created_by=ms.created_by,
            aliases=ms.aliases,
            site_rank=ms.rank,
            site_type=ms.type,
            location_info=(
                LocationInfo(
                    country=ms.location.country,
                    state_or_province=ms.location.state_or_province,
                    crs=ms.location.crs,
                    location=ms.location.coordinates,
                )
                if ms.location is not None
                else None
            ),
            deposit_type_candidate=ms.deposit_type_candidates,
            mineral_inventory=ms.inventories,
            reference=ms.reference,
            modified_at=format_nanoseconds(ms.modified_at),
            coordinates=(
                Coordinates(
                    lat=ms.location_view.lat,
                    lon=ms.location_view.lon,
                )
                if ms.location_view.lat is not None and ms.location_view.lon is not None
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
                for gt in site.invs
            ],
            snapshot_id=str(ms.modified_at),
        )

    def clone(self):
        return self.model_copy()

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
                (
                    "snapshot_id",
                    self.snapshot_id,
                ),
            )
        )


@dataclass
class InputPublicMineralSite(InputMineralSite):
    created_by: Union[str, list[str]] = field(default_factory=list)
    dedup_site_uri: Optional[IRI] = None

    def __post_init__(self):
        if self.dedup_site_uri is None:
            self.dedup_site_uri = MINMOD_NS.md.uristr(
                MineralSite.get_dedup_id((self.id,))
            )
        if isinstance(self.created_by, str):
            self.created_by = [self.created_by]
        if not isinstance(self.created_by, list):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="created_by must be a string or a list of strings.",
            )

    @property
    def dedup_site_id(self):
        assert self.dedup_site_uri is not None
        return MINMOD_NS.md.id(self.dedup_site_uri)

    def to_kgrel(
        self,
        material_form: dict[str, float],
        crs_names: dict[str, str],
        source_score: dict[str, float],
    ) -> MineralSiteAndInventory:
        site = MineralSiteAndInventory.from_raw_site(
            self,
            material_form=material_form,
            crs_names=crs_names,
            source_score=source_score,
        )
        site.ms.dedup_site_id = self.dedup_site_id
        return site

    def to_dict(self):
        d = super().to_dict()
        if self.dedup_site_uri is not None:
            d["dedup_site_uri"] = self.dedup_site_uri
        return d


class UpdateDedupLink(BaseModel):
    """A class represents the latest dedup links"""

    sites: list[InternalID]
