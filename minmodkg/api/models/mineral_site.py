from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, Field


class CandidateExtractedEntity(BaseModel):
    source: str
    confidence: float
    observed_name: Optional[str]
    normalized_uri: str


class LocationInfo(BaseModel):
    country: Optional[CandidateExtractedEntity] = None
    crs: Optional[CandidateExtractedEntity] = None
    location: Optional[str] = None


class Document(BaseModel):
    doi: Optional[str]
    uri: str
    title: Optional[str]


class BoundingBox(BaseModel):
    x_max: float
    x_min: float
    y_max: float
    y_min: float


class PageInfo(BaseModel):
    bounding_box: Optional[BoundingBox]
    page: int


class Reference(BaseModel):
    document: Document
    page_info: list[PageInfo]
    comment: Optional[str]
    property: Optional[str]


class MineralSiteBase(BaseModel):
    source_id: str
    record_id: str
    name: str
    location_info: Optional[LocationInfo] = None
    deposit_type_candidate: list[CandidateExtractedEntity] = Field(default_factory=list)
    reference: list[Reference] = Field(default_factory=list)


class MineralSite(MineralSiteBase):
    modified_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
    )
    created_by: str


class MineralSiteCreate(MineralSiteBase):
    same_as: list[str] = Field(default_factory=list)


# def get_site_uri(source_id: str, record_id)
