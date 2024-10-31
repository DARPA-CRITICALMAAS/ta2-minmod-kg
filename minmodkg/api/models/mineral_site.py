from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
from typing import Optional

from pydantic import BaseModel, Field


class CandidateExtractedEntity(BaseModel):
    source: str
    confidence: float
    observed_name: Optional[str] = None
    normalized_uri: Optional[str] = None


class LocationInfo(BaseModel):
    country: Optional[list[CandidateExtractedEntity]] = None
    state_or_province: Optional[list[CandidateExtractedEntity]] = None
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

    def to_enc_str(self):
        return f"BB:{self.x_max:.3f}_{self.x_min:.3f}_{self.y_max:.3f}_{self.y_min:.3f}"


class PageInfo(BaseModel):
    bounding_box: Optional[BoundingBox] = None
    page: int

    def to_enc_str(self):
        if self.bounding_box is None:
            return str(self.page)
        return f"PI:{self.page}|{self.bounding_box.to_enc_str()}"


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


class MineralSiteUpdate(MineralSiteBase):
    same_as: list[str] = Field(default_factory=list)


# class MineralSiteUpdate(BaseModel):
#     source: Optional[str] = None
#     source_prop: Optional[str] = None
#     prop: str
#     object: str


# def get_site_uri(source_id: str, record_id)
