from __future__ import annotations

from typing import Annotated, Optional

from minmodkg.models.derived_mineral_site import GradeTonnage
from minmodkg.typing import IRI
from pydantic import BaseModel


class DedupMineralSiteDepositType(BaseModel):
    source: str
    confidence: float
    id: IRI


class DedupMineralSiteLocation(BaseModel):
    lat: Optional[float]
    long: Optional[float]
    country: list[IRI]
    state_or_province: list[IRI]


class DedupMineralSite(BaseModel):
    id: str
    name: str
    sites: list[IRI]
    deposit_types: list[DedupMineralSiteDepositType]
    location: Optional[DedupMineralSiteLocation]
    grade_tonnage: GradeTonnage
