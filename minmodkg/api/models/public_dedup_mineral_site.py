from __future__ import annotations

from typing import Optional

from minmodkg.models.derived_mineral_site import GradeTonnage
from minmodkg.models_v2.kgrel.dedup_mineral_site import DedupMineralSite
from minmodkg.typing import InternalID
from pydantic import BaseModel


class DedupMineralSiteDepositType(BaseModel):
    id: InternalID
    source: str
    confidence: float


class DedupMineralSiteLocation(BaseModel):
    lat: Optional[float] = None
    lon: Optional[float] = None
    country: list[InternalID]
    state_or_province: list[InternalID]

    def is_empty(self):
        return (
            self.lat is None
            and self.lon is None
            and len(self.country) == 0
            and len(self.state_or_province) == 0
        )


class DedupMineralSiteIdAndScore(BaseModel):
    id: str
    score: float


class DedupMineralSitePublic(BaseModel):
    id: InternalID
    name: str
    type: str
    rank: str
    sites: list[DedupMineralSiteIdAndScore]
    deposit_types: list[DedupMineralSiteDepositType]
    location: Optional[DedupMineralSiteLocation] = None
    grade_tonnage: list[GradeTonnage]
    modified_at: str

    @staticmethod
    def from_kgrel(dms: DedupMineralSite, commodity: Optional[InternalID]):
        loc = DedupMineralSiteLocation(
            country=dms.country.value, state_or_province=dms.state_or_province.value
        )
        if dms.coordinates is not None:
            loc.lat = dms.coordinates.value.lat
            loc.lon = dms.coordinates.value.lon

        if loc.is_empty():
            loc = None

        return DedupMineralSitePublic(
            id=dms.id,
            name=dms.name.value if dms.name is not None else "",
            type=dms.type.value if dms.type is not None else "NotSpecified",
            rank=dms.rank.value if dms.rank is not None else "U",
            location=loc,
            deposit_types=[
                DedupMineralSiteDepositType(
                    id=dt.id, source=dt.source, confidence=dt.confidence
                )
                for dt in dms.deposit_types
            ],
            grade_tonnage=[
                GradeTonnage(
                    commodity=inv.commodity,
                    total_contained_metal=inv.contained_metal,
                    total_tonnage=inv.tonnage,
                    total_grade=inv.grade,
                    date=inv.date,
                )
                for inv in dms.inventory_views
            ],
            sites=[
                DedupMineralSiteIdAndScore(
                    id=site.site_id, score=DedupMineralSite.get_site_score(site)[0]
                )
                for site in dms.sites
            ],
            modified_at=dms.modified_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        )
