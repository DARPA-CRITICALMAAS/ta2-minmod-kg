from __future__ import annotations

from dataclasses import dataclass, field
from functools import cached_property
from typing import Optional

from minmodkg.misc.utils import format_nanoseconds, makedict
from minmodkg.models.kg.base import NS_MD
from minmodkg.models.kgrel.dedup_mineral_site import DedupMineralSiteAndInventory
from minmodkg.typing import InternalID


@dataclass
class DedupMineralSiteDepositType:
    id: InternalID
    source: str
    confidence: float

    def to_dict(self):
        return {
            "id": self.id,
            "source": self.source,
            "confidence": self.confidence,
        }

    @classmethod
    def from_dict(cls, d: dict):
        return cls(id=d["id"], source=d["source"], confidence=d["confidence"])


@dataclass
class DedupMineralSiteLocation:
    lat: Optional[float] = None
    lon: Optional[float] = None
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
        return makedict.without_none(
            (
                ("lat", self.lat),
                ("lon", self.lon),
                ("country", self.country),
                ("state_or_province", self.state_or_province),
            )
        )

    @classmethod
    def from_dict(cls, d: dict):
        return cls(
            lat=d.get("lat"),
            lon=d.get("lon"),
            country=d["country"],
            state_or_province=d["state_or_province"],
        )


@dataclass
class DedupMineralSiteIdAndScore:
    id: str
    score: float

    def to_dict(self):
        return {"id": self.id, "score": self.score}

    @classmethod
    def from_dict(cls, d: dict):
        return cls(id=d["id"], score=d["score"])


@dataclass
class GradeTonnage:
    commodity: InternalID
    total_contained_metal: Optional[float] = None
    total_tonnage: Optional[float] = None
    total_grade: Optional[float] = None
    date: Optional[str] = None

    def to_dict(self):
        return makedict.without_none(
            (
                ("commodity", self.commodity),
                ("total_contained_metal", self.total_contained_metal),
                ("total_tonnage", self.total_tonnage),
                ("total_grade", self.total_grade),
                ("date", self.date),
            )
        )

    @classmethod
    def from_dict(cls, d: dict):
        return cls(
            commodity=d["commodity"],
            total_contained_metal=d.get("total_contained_metal"),
            total_tonnage=d.get("total_tonnage"),
            total_grade=d.get("total_grade"),
            date=d.get("date"),
        )


@dataclass
class DedupMineralSitePublic:
    id: InternalID
    name: str
    type: str
    rank: str
    sites: list[DedupMineralSiteIdAndScore]
    deposit_types: list[DedupMineralSiteDepositType]
    location: Optional[DedupMineralSiteLocation]
    grade_tonnage: list[GradeTonnage]
    modified_at: str

    @cached_property
    def uri(self):
        return NS_MD.uri(self.id)

    @staticmethod
    def from_kgrel(dmsi: DedupMineralSiteAndInventory, commodity: Optional[InternalID]):
        dms = dmsi.dms
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
                for dt in dms.ranked_deposit_types
            ],
            grade_tonnage=[
                GradeTonnage(
                    commodity=inv.commodity,
                    total_contained_metal=inv.contained_metal,
                    total_tonnage=inv.tonnage,
                    total_grade=inv.grade,
                    date=inv.date,
                )
                for inv in dmsi.invs
            ],
            sites=[
                DedupMineralSiteIdAndScore(id=site.site_id, score=site.score.score)
                for site in dms.ranked_sites
            ],
            modified_at=format_nanoseconds(dms.modified_at),
        )

    def to_dict(self):
        out = {
            "id": self.id,
            "name": self.name,
            "type": self.type,
            "rank": self.rank,
            "sites": [s.to_dict() for s in self.sites],
            "deposit_types": [dt.to_dict() for dt in self.deposit_types],
            "grade_tonnage": [gt.to_dict() for gt in self.grade_tonnage],
            "modified_at": self.modified_at,
        }
        if self.location is not None:
            out["location"] = self.location.to_dict()
        return out

    @classmethod
    def from_dict(cls, d: dict):
        return cls(
            id=d["id"],
            name=d["name"],
            type=d["type"],
            rank=d["rank"],
            sites=[DedupMineralSiteIdAndScore.from_dict(s) for s in d["sites"]],
            deposit_types=[
                DedupMineralSiteDepositType.from_dict(dt) for dt in d["deposit_types"]
            ],
            location=(
                DedupMineralSiteLocation.from_dict(d["location"])
                if "location" in d
                else None
            ),
            grade_tonnage=[GradeTonnage.from_dict(gt) for gt in d["grade_tonnage"]],
            modified_at=d["modified_at"],
        )
