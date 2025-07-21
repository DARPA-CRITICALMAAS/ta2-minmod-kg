from __future__ import annotations

from dataclasses import dataclass, field
from functools import cached_property
from typing import Optional

from minmodkg.misc.utils import format_nanoseconds, makedict
from minmodkg.models.kg.base import NS_MD
from minmodkg.models.kg.geology_info import GeologyInfo
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
    mineral_form: list[str]
    geology_info: Optional[GeologyInfo]
    discovered_year: Optional[int]
    modified_at: str
    trace: dict[str, str] = field(default_factory=dict)

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

        trace = {}
        if dms.name is not None:
            trace["name"] = dms.name.refid
        if dms.type is not None:
            trace["type"] = dms.type.refid
        if dms.rank is not None:
            trace["rank"] = dms.rank.refid
        if dms.coordinates is not None:
            trace["coordinates"] = dms.coordinates.refid
        if len(dms.country.value) > 0:
            trace["country"] = dms.country.refid
        if len(dms.state_or_province.value) > 0:
            trace["state_or_province"] = dms.state_or_province.refid
        if len(dms.ranked_deposit_types) > 0:
            trace["deposit_types"] = [dt.refid for dt in dms.ranked_deposit_types]

        trace["grade_tonnage"] = [
            {"commodity": inv.commodity, "site_id": inv.site_id} for inv in dmsi.invs
        ]
        if len(dms.mineral_form.value) > 0:
            trace["mineral_form"] = dms.mineral_form.refid
        if not dms.geology_info.is_empty():
            trace["geology_info"] = {}
            if dms.geology_info.alteration is not None:
                trace["geology_info"]["alteration"] = dms.geology_info.alteration.refid
            if dms.geology_info.concentration_process is not None:
                trace["geology_info"][
                    "concentration_process"
                ] = dms.geology_info.concentration_process.refid
            if dms.geology_info.ore_control is not None:
                trace["geology_info"][
                    "ore_control"
                ] = dms.geology_info.ore_control.refid
            if dms.geology_info.host_rock is not None:
                trace["geology_info"]["host_rock"] = {
                    "unit": (
                        dms.geology_info.host_rock.unit.refid
                        if dms.geology_info.host_rock.unit is not None
                        else None
                    ),
                    "type": (
                        dms.geology_info.host_rock.type.refid
                        if dms.geology_info.host_rock.type is not None
                        else None
                    ),
                }
            if dms.geology_info.associated_rock is not None:
                trace["geology_info"]["associated_rock"] = {
                    "unit": (
                        dms.geology_info.associated_rock.unit.refid
                        if dms.geology_info.associated_rock.unit is not None
                        else None
                    ),
                    "type": (
                        dms.geology_info.associated_rock.type.refid
                        if dms.geology_info.associated_rock.type is not None
                        else None
                    ),
                }
            if dms.geology_info.structure is not None:
                trace["geology_info"]["structure"] = dms.geology_info.structure.refid
            if dms.geology_info.tectonic is not None:
                trace["geology_info"]["tectonic"] = dms.geology_info.tectonic.refid
        if dms.discovered_year is not None:
            trace["discovered_year"] = dms.discovered_year.refid
        return DedupMineralSitePublic(
            id=dms.id,
            name=dms.name.value if dms.name is not None else "",
            type=dms.type.value if dms.type is not None else "NotSpecified",
            rank=dms.rank.value if dms.rank is not None else "U",
            location=loc,
            deposit_types=[
                DedupMineralSiteDepositType(
                    id=dt.value.id,
                    source=dt.value.source,
                    confidence=dt.value.confidence,
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
            mineral_form=dms.mineral_form.value,
            geology_info=dms.geology_info.to_geology_info(),
            discovered_year=(
                dms.discovered_year.value if dms.discovered_year is not None else None
            ),
            modified_at=format_nanoseconds(dms.modified_at),
            trace=trace,
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
            "mineral_form": self.mineral_form,
            "discovered_year": self.discovered_year,
            "modified_at": self.modified_at,
            "trace": self.trace,
        }
        if self.geology_info is not None:
            out["geology_info"] = self.geology_info.to_dict()
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
            mineral_form=d["mineral_form"],
            geology_info=(
                GeologyInfo.from_dict(d["geology_info"])
                if "geology_info" in d
                else None
            ),
            discovered_year=d["discovered_year"],
            modified_at=d["modified_at"],
            trace=d["trace"],
        )
