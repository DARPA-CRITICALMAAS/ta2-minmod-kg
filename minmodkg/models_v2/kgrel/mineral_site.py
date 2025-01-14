from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Annotated, Iterable, Optional

import minmodkg.models.candidate_entity
import minmodkg.models.mineral_inventory
import minmodkg.models.reference
from minmodkg.grade_tonnage_model import GradeTonnageModel
from minmodkg.misc.utils import datetime_to_nanoseconds, makedict
from minmodkg.models.base import MINMOD_NS
from minmodkg.models_v2.inputs.candidate_entity import CandidateEntity
from minmodkg.models_v2.inputs.measure import Measure
from minmodkg.models_v2.inputs.mineral_inventory import MineralInventory
from minmodkg.models_v2.inputs.mineral_site import MineralSite as InMineralSite
from minmodkg.models_v2.inputs.reference import (
    BoundingBox,
    Document,
    PageInfo,
    Reference,
)
from minmodkg.models_v2.kgrel.base import Base
from minmodkg.models_v2.kgrel.custom_types import Location, LocationView
from minmodkg.models_v2.kgrel.views.mineral_inventory_view import MineralInventoryView
from minmodkg.transformations import get_source_uri
from minmodkg.typing import IRI, URN, InternalID
from sqlalchemy import JSON, BigInteger, ForeignKey
from sqlalchemy.orm import (
    Mapped,
    MappedAsDataclass,
    mapped_column,
    reconstructor,
    relationship,
)

if TYPE_CHECKING:
    from minmodkg.models_v2.kgrel.dedup_mineral_site import DedupMineralSite


@dataclass
class MineralSiteAndInventory:
    ms: MineralSite
    invs: list[MineralInventoryView]

    def set_id(self, ms_id: int) -> MineralSiteAndInventory:
        self.ms.id = ms_id
        for inv in self.invs:
            inv.site_id = ms_id
        return self

    def to_dict(self):
        return makedict.without_none_or_empty_list(
            (("ms", self.ms.to_dict()), ("invs", [inv.to_dict() for inv in self.invs]))
        )

    @classmethod
    def from_dict(cls, d: dict):
        return cls(
            ms=MineralSite.from_dict(d["ms"]),
            invs=[MineralInventoryView.from_dict(inv) for inv in d.get("invs", [])],
        )

    @staticmethod
    def from_raw_site(
        raw_site: dict | InMineralSite,
        material_form: dict[str, float],
        crs_names: dict[str, str],
        source_score: dict[IRI, float],
    ) -> MineralSiteAndInventory:
        ms = MineralSite.from_raw_site(raw_site, crs_names, source_score)

        invs: dict[InternalID, list[GradeTonnageModel.MineralInventory]] = defaultdict(
            list
        )
        grade_tonnage_model = GradeTonnageModel()
        commodities = set()

        for inv_id, inv in enumerate(ms.inventories):
            if inv.commodity.normalized_uri is None:
                continue

            commodity = MINMOD_NS.mr.id(inv.commodity.normalized_uri)
            commodities.add(commodity)

            if (
                inv.ore is None
                or inv.ore.value is None
                or inv.ore.unit is None
                or inv.ore.unit.normalized_uri is None
                or inv.grade is None
                or inv.grade.value is None
                or inv.grade.unit is None
                or inv.grade.unit.normalized_uri is None
                or len(inv.category) == 0
            ):
                continue

            mi_form_conversion = None
            if (
                inv.material_form is not None
                and inv.material_form.normalized_uri is not None
            ):
                mi_form_conversion = material_form[inv.material_form.normalized_uri]

            invs[commodity].append(
                GradeTonnageModel.MineralInventory(
                    id=str(inv_id),
                    date=inv.date,
                    zone=inv.zone,
                    category=[
                        cat.normalized_uri
                        for cat in inv.category
                        if cat.normalized_uri is not None
                    ],
                    material_form_conversion=mi_form_conversion,
                    ore_value=inv.ore.value,
                    ore_unit=inv.ore.unit.normalized_uri,
                    grade_value=inv.grade.value,
                    grade_unit=inv.grade.unit.normalized_uri,
                )
            )

        inv_views = []
        for commodity, gt_invs in invs.items():
            grade_tonnage = grade_tonnage_model(gt_invs)

            if grade_tonnage is not None and grade_tonnage.total_estimate is not None:
                total_contained_metal = grade_tonnage.total_estimate.contained_metal
                total_tonnage = grade_tonnage.total_estimate.tonnage
                total_grade = grade_tonnage.total_estimate.get_grade()
            else:
                total_contained_metal = None
                total_tonnage = None
                total_grade = None

            inv_views.append(
                MineralInventoryView(
                    commodity=commodity,
                    contained_metal=total_contained_metal,
                    tonnage=total_tonnage,
                    grade=total_grade,
                    date=None,
                )
            )
        for comm in commodities:
            if comm not in invs:
                inv_views.append(
                    MineralInventoryView(
                        commodity=comm,
                        contained_metal=None,
                        tonnage=None,
                        grade=None,
                        date=None,
                    )
                )

        return MineralSiteAndInventory(ms, inv_views)


class MineralSite(MappedAsDataclass, Base):
    __tablename__ = "mineral_site"

    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    site_id: Mapped[InternalID] = mapped_column(unique=True)
    dedup_site_id: Mapped[
        Annotated[InternalID, "Id of the mineral site that this site is the same as"]
    ] = mapped_column(
        ForeignKey("dedup_mineral_site.id", ondelete="SET NULL"), index=True
    )
    source_id: Mapped[URN] = mapped_column()
    source_score: Mapped[Optional[float]] = mapped_column()
    record_id: Mapped[str] = mapped_column()
    name: Mapped[str | None] = mapped_column()
    aliases: Mapped[list[str]] = mapped_column(JSON)
    rank: Mapped[str | None] = mapped_column()
    type: Mapped[str | None] = mapped_column()

    location: Mapped[Location | None] = mapped_column()
    location_view: Mapped[LocationView] = mapped_column()
    deposit_type_candidates: Mapped[list[CandidateEntity]] = mapped_column()
    inventories: Mapped[list[MineralInventory]] = mapped_column()
    reference: Mapped[list[Reference]] = mapped_column()

    created_by: Mapped[list[IRI]] = mapped_column(JSON)
    # timestamp in nano seconds
    modified_at: Mapped[int] = mapped_column(BigInteger)

    def set_id(self, id: int) -> MineralSite:
        self.id = id
        return self

    @staticmethod
    def from_raw_site(
        raw_site: dict | InMineralSite,
        crs_names: dict[str, str],
        source_score: dict[IRI, float],
    ) -> MineralSite:
        site = (
            InMineralSite.from_dict(raw_site)
            if isinstance(raw_site, dict)
            else raw_site
        )
        location = None
        location_view = LocationView()

        if site.location_info is not None:
            location = Location(
                country=site.location_info.country,
                state_or_province=site.location_info.state_or_province,
                crs=site.location_info.crs,
                coordinates=site.location_info.location,
            )
            location_view = LocationView.from_location(location, crs_names)

        out_site = MineralSite(
            site_id=site.id,
            dedup_site_id=MineralSite.get_dedup_id((site.id,)),
            source_id=site.source_id,
            source_score=source_score.get(get_source_uri(site.source_id)),
            record_id=str(site.record_id),
            name=site.name,
            aliases=site.aliases,
            rank=site.site_rank,
            type=site.site_type,
            location=location,
            location_view=location_view,
            deposit_type_candidates=site.deposit_type_candidate,
            inventories=site.mineral_inventory,
            reference=site.reference,
            created_by=site.created_by,
            modified_at=datetime_to_nanoseconds(
                datetime.fromisoformat(site.modified_at)
            ),
        )
        return out_site

    def to_dict(self):
        return makedict.without_none_or_empty_list(
            (
                ("id", self.id),
                ("site_id", self.site_id),
                ("dedup_site_id", self.dedup_site_id),
                ("source_id", self.source_id),
                ("source_score", self.source_score),
                ("record_id", self.record_id),
                ("name", self.name),
                ("aliases", self.aliases),
                ("rank", self.rank),
                ("type", self.type),
                (
                    "location",
                    (self.location.to_dict() if self.location is not None else None),
                ),
                (
                    "location_view",
                    (
                        self.location_view.to_dict()
                        if not self.location_view.is_empty()
                        else None
                    ),
                ),
                (
                    "deposit_type_candidates",
                    [x.to_dict() for x in self.deposit_type_candidates],
                ),
                ("reference", [x.to_dict() for x in self.reference]),
                ("created_by", self.created_by),
                ("modified_at", self.modified_at),
            )
        )

    @classmethod
    def from_dict(cls, d):
        obj = cls(
            site_id=d["site_id"],
            dedup_site_id=d["dedup_site_id"],
            source_id=d["source_id"],
            source_score=d.get("source_score"),
            record_id=d["record_id"],
            name=d.get("name"),
            aliases=d.get("aliases", []),
            rank=d.get("rank"),
            type=d.get("type"),
            location=Location.from_dict(d["location"]) if d.get("location") else None,
            location_view=LocationView.from_dict(d.get("location_view", {})),
            deposit_type_candidates=[
                CandidateEntity.from_dict(x)
                for x in d.get("deposit_type_candidates", [])
            ],
            inventories=[
                MineralInventory.from_dict(x) for x in d.get("inventories", [])
            ],
            reference=[Reference.from_dict(x) for x in d.get("reference", [])],
            created_by=d["created_by"],
            modified_at=d["modified_at"],
        )
        if "id" in d:
            obj.id = d["id"]
        return obj

    @staticmethod
    def get_dedup_id(site_ids: Iterable[InternalID]):
        return "dedup_" + min(site_ids)


class DataTranslation:

    @classmethod
    def convert_reference(cls, ref: minmodkg.models.reference.Reference) -> Reference:
        return Reference(
            document=Document(
                doi=ref.document.doi,
                uri=ref.document.uri,
                title=ref.document.title,
            ),
            page_info=[
                PageInfo(
                    page=pi.page,
                    bounding_box=(
                        cls.convert_bounding_box(pi.bounding_box)
                        if pi.bounding_box is not None
                        else None
                    ),
                )
                for pi in ref.page_info
            ],
            comment=ref.comment,
            property=ref.property,
        )

    @classmethod
    def convert_bounding_box(
        cls, bb: minmodkg.models.reference.BoundingBox
    ) -> BoundingBox:
        return BoundingBox(
            x_max=bb.x_max,
            x_min=bb.x_min,
            y_max=bb.y_max,
            y_min=bb.y_min,
        )

    @classmethod
    def convert_candidate_entity(
        cls,
        ent: minmodkg.models.candidate_entity.CandidateEntity,
    ) -> CandidateEntity:
        if ent is None:
            return None
        return CandidateEntity(
            source=ent.source,
            confidence=ent.confidence,
            observed_name=ent.observed_name,
            normalized_uri=ent.normalized_uri,
        )

    @classmethod
    def convert_measure(
        cls,
        measure: minmodkg.models.mineral_inventory.Measure,
    ) -> Measure:
        if measure is None:
            return None
        return Measure(
            value=measure.value,
            unit=(
                cls.convert_candidate_entity(measure.unit)
                if measure.unit is not None
                else None
            ),
        )
