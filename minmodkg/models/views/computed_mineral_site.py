from __future__ import annotations

import re
from collections import defaultdict
from typing import Iterable, Optional

import shapely.wkt
from minmodkg.grade_tonnage_model import GradeTonnageModel
from minmodkg.misc.geo import reproject_wkt
from minmodkg.misc.utils import assert_not_none, exclude_none_or_empty_list
from minmodkg.models.dedup_mineral_site import DedupMineralSite
from minmodkg.models.mineral_site import MineralSite
from minmodkg.models.views.base import Base
from minmodkg.models.views.custom_types import ComputedLocation
from minmodkg.typing import InternalID
from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, MappedAsDataclass, mapped_column, relationship


class ComputedGradeTonnage(MappedAsDataclass, Base):
    __tablename__ = "computed_grade_tonnage"

    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    commodity: Mapped[str] = mapped_column(String(30), index=True)
    contained_metal: Mapped[Optional[float]]
    tonnage: Mapped[Optional[float]]
    grade: Mapped[Optional[float]]
    date: Mapped[Optional[str]]

    site_id: Mapped[int] = mapped_column(
        ForeignKey("computed_mineral_site.id"), default=None
    )
    site: Mapped[ComputedMineralSite] = relationship(
        default=None, back_populates="grade_tonnages", lazy="raise_on_sql"
    )

    def set_id(self, id: Optional[int]):
        if id is not None:
            self.id = id
        return self


class ComputedMineralSite(MappedAsDataclass, Base):
    __tablename__ = "computed_mineral_site"

    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    snapshot_id: Mapped[str] = mapped_column(String(40))
    snapshot_timestamp: Mapped[float]
    is_updated: Mapped[bool] = mapped_column()
    site_id: Mapped[InternalID] = mapped_column(index=True)
    dedup_id: Mapped[InternalID]
    location: Mapped[Optional[ComputedLocation]]
    grade_tonnages: Mapped[list[ComputedGradeTonnage]] = relationship(
        init=False, back_populates="site", lazy="raise_on_sql"
    )

    def set_id(self, id: Optional[int]):
        if id is not None:
            self.id = id
        return self

    def to_dict(self):
        return exclude_none_or_empty_list(
            {
                "id": self.id,
                "site_id": self.site_id,
                "dedup_id": self.dedup_id,
                "snapshot_id": self.snapshot_id,
                "snapshot_timestamp": self.snapshot_timestamp,
                "is_updated": self.is_updated,
                "location": (
                    self.location.to_dict() if self.location is not None else None
                ),
                "grade_tonnages": [
                    exclude_none_or_empty_list(
                        {
                            "id": gt.id,
                            "commodity": gt.commodity,
                            "contained_metal": gt.contained_metal,
                            "tonnage": gt.tonnage,
                            "grade": gt.grade,
                            "date": gt.date,
                        }
                    )
                    for gt in self.grade_tonnages
                ],
            }
        )

    @staticmethod
    def from_dict(obj: dict):
        this = ComputedMineralSite(
            site_id=obj["site_id"],
            dedup_id=obj["dedup_id"],
            snapshot_id=obj["snapshot_id"],
            snapshot_timestamp=obj["snapshot_timestamp"],
            is_updated=obj["is_updated"],
            location=(
                ComputedLocation(**obj["location"])
                if obj.get("location") is not None
                else None
            ),
        ).set_id(obj.get("id"))

        this.grade_tonnages = [
            ComputedGradeTonnage(
                commodity=gt["commodity"],
                contained_metal=gt.get("contained_metal"),
                tonnage=gt.get("tonnage"),
                grade=gt.get("grade"),
                date=gt.get("date"),
                site=this,
            ).set_id(gt.get("id"))
            for gt in obj.get("grade_tonnages", [])
        ]

        return this

    @staticmethod
    def get_dedup_id(site_ids: Iterable[InternalID]):
        return "dedup_" + min(site_ids)

    @classmethod
    def from_mineral_site(
        cls,
        site: MineralSite,
        material_form: dict[str, float],
        crss: dict[str, str],
    ):
        site_ns = MineralSite.qbuilder.class_namespace
        location = ComputedLocation()
        if site.location_info is not None:
            if site.location_info.location is not None:
                if (
                    site.location_info.crs is None
                    or site.location_info.crs.normalized_uri is None
                ):
                    crs = "EPSG:4326"
                else:
                    crs = crss[site.location_info.crs.normalized_uri]

                # TODO: fix this nan
                if "nan" in site.location_info.location.lower():
                    centroid = None
                else:
                    try:
                        geometry = shapely.wkt.loads(site.location_info.location)
                        centroid = shapely.wkt.dumps(shapely.centroid(geometry))
                        centroid = reproject_wkt(centroid, crs, "EPSG:4326")
                    except shapely.errors.GEOSException:
                        centroid = None

                if centroid is not None:
                    m = re.match(
                        r"POINT \(([+-]?(?:[0-9]*[.])?[0-9]+) ([+-]?(?:[0-9]*[.])?[0-9]+)\)",
                        centroid,
                    )
                    assert m is not None, (centroid, site.source_id, site.record_id)
                    location.lat = float(m.group(2))
                    location.lon = float(m.group(1))
            location.country = [
                site_ns.id(ent.normalized_uri)
                for ent in site.location_info.country
                if ent.normalized_uri is not None
            ]
            location.state_or_province = [
                site_ns.id(ent.normalized_uri)
                for ent in site.location_info.state_or_province
                if ent.normalized_uri is not None
            ]

        if location.is_empty():
            location = None

        invs: dict[InternalID, list] = defaultdict(list)
        grade_tonnage_model = GradeTonnageModel()
        commodities = set()

        for inv_id, inv in enumerate(site.mineral_inventory):
            if inv.commodity.normalized_uri is None:
                continue

            commodity = site_ns.id(inv.commodity.normalized_uri)
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

        site_id = site_ns.id(site.uri)
        output = ComputedMineralSite(
            site_id=site_id,
            dedup_id=(
                DedupMineralSite.qbuilder.class_namespace.id(site.dedup_site_uri)
                if site.dedup_site_uri is not None
                else cls.get_dedup_id((site_id,))
            ),
            location=location,
            snapshot_id=assert_not_none(site.snapshot_id),
            snapshot_timestamp=site.get_modified_timestamp(),
            is_updated=True,
        )

        site_comms = []
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

            site_comms.append(
                ComputedGradeTonnage(
                    commodity=commodity,
                    contained_metal=total_contained_metal,
                    tonnage=total_tonnage,
                    grade=total_grade,
                    date=None,
                    site=output,
                )
            )
        for comm in commodities:
            if comm not in invs:
                site_comms.append(
                    ComputedGradeTonnage(
                        commodity=comm,
                        contained_metal=None,
                        tonnage=None,
                        grade=None,
                        date=None,
                        site=output,
                    )
                )

        output.grade_tonnages = site_comms
        return output

    def merge(self, other: ComputedMineralSite):
        """Merge two derived mineral sites together.

        For location, we shouldn't have two different locations of the same records
        as each team is not supposed to work on separate records or separate infomration.
        """
        if self.location is not None:
            self.location.combine(other.location)
        else:
            self.location = other.location

        com2idx = {gt.commodity: idx for idx, gt in enumerate(self.grade_tonnages)}
        for gt in other.grade_tonnages:
            if gt.commodity not in com2idx:
                self.grade_tonnages.append(gt)
            elif gt.contained_metal is not None:
                mgt = self.grade_tonnages[com2idx[gt.commodity]]
                if (
                    mgt.contained_metal is None
                    or gt.contained_metal > mgt.contained_metal
                ):
                    self.grade_tonnages[com2idx[gt.commodity]] = gt
