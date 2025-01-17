from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from functools import cached_property, cmp_to_key, total_ordering
from typing import Iterable, Literal, Optional

from minmodkg.misc import UnconvertibleUnitError, group_by_attr
from minmodkg.models.kg.base import MINMOD_NS

MR_NS = MINMOD_NS.mr
Mt_unit = MR_NS.uristr("Q202")
percent_unit = MR_NS.uristr("Q201")


class OtherCategory(str, Enum):
    """Other reported category of a grade/tonnage"""

    OriginalResource = MR_NS.uristr("OriginalResource")
    Extracted = MR_NS.uristr("Extracted")
    CumulativeExtracted = MR_NS.uristr("CumulativeExtracted")


class ResourceCategory(str, Enum):
    """Resource category of a mineral site"""

    Inferred = MR_NS.uristr("Inferred")
    Indicated = MR_NS.uristr("Indicated")
    Measured = MR_NS.uristr("Measured")


class ReserveCategory(str, Enum):
    """Reserve category of a mineral site"""

    Proven = MR_NS.uristr("Proven")
    Probable = MR_NS.uristr("Probable")


@total_ordering
@dataclass(frozen=True)
class GradeTonnageEstimate:
    tonnage: float
    contained_metal: float

    def is_equal_or_better(self, other: GradeTonnageEstimate):
        return self.contained_metal >= other.contained_metal

    def get_grade(self, unit: Optional[str] = None) -> float:
        if unit is None:
            unit = percent_unit

        if self.contained_metal == 0.0:
            return 0.0

        return unit_conversion(
            self.contained_metal / self.tonnage * 100,
            percent_unit,
            unit,
        )

    def __add__(self, other: GradeTonnageEstimate) -> GradeTonnageEstimate:
        return GradeTonnageEstimate(
            self.tonnage + other.tonnage, self.contained_metal + other.contained_metal
        )

    def __eq__(self, value: Optional[GradeTonnageEstimate]) -> bool:
        if value is None:
            return False
        return (
            self.contained_metal == value.contained_metal
            and self.tonnage == value.tonnage
        )

    def approx_eq(
        self, value: Optional[GradeTonnageEstimate], tol: float = 1e-7
    ) -> bool:
        if value is None:
            return False
        return (
            abs(self.contained_metal - value.contained_metal) < tol
            and abs(self.tonnage - value.tonnage) < tol
        )

    def __lt__(self, value: Optional[GradeTonnageEstimate]) -> bool:
        if value is None:
            return False
        if self.contained_metal == value.contained_metal:
            return self.tonnage < value.tonnage
        return self.contained_metal < value.contained_metal

    @staticmethod
    def max(
        a: Optional[GradeTonnageEstimate], b: Optional[GradeTonnageEstimate]
    ) -> Optional[GradeTonnageEstimate]:
        if a is None:
            return b
        if b is None:
            return a
        return max(a, b)


@dataclass(frozen=True)
class SiteGradeTonnage:
    resource_estimate: Optional[GradeTonnageEstimate]
    reserve_estimate: Optional[GradeTonnageEstimate]
    original_estimate: Optional[GradeTonnageEstimate]
    # the extracted estimate only applies to the reported date/year (e.g., 2020) so to get the cumulative value
    # we need to sum up the extracted estimate of all date, i.e., we need to sum all site grade tonnages.
    extracted_estimate: Optional[GradeTonnageEstimate]
    # either the reported cumulative extracted estimate or the sum of all extracted estimates, depends on whether
    # the cumulative extracted estimate is reported or not -- it will be all extracted minerals including the current date
    cumulative_extracted_estimate: Optional[GradeTonnageEstimate]
    date: Optional[str]

    @cached_property
    def total_estimate(self) -> Optional[GradeTonnageEstimate]:
        "Do not use extracted estimate as we assume the cumulative extracted estimate is updated"
        remained_estimate = None

        if self.resource_estimate is not None:
            remained_estimate = self.resource_estimate
        if self.reserve_estimate is not None:
            remained_estimate = GradeTonnageEstimate.max(
                remained_estimate, self.reserve_estimate
            )

        if (
            self.cumulative_extracted_estimate is not None
            and remained_estimate is not None
        ):
            remained_estimate = remained_estimate + self.cumulative_extracted_estimate

        return GradeTonnageEstimate.max(remained_estimate, self.original_estimate)

    def add(
        self,
        other: SiteGradeTonnage,
        *,
        handle_original_estimate: Literal["add", "max"],
    ):
        assert self.date == other.date, "Cannot add data of two different dates"

        resource_estimate = self.resource_estimate
        reserve_estimate = self.reserve_estimate
        original_estimate = self.original_estimate
        extracted_estimate = self.extracted_estimate
        cumulative_extracted_estimate = self.cumulative_extracted_estimate

        if resource_estimate is not None:
            if other.resource_estimate is not None:
                resource_estimate = resource_estimate + other.resource_estimate
        elif other.resource_estimate is not None:
            resource_estimate = other.resource_estimate

        if reserve_estimate is not None:
            if other.reserve_estimate is not None:
                reserve_estimate = reserve_estimate + other.reserve_estimate
        elif other.reserve_estimate is not None:
            reserve_estimate = other.reserve_estimate

        if original_estimate is not None:
            if other.original_estimate is not None:
                if handle_original_estimate == "add":
                    original_estimate = original_estimate + other.original_estimate
                elif handle_original_estimate == "max":
                    original_estimate = max(original_estimate, other.original_estimate)
                else:
                    raise ValueError(
                        f"Invalid value for handle_original_estimate: {handle_original_estimate}"
                    )
        elif other.original_estimate is not None:
            original_estimate = other.original_estimate

        if extracted_estimate is not None:
            if other.extracted_estimate is not None:
                extracted_estimate = extracted_estimate + other.extracted_estimate
        elif other.extracted_estimate is not None:
            extracted_estimate = other.extracted_estimate

        if cumulative_extracted_estimate is not None:
            if other.cumulative_extracted_estimate is not None:
                cumulative_extracted_estimate = (
                    cumulative_extracted_estimate + other.cumulative_extracted_estimate
                )

        elif other.cumulative_extracted_estimate is not None:
            cumulative_extracted_estimate = other.cumulative_extracted_estimate

        return SiteGradeTonnage(
            resource_estimate=resource_estimate,
            reserve_estimate=reserve_estimate,
            original_estimate=original_estimate,
            extracted_estimate=extracted_estimate,
            cumulative_extracted_estimate=cumulative_extracted_estimate,
            date=self.date,
        )

    def max(self, other: SiteGradeTonnage):
        assert self.date == other.date, "Cannot compare data of two different dates"
        return SiteGradeTonnage(
            resource_estimate=GradeTonnageEstimate.max(
                self.resource_estimate,
                other.resource_estimate,
            ),
            reserve_estimate=GradeTonnageEstimate.max(
                self.reserve_estimate, other.reserve_estimate
            ),
            original_estimate=GradeTonnageEstimate.max(
                self.original_estimate,
                other.original_estimate,
            ),
            extracted_estimate=GradeTonnageEstimate.max(
                self.extracted_estimate,
                other.extracted_estimate,
            ),
            cumulative_extracted_estimate=GradeTonnageEstimate.max(
                self.cumulative_extracted_estimate,
                other.cumulative_extracted_estimate,
            ),
            date=self.date,
        )


class GradeTonnageModel:
    """Computing grade & tonnage data of a single mineral site from mineral inventories"""

    VERSION = 101

    @dataclass
    class MineralInventory:
        id: str  # unique identifier of the inventory -- for knowing the reported data is sum of multiple categories or not
        date: Optional[
            str
        ]  # %YYYY-%MM-%DD: this allow us to group by and sort by date without parsing it
        zone: Optional[str | int]
        category: list[str]
        # if available, is a factor that helps to convert data from the material form to the original form
        material_form_conversion: Optional[float]
        ore_value: float
        ore_unit: str
        grade_value: float
        grade_unit: str

    def __call__(
        self,
        invs: list[MineralInventory],
        norm_tonnage_unit: Optional[str] = None,
        norm_grade_unit: Optional[str] = None,
    ) -> Optional[SiteGradeTonnage]:
        if norm_tonnage_unit is None:
            norm_tonnage_unit = Mt_unit
        if norm_grade_unit is None:
            norm_grade_unit = percent_unit

        other_cat = frozenset({c.value for c in OtherCategory})
        resource_cat = frozenset({c.value for c in ResourceCategory})
        reserve_cat = frozenset({c.value for c in ReserveCategory})

        # group by zone & date
        grade_tonnages = []
        for date, invs_by_date in group_by_attr(invs, "date").items():
            grade_tonnage_per_zones: dict[Optional[str], SiteGradeTonnage] = {}
            for zone, invs_by_date_zone in group_by_attr(invs_by_date, "zone").items():
                # the extraction may went wrong and we have multiple results per category
                # therefore, we need to handle them by choosing the best one among them.
                # the order is provided by the `GradeTonnageEstimate.is_equal_or_better` function

                # the first step is normalization
                cat2ests: dict[frozenset[str], list[GradeTonnageEstimate]] = (
                    defaultdict(list)
                )
                for inv in invs_by_date_zone:
                    try:
                        ore = unit_conversion(
                            inv.ore_value, inv.ore_unit, norm_tonnage_unit
                        )
                        grade = unit_conversion(
                            inv.grade_value, inv.grade_unit, norm_grade_unit
                        )
                    except UnconvertibleUnitError as e:
                        # the data is broken, so we skip it
                        continue

                    # if they report X tonnes of Y percentage grade for Li2O, then they should have X * Y of Li2O, then
                    # X * Y * 0.464 of Li.
                    if inv.material_form_conversion is not None:
                        ore *= inv.material_form_conversion

                    cat = frozenset(inv.category)
                    if not (
                        cat.issubset(resource_cat)
                        or cat.issubset(reserve_cat)
                        or (len(inv.category) == 1 and inv.category[0] in other_cat)
                    ):
                        # ignore errorneous data
                        continue

                    norm_grade = unit_conversion(grade, norm_grade_unit, percent_unit)
                    if ore < 0.0 or norm_grade < 0.0:
                        # ignore errorneous data -- allow 0.0 grade or tonnage
                        continue

                    cat2ests[frozenset(inv.category)].append(
                        GradeTonnageEstimate(
                            tonnage=ore,
                            contained_metal=ore * norm_grade / 100,
                        )
                    )

                cat_est = [
                    (
                        cat,
                        max(
                            ests,
                            key=cmp_to_key(GradeTonnageEstimate.is_equal_or_better),
                        ),
                    )
                    for cat, ests in cat2ests.items()
                ]

                if len(cat_est) == 0:
                    # no data for this zone
                    continue

                # now, we need to compute resource/reserve estimates by summing up the estimate
                resource_est = [x for x in cat_est if x[0].issubset(resource_cat)]
                reserve_est = [x for x in cat_est if x[0].issubset(reserve_cat)]
                other_est = [x for x in cat_est if x[0].issubset(other_cat)]

                attr2est: dict[str, Optional[GradeTonnageEstimate]] = {
                    "resource": None,
                    "reserve": None,
                    "original": None,
                    "extracted": None,
                    "cumulative_extracted": None,
                }
                for attr, ests in [
                    ("resource", resource_est),
                    ("reserve", reserve_est),
                ]:
                    allcats = {cat for cat, _ in ests}
                    while True:
                        new_ests = []
                        for i in range(len(ests)):
                            cat, est = ests[i]
                            for j in range(i + 1, len(ests)):
                                if cat.isdisjoint(ests[j][0]):
                                    newcat = cat.union(ests[j][0])
                                    if newcat not in allcats:
                                        # we can merge them
                                        new_ests.append((newcat, est + ests[j][1]))
                                        allcats.add(newcat)
                        if len(new_ests) == 0:
                            break
                        ests.extend(new_ests)

                    if len(ests) != 0:
                        attr2est[attr] = max(
                            (x[1] for x in ests),
                            key=cmp_to_key(GradeTonnageEstimate.is_equal_or_better),
                        )
                for key, catval in [
                    ("original", OtherCategory.OriginalResource.value),
                    ("extracted", OtherCategory.Extracted.value),
                    ("cumulative_extracted", OtherCategory.CumulativeExtracted.value),
                ]:
                    attr2est[key] = max(
                        (est for cat, est in other_est if catval in cat),
                        default=None,
                        key=cmp_to_key(GradeTonnageEstimate.is_equal_or_better),
                    )

                grade_tonnage_per_zones[zone] = SiteGradeTonnage(
                    resource_estimate=attr2est["resource"],
                    reserve_estimate=attr2est["reserve"],
                    original_estimate=attr2est["original"],
                    extracted_estimate=attr2est["extracted"],
                    cumulative_extracted_estimate=attr2est["cumulative_extracted"],
                    date=date,
                )

            if len(grade_tonnage_per_zones) == 0:
                continue
            grade_tonnages.append(
                (date, self.aggregate_site_tonnages_by_zone(grade_tonnage_per_zones))
            )

        if len(grade_tonnages) == 0:
            return None

        return self.aggregate_site_tonnages_by_date(grade_tonnages)

    def aggregate_site_tonnages_by_zone(
        self, vals: dict[Optional[str], SiteGradeTonnage]
    ) -> SiteGradeTonnage:
        site_tonnage: Optional[SiteGradeTonnage] = None
        zone_tonnage: Optional[SiteGradeTonnage] = None

        for zone, val in vals.items():
            if zone is None:
                site_tonnage = val
            else:
                if zone_tonnage is None:
                    zone_tonnage = val
                else:
                    zone_tonnage = zone_tonnage.add(val, handle_original_estimate="add")

        if site_tonnage is not None:
            if zone_tonnage is not None:
                assert site_tonnage.date == zone_tonnage.date
                return site_tonnage.max(zone_tonnage)

            return site_tonnage
        else:
            assert zone_tonnage is not None
            return zone_tonnage

    def aggregate_site_tonnages_by_date(
        self, vals: Iterable[tuple[Optional[str], SiteGradeTonnage]]
    ):
        # ignore the data without date and always choose the one with the date
        # assuming that the data without date is more likely to be older
        site_gt = max(vals, key=lambda val: val[0] or "0000-00-00")[1]

        # now, we need to compute the cumulative extracted estimate if it is not reported
        # we need to go back from the history and use the cumulative extracted estimate if it is available
        if site_gt.cumulative_extracted_estimate is None and any(
            val.extracted_estimate is not None
            or val.cumulative_extracted_estimate is not None
            for date, val in vals
        ):
            extracted_estimates = []
            for date, val in sorted(
                ((date, val) for date, val in vals if date is not None),
                key=lambda x: x[0],
                reverse=True,
            ):
                if val.cumulative_extracted_estimate is not None:
                    # this cumulative extracted estimate is still better than the sum of all extracted estimates in the past
                    # so we can stop -- it includes the current date so we can stop here
                    extracted_estimates.append(val.cumulative_extracted_estimate)
                    break
                if val.extracted_estimate is not None:
                    extracted_estimates.append(val.extracted_estimate)

            if len(extracted_estimates) > 0:
                site_gt = SiteGradeTonnage(
                    resource_estimate=site_gt.resource_estimate,
                    reserve_estimate=site_gt.reserve_estimate,
                    original_estimate=site_gt.original_estimate,
                    extracted_estimate=site_gt.extracted_estimate,
                    cumulative_extracted_estimate=sum(
                        extracted_estimates, start=GradeTonnageEstimate(0, 0)
                    ),
                    date=site_gt.date,
                )

        return site_gt


weight_uncompatible_units = {
    MR_NS.uristr(id)
    for id in [
        "Q201",
        "Q203",
        "Q204",
        "Q205",
        "Q207",
        "Q208",
        "Q209",
        "Q210",
        "Q212",
        "Q216",
        "Q217",
        "Q220",
        "Q206",
        "Q211",
        "Q221",
        "Q218",
        "Q219",
    ]
}
percent_uncompatible_units = {
    MR_NS.uristr(id)
    for id in [
        "Q200",
        "Q202",
        "Q204",
        "Q205",
        "Q206",
        "Q207",
        "Q208",
        "Q209",
        "Q210",
        "Q211",
        "Q212",
        "Q213",
        "Q214",
        "Q215",
        "Q216",
        "Q218",
        "Q219",
        "Q221",
    ]
}


def unit_conversion(value: float, unit: str, to_unit: str) -> float:

    if unit == to_unit:
        return value

    if to_unit == MR_NS.uristr("Q202"):
        # convert to million tonnes
        if unit == MR_NS.uristr("Q200"):
            # from tonnes
            return value / 1_000_000
        if unit == MR_NS.uristr("Q213"):
            # from million short tons
            return value / 1.10231
        if unit == MR_NS.uristr("Q214"):
            return value / 1_000_000 / 1.10231
        if unit == MR_NS.uristr("Q215"):
            # million pounds
            return value * 0.000454
        if unit in weight_uncompatible_units:
            raise UnconvertibleUnitError((value, unit, to_unit))
        raise NotImplementedError((value, unit, to_unit))

    if to_unit == MR_NS.uristr("Q201"):
        # convert to percentage
        if unit == MR_NS.uristr("Q203") or unit == MR_NS.uristr("Q220"):
            # from grams per tonne or parts per million
            return value / 10_000
        if unit == MR_NS.uristr("Q217"):
            # from kg per tonne
            return value / 10
        if unit in percent_uncompatible_units:
            raise UnconvertibleUnitError((value, unit, to_unit))
        raise NotImplementedError((value, unit, to_unit))

    raise NotImplementedError((value, unit, to_unit))
