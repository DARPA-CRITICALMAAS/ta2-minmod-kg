from __future__ import annotations

from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from functools import cached_property, cmp_to_key, lru_cache
from typing import Iterable, Optional, Union

from minmodkg.misc import (
    MNR_NS,
    UnconvertibleUnitError,
    V,
    assert_isinstance,
    group_by_attr,
)


class ResourceCategory(str, Enum):
    """Resource category of a mineral site"""

    Inferred = f"{MNR_NS}Inferred"
    Indicated = f"{MNR_NS}Indicated"
    Measured = f"{MNR_NS}Measured"


class ReserveCategory(str, Enum):
    """Reserve category of a mineral site"""

    Proven = f"{MNR_NS}Proven"
    Probable = f"{MNR_NS}Probable"


# note that they may have more such as accumulated, previously mined, etc.
ResourceReserveCategory = str
Mt_unit = f"{MNR_NS}Q202"
percent_unit = f"{MNR_NS}Q201"


@dataclass(frozen=True)
class GradeTonnageEstimate:
    tonnage: float
    contained_metal: float

    def is_equal_or_better(self, other: GradeTonnageEstimate):
        return self.contained_metal >= other.contained_metal


@dataclass(frozen=True)
class SiteGradeTonnage:
    resource_estimate: Optional[GradeTonnageEstimate] = None
    reserve_estimate: Optional[GradeTonnageEstimate] = None

    # category2estimate: dict[str, GradeTonnageEstimate]

    @cached_property
    def total_resource_contained_metal(self):
        if self.resource_estimate is not None:
            return self.resource_estimate.contained_metal
        return None
        # if all(cat.value not in self.category2estimate for cat in ResourceCategory):
        #     return None
        # return sum(
        #     self.category2estimate[cat.value].contained_metal
        #     for cat in ResourceCategory
        #     if cat.value in self.category2estimate
        # )

    @cached_property
    def total_resource_tonnage(self):
        if self.resource_estimate is not None:
            return self.resource_estimate.tonnage
        return None
        # if all(cat.value not in self.category2estimate for cat in ResourceCategory):
        #     return None
        # return sum(
        #     self.category2estimate[cat.value].tonnage
        #     for cat in ResourceCategory
        #     if cat.value in self.category2estimate
        # )

    @cached_property
    def total_reserve_contained_metal(self):
        if self.reserve_estimate is not None:
            return self.reserve_estimate.contained_metal
        return None
        # if all(cat.value not in self.category2estimate for cat in ReserveCategory):
        #     return None
        # return sum(
        #     self.category2estimate[cat.value].contained_metal
        #     for cat in ReserveCategory
        #     if cat.value in self.category2estimate
        # )

    @cached_property
    def total_reserve_tonnage(self):
        if self.reserve_estimate is not None:
            return self.reserve_estimate.tonnage
        return None
        # if all(cat.value not in self.category2estimate for cat in ReserveCategory):
        #     return None
        # return sum(
        #     self.category2estimate[cat.value].tonnage
        #     for cat in ReserveCategory
        #     if cat.value in self.category2estimate
        # )

    @cached_property
    def total_compared_contained_metal(self):
        if self.total_resource_contained_metal is not None:
            return self.total_resource_contained_metal
        return self.total_reserve_contained_metal

    @cached_property
    def total_contained_metal(self):
        total = 0
        if self.total_resource_contained_metal is not None:
            total += self.total_resource_contained_metal
        if self.total_reserve_contained_metal is not None:
            total += self.total_reserve_contained_metal
        return total
        # return sum(val.contained_metal for val in self.category2estimate.values())

    def get_total_resource_grade(self, unit: Optional[str] = None) -> Optional[float]:
        if unit is None:
            unit = percent_unit

        total_resource_contained_metal = self.total_resource_contained_metal
        if total_resource_contained_metal is None:
            return None

        total_resource_tonnage = self.total_resource_tonnage
        assert total_resource_tonnage is not None
        if total_resource_contained_metal == 0.0:
            assert total_resource_tonnage == 0.0
            return 0.0

        return unit_conversion(
            total_resource_contained_metal / total_resource_tonnage, percent_unit, unit
        )

    def get_total_reserve_grade(self, unit: Optional[str] = None) -> Optional[float]:
        if unit is None:
            unit = percent_unit

        total_reserve_contained_metal = self.total_reserve_contained_metal
        if total_reserve_contained_metal is None:
            return None

        total_reserve_tonnage = self.total_reserve_tonnage
        assert total_reserve_tonnage is not None
        if total_reserve_contained_metal == 0.0:
            assert total_reserve_tonnage == 0.0
            return 0.0

        return unit_conversion(
            total_reserve_contained_metal / total_reserve_tonnage, percent_unit, unit
        )

    def is_equal_or_better(self, other: SiteGradeTonnage):
        if self.total_compared_contained_metal is not None:
            if other.total_compared_contained_metal is not None:
                return (
                    self.total_compared_contained_metal
                    >= other.total_compared_contained_metal
                )
            return True
        elif other.total_compared_contained_metal is not None:
            return False

        return self.total_contained_metal >= other.total_contained_metal

    def add(self, other: SiteGradeTonnage):
        newval = self.category2estimate.copy()
        for k, v in other.category2estimate.items():
            if k not in newval:
                newval[k] = v
            else:
                newval[k] = GradeTonnageEstimate(
                    tonnage=newval[k].tonnage + v.tonnage,
                    contained_metal=newval[k].contained_metal + v.contained_metal,
                )
        return SiteGradeTonnage(newval)

    def get_estimate(self, category: str, default: V) -> Union[GradeTonnageEstimate, V]:
        return self.category2estimate.get(category, default)


class GradeTonnageModel:
    """Computing grade & tonnage data of a single mineral site from mineral inventories"""

    @dataclass
    class MineralInventory:
        id: str  # unique identifier of the inventory -- for knowing the reported data is sum of multiple categories or not
        date: Optional[
            str
        ]  # %YYYY-%MM-%DD: this allow us to group by and sort by date without parsing it
        zone: Optional[str]
        category: list[str]
        ore_value: float
        ore_unit: str
        grade_value: float
        grade_unit: str

    def __call__(
        self,
        invs: list[MineralInventory],
        norm_tonnage_unit: Optional[str] = None,
        norm_grade_unit: Optional[str] = None,
    ):
        if norm_tonnage_unit is None:
            norm_tonnage_unit = Mt_unit
        if norm_grade_unit is None:
            norm_grade_unit = percent_unit

        # group by zone & date
        grade_tonnages = []
        for date, invs_by_date in group_by_attr(invs, "date").items():
            grade_tonnage_per_zones = []
            for zone, invs_by_date_zone in group_by_attr(invs_by_date, "zone").items():
                # the extraction may went wrong and we have multiple results per category
                # therefore, we need to handle them.
                # assert len(invs_by_date_zone) == 1

                grade_tonnage_zone = SiteGradeTonnage()

                # the first step is normalization
                estimates = []
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

                    estimates.append(
                        (
                            frozenset(inv.category),
                            GradeTonnageEstimate(
                                tonnage=ore,
                                contained_metal=ore
                                * unit_conversion(grade, norm_grade_unit, percent_unit),
                            ),
                        )
                    )

                cat2estimates = defaultdict(list)
                for inv, estimate in estimates:
                    cat = tuple(sorted(inv.category))
                    cat2estimates[cat].append(estimate)
                cat2estimate = {
                    cat: max(
                        ests, key=cmp_to_key(GradeTonnageEstimate.is_equal_or_better)
                    )
                    for cat, ests in cat2estimates.items()
                }

                # now, we need to compute resource/reserve estimates by summing up the estimate

                grade_tonnage_per_zones.append(
                    (
                        zone,
                        SiteGradeTonnage(
                            {
                                cat: self.aggregate_tonnage_estimates(vals)
                                for cat, vals in estimates.items()
                            }
                        ),
                    )
                )

            if len(grade_tonnage_per_zones) == 0:
                continue

            grade_tonnages.append(
                (date, self.aggregate_site_tonnages_by_zone(grade_tonnage_per_zones))
            )

        if len(grade_tonnages) == 0:
            return None

        return self.aggregate_site_tonnages_by_date(grade_tonnages)

    def aggregate_tonnage_estimates(self, vals: Iterable[GradeTonnageEstimate]):
        return max(vals, key=cmp_to_key(GradeTonnageEstimate.is_equal_or_better))

    def aggregate_site_tonnages_by_zone(
        self, vals: list[tuple[Optional[str], SiteGradeTonnage]]
    ):
        site_tonnage = None
        zone_tonnage = None

        for zone, val in vals:
            if zone is None:
                if site_tonnage is not None:
                    raise Exception(
                        "We should encounter the site inventory (no zone) only once"
                    )
                site_tonnage = val
            else:
                if zone_tonnage is None:
                    zone_tonnage = val
                else:
                    zone_tonnage = zone_tonnage.add(val)

        if site_tonnage is not None:
            if zone_tonnage is not None:
                if site_tonnage.is_equal_or_better(zone_tonnage):
                    return site_tonnage
                return zone_tonnage

            return site_tonnage
        else:
            assert zone_tonnage is not None
            return zone_tonnage

    def aggregate_site_tonnages_by_date(
        self, vals: Iterable[tuple[Optional[str], SiteGradeTonnage]]
    ):
        return max(vals, key=cmp_to_key(lambda a, b: a[1].is_equal_or_better(b[1])))[1]


def unit_conversion(value: float, unit: str, to_unit: str) -> float:
    if unit == to_unit:
        return value

    if to_unit == f"{MNR_NS}Q202":
        # convert to million tonnes
        if unit == f"{MNR_NS}Q200":
            # from tonnes
            return value / 1_000_000
        if unit in {f"{MNR_NS}Q217", f"{MNR_NS}Q218"}:
            raise UnconvertibleUnitError((value, unit, to_unit))
        raise NotImplementedError((value, unit, to_unit))

    if to_unit == f"{MNR_NS}Q201":
        # convert to percentage
        if unit == f"{MNR_NS}Q203" or unit == f"{MNR_NS}Q220":
            # from grams per tonne or parts per million
            return value / 10_000
        if unit == f"{MNR_NS}Q217":
            # from kg per tonne
            return value / 10
        raise NotImplementedError((value, unit, to_unit))

    raise NotImplementedError((value, unit, to_unit))
