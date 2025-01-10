from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Optional

from minmodkg.models.base import MINMOD_NS
from minmodkg.models.derived_mineral_site import GradeTonnage
from minmodkg.models_v2.kgrel.custom_types.location import LocationView
from minmodkg.models_v2.kgrel.mineral_site import MineralSite
from minmodkg.models_v2.kgrel.user import is_system_user
from minmodkg.models_v2.kgrel.views.mineral_inventory_view import MineralInventoryView
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
    def from_kgrel(
        same_sites: list[MineralSite],
        commodity: Optional[InternalID],
        default_source_score: float = 0.5,
    ):
        """Create a public dedup mineral site from a list of same sites"""
        mr = MINMOD_NS.mr

        _tmp_deposit_types: dict[str, DedupMineralSiteDepositType] = {}
        for site in same_sites:
            for dt in site.deposit_type_candidates:
                if dt.normalized_uri is None:
                    continue
                dt_id = mr.id(dt.normalized_uri)
                if (
                    dt_id not in _tmp_deposit_types
                    or dt.confidence > _tmp_deposit_types[dt_id].confidence
                ):
                    _tmp_deposit_types[dt_id] = DedupMineralSiteDepositType(
                        id=dt_id,
                        source=dt.source,
                        confidence=dt.confidence,
                    )

        deposit_types: list[DedupMineralSiteDepositType] = list(
            _tmp_deposit_types.values()
        )

        ranked_site_and_scores = [
            (site, min(site_score[0], 1.0))
            for site, site_score in sorted(
                (
                    (
                        site,
                        get_ms_source_score(
                            site.source_score,
                            site.created_by,
                            site.modified_at,
                            default_source_score,
                        ),
                    )
                    for site in same_sites
                ),
                key=lambda x: x[1],
                reverse=True,
            )
        ]
        site_name: str = next(
            (
                site.name
                for site, site_score in ranked_site_and_scores
                if site.name is not None
            ),
            "",
        )
        site_type = next(
            (
                site.type
                for site, site_score in ranked_site_and_scores
                if site.type is not None
            ),
            "NotSpecified",
        )
        site_rank = next(
            (
                site.rank
                for site, site_score in ranked_site_and_scores
                if site.rank is not None
            ),
            "U",
        )
        # TODO: fix me! we should normalize this rank & type at the beginning
        if site_type == "":
            site_type = "NotSpecified"
        if site_rank == "":
            site_rank = "U"

        country = next(
            (
                site.location_view.country
                for site, site_score in ranked_site_and_scores
                if len(site.location_view.country) > 0
            ),
            [],
        )
        state_or_province = next(
            (
                site.location_view.state_or_province
                for site, site_score in ranked_site_and_scores
                if len(site.location_view.state_or_province) > 0
            ),
            [],
        )
        lat, lon = next(
            (
                (site.location_view.lat, site.location_view.lon)
                for site, site_score in ranked_site_and_scores
                if site.location_view.lat is not None
                and site.location_view.lon is not None
            ),
            (None, None),
        )
        locview = LocationView(
            lat=lat,
            lon=lon,
            country=country,
            state_or_province=state_or_province,
        )
        if locview.is_empty():
            location = None
        else:
            location = DedupMineralSiteLocation(
                lat=locview.lat,
                lon=locview.lon,
                country=locview.country,
                state_or_province=locview.state_or_province,
            )

        # compute grade & tonnage
        commodity_gt_sites: dict[
            InternalID, list[tuple[MineralSite, MineralInventoryView]]
        ] = defaultdict(list)
        for site in same_sites:
            for inv in site.inventory_views:
                if commodity is not None and inv.commodity != commodity:
                    continue
                commodity_gt_sites[inv.commodity].append((site, inv))

        gts = []
        for commodity, lst_site_inv in commodity_gt_sites.items():
            gt_sites = [
                (site, inv)
                for site, inv in lst_site_inv
                if inv.contained_metal is not None
            ]
            if len(gt_sites) > 0:
                # if there is grade & tonnage from the users, prefer it
                curated_gt_sites = [
                    (site, inv)
                    for site, inv in gt_sites
                    if any(not is_system_user(u) for u in site.created_by)
                ]
                if len(curated_gt_sites) > 0:
                    # choose based on the latest modified date
                    inv = max(
                        curated_gt_sites,
                        key=lambda site_inv: site_inv[0].modified_at,
                    )[1]
                    gt = GradeTonnage(
                        commodity=commodity,
                        total_contained_metal=inv.contained_metal,
                        total_tonnage=inv.tonnage,
                        total_grade=inv.grade,
                    )
                else:
                    # no curated grade & tonnage, choose the one with the highest contained metal
                    # TODO: choose the one with the most recent date
                    inv = max(
                        (
                            (inv, inv.contained_metal)
                            for site, inv in gt_sites
                            if inv.contained_metal is not None
                        ),
                        key=lambda x: x[1],
                    )[
                        0
                    ]  # doing this weird to avoid typing error
                    gt = GradeTonnage(
                        commodity=commodity,
                        total_contained_metal=inv.contained_metal,
                        total_tonnage=inv.tonnage,
                        total_grade=inv.grade,
                    )
            else:
                gt = GradeTonnage(
                    commodity=commodity,
                )
            gts.append(gt)

        return DedupMineralSitePublic(
            id=same_sites[0].dedup_site_id,
            name=site_name,
            type=site_type,
            rank=site_rank,
            sites=[
                DedupMineralSiteIdAndScore(
                    id=site.site_id,
                    score=site_score,
                )
                for site, site_score in ranked_site_and_scores
            ],
            deposit_types=sorted(
                deposit_types, key=lambda x: x.confidence, reverse=True
            )[:5],
            location=location,
            grade_tonnage=gts,
            modified_at=max(site.modified_at for site in same_sites).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
        )


def get_ms_source_score(
    source_score: Optional[float],
    created_by: list[str],
    modified_at: datetime,
    default_score: float,
):
    if source_score is None or source_score < 0:
        source_score = default_score
    if any(not is_system_user(x) for x in created_by):
        # expert get the highest priority
        return (100.0, modified_at)
    return (min(source_score, 99.0), modified_at)
