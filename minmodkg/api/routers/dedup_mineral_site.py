from __future__ import annotations

from typing import Annotated, Optional

from fastapi import APIRouter, Body, HTTPException, Query, status
from minmodkg.api.dependencies import is_minmod_id, norm_commodity
from minmodkg.api.models.public_dedup_mineral_site import DedupMineralSitePublic
from minmodkg.services.mineral_site import MineralSiteService
from minmodkg.typing import InternalID

router = APIRouter(tags=["mineral_sites"])


@router.get("/dedup-mineral-sites")
def dedup_mineral_sites_v2(
    commodity: Optional[str] = None,
    deposit_type: Optional[InternalID] = None,
    country: Optional[InternalID] = None,
    state_or_province: Optional[InternalID] = None,
    has_grade_tonnage: Optional[bool] = None,
    limit: Annotated[int, Query(ge=0)] = 0,
    offset: Annotated[int, Query(ge=0)] = 0,
    return_count: Annotated[bool, Query()] = False,
):
    if commodity is not None:
        commodity = norm_commodity(commodity)
    if deposit_type is not None:
        if not is_minmod_id(deposit_type):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Expect deposit_type to be a Q node, but get: {deposit_type}",
            )
    if country is not None:
        if not is_minmod_id(country):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Expect country to be a Q node, but get: {country}",
            )
    if state_or_province is not None:
        if not is_minmod_id(state_or_province):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Expect state_or_province to be a Q node, but get: {state_or_province}",
            )
    res = MineralSiteService().find_dedup_mineral_sites(
        commodity=commodity,
        deposit_type=deposit_type,
        country=country,
        state_or_province=state_or_province,
        has_grade_tonnage=has_grade_tonnage,
        limit=limit,
        offset=offset,
        return_count=return_count,
    )

    items = [
        DedupMineralSitePublic.from_kgrel(dmsi, commodity).to_dict()
        for dmsi in res["items"].values()
    ]
    if return_count:
        return {
            "items": items,
            "total": res["total"],
        }
    return items


@router.post("/dedup-mineral-sites/find_by_ids")
def api_get_dedup_mineral_sites(
    ids: Annotated[list[InternalID], Body(embed=True)],
    commodity: Annotated[InternalID, Body(embed=True)],
) -> dict[InternalID, dict]:
    res = MineralSiteService().find_dedup_mineral_sites(
        commodity=commodity, dedup_site_ids=ids
    )
    return {
        dms_id: DedupMineralSitePublic.from_kgrel(dmsi, commodity).to_dict()
        for dms_id, dmsi in res["items"].items()
    }


@router.get("/dedup-mineral-sites/{dedup_site_id}")
def api_get_dedup_mineral_site(
    dedup_site_id: str,
    commodity: Optional[str] = None,
):
    if commodity is not None:
        commodity = norm_commodity(commodity)
    qres = MineralSiteService().find_dedup_mineral_sites(
        commodity=commodity, dedup_site_ids=[dedup_site_id]
    )
    output = {
        dms_id: DedupMineralSitePublic.from_kgrel(dmsi, commodity)
        for dms_id, dmsi in qres["items"].items()
    }

    if len(output) == 0:
        raise HTTPException(status_code=404, detail=f"{dedup_site_id} not found")
    return output[dedup_site_id].to_dict()
