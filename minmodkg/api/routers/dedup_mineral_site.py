from __future__ import annotations

from typing import Annotated, Optional

from fastapi import APIRouter, Body, HTTPException, Query
from minmodkg.api.dependencies import norm_commodity
from minmodkg.api.models.public_dedup_mineral_site import DedupMineralSitePublic
from minmodkg.services.mineral_site_v2 import MineralSiteService
from minmodkg.typing import InternalID

router = APIRouter(tags=["mineral_sites"])


@router.get("/dedup-mineral-sites")
def dedup_mineral_sites_v2(
    commodity: Optional[str] = None,
    limit: Annotated[int, Query(ge=0)] = 0,
    offset: Annotated[int, Query(ge=0)] = 0,
):
    if commodity is None:
        dms2sites = MineralSiteService().find_dedup_mineral_sites(commodity=commodity)
    else:
        commodity = norm_commodity(commodity)
        dms2sites = MineralSiteService().find_dedup_mineral_sites(commodity=commodity)
    output = [
        DedupMineralSitePublic.from_kgrel(same_sites, commodity)
        for same_sites in dms2sites.values()
    ]

    return [x.model_dump(exclude_none=True) for x in output]


@router.post("/dedup-mineral-sites/find_by_ids")
def api_get_dedup_mineral_sites(
    ids: Annotated[list[InternalID], Body(embed=True)],
    commodity: Annotated[InternalID, Body(embed=True)],
) -> dict[InternalID, dict]:
    dms2sites = MineralSiteService().find_dedup_mineral_sites(
        commodity=commodity, dedup_site_ids=ids
    )
    return {
        dedup_id: DedupMineralSitePublic.from_kgrel(same_sites, commodity).model_dump(
            exclude_none=True
        )
        for dedup_id, same_sites in dms2sites.items()
    }


@router.get("/dedup-mineral-sites/{dedup_site_id}")
def api_get_dedup_mineral_site(
    dedup_site_id: str,
    commodity: Optional[str] = None,
):
    if commodity is not None:
        commodity = norm_commodity(commodity)
    dms2sites = MineralSiteService().find_dedup_mineral_sites(
        commodity=commodity, dedup_site_ids=[dedup_site_id]
    )
    output = {
        dedup_id: DedupMineralSitePublic.from_kgrel(same_sites, commodity)
        for dedup_id, same_sites in dms2sites.items()
    }

    if len(output) == 0:
        raise HTTPException(status_code=404, detail=f"{dedup_site_id} not found")
    return output[dedup_site_id].model_dump(exclude_none=True)
