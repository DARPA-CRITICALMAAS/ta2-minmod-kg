from __future__ import annotations

from functools import lru_cache
from typing import Annotated, Literal

from fastapi import APIRouter, Body, HTTPException, Response, status
from minmodkg.api.dependencies import (
    CurrentUserDep,
    MineralSiteServiceDep,
    get_snapshot_id,
)
from minmodkg.api.models.public_mineral_site import (
    InputPublicMineralSite,
    PublicMineralSite,
)
from minmodkg.api.routers.predefined_entities import get_crs, get_material_forms
from minmodkg.transformations import make_site_uri
from minmodkg.typing import InternalID
from pydantic import BaseModel

router = APIRouter(tags=["mineral_sites"])


class UpdateDedupLink(BaseModel):
    """A class represents the latest dedup links"""

    sites: list[InternalID]


@router.get("/mineral-sites/make-id")
def get_site_uri(source_id: str, record_id: str):
    return make_site_uri(source_id, record_id)


@router.post("/mineral-sites/find_by_ids")
def get_sites(
    ids: Annotated[list[InternalID], Body(embed=True, alias="ids")],
    mineral_site_service: MineralSiteServiceDep,
):
    return {
        k: PublicMineralSite.from_kgrel(v).to_dict()
        for k, v in mineral_site_service.find_by_ids(ids).items()
    }


@router.get("/mineral-sites/{site_id}")
def get_site(
    site_id: InternalID,
    mineral_site_service: MineralSiteServiceDep,
    format: Literal["json", "ttl"] = "json",
):
    mineral_site = mineral_site_service.find_by_id(site_id)
    if mineral_site is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="The requested site does not exist.",
        )
    if format == "json":
        return PublicMineralSite.from_kgrel(mineral_site).to_dict()
    elif format == "ttl":
        raise NotImplementedError()
        return Response(
            content=(g_site + g_derived_site).serialize(format="ttl"),
            media_type="text/turtle",
        )
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid format.",
        )


@router.post("/same-as")
def update_same_as(
    same_site_groups: list[UpdateDedupLink],
    mineral_site_service: MineralSiteServiceDep,
    current_user: CurrentUserDep,
):
    mineral_site_service.update_same_as([g.sites for g in same_site_groups])


@router.post("/mineral-sites")
def create_site(
    create_site: Annotated[InputPublicMineralSite, Body()],
    mineral_site_service: MineralSiteServiceDep,
    user: CurrentUserDep,
):
    snapshot_id = get_snapshot_id()
    new_site = create_site.to_kgrel(
        material_form_uri_to_conversion(snapshot_id),
        crs_uri_to_name(snapshot_id),
    )

    if mineral_site_service.contain_site_id(new_site.site_id):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="The site already exists.",
        )

    mineral_site_service.create(user, new_site)
    return PublicMineralSite.from_kgrel(new_site).to_dict()


@router.put("/mineral-sites/{site_id}")
def update_site(
    site_id: str,
    update_site: InputPublicMineralSite,
    mineral_site_service: MineralSiteServiceDep,
    user: CurrentUserDep,
):
    snapshot_id = get_snapshot_id()
    new_site = update_site.to_kgrel(
        material_form_uri_to_conversion(snapshot_id),
        crs_uri_to_name(snapshot_id),
    )

    if site_id != new_site.site_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The site_id in the request body does not match the site_id in the URL.",
        )

    if not mineral_site_service.contain_site_id(site_id):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="The site already exists.",
        )

    mineral_site_service.update(user, new_site)
    return PublicMineralSite.from_kgrel(new_site).to_dict()


@lru_cache(maxsize=1)
def crs_uri_to_name(snapshot_id: str):
    return {crs.uri: crs.name for crs in get_crs(snapshot_id)}


@lru_cache(maxsize=1)
def material_form_uri_to_conversion(snapshot_id: str):
    return {mf.uri: mf.conversion for mf in get_material_forms(snapshot_id)}
