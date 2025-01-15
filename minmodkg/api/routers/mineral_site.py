from __future__ import annotations

from functools import lru_cache
from typing import Annotated, Literal, Optional

from fastapi import APIRouter, Body, HTTPException, Query, Response, status
from minmodkg.api.dependencies import (
    CurrentUserDep,
    MineralSiteServiceDep,
    get_snapshot_id,
)
from minmodkg.api.models.public_mineral_site import (
    InputPublicMineralSite,
    OutputPublicMineralSite,
)
from minmodkg.api.routers.predefined_entities import (
    get_crs,
    get_material_forms,
    get_sources,
)
from minmodkg.services.mineral_site import (
    ExpiredSnapshotIdError,
    UnsupportOperationError,
)
from minmodkg.transformations import make_site_uri
from minmodkg.typing import InternalID
from pydantic import BaseModel

router = APIRouter(tags=["mineral_sites"])


class UpdateDedupLink(BaseModel):
    """A class represents the latest dedup links"""

    sites: list[InternalID]


@router.get("/mineral-sites/make-id")
def get_site_uri(source_id: str, record_id: str, return_uri: bool = False):
    if return_uri:
        return make_site_uri(source_id, record_id)
    return Response(
        make_site_uri(source_id, record_id, namespace=""), media_type="text/plain"
    )


@router.post("/mineral-sites/find_by_ids")
def get_sites(
    ids: Annotated[list[InternalID], Body(embed=True, alias="ids")],
    mineral_site_service: MineralSiteServiceDep,
):
    return {
        k: OutputPublicMineralSite.from_kgrel(v).to_dict()
        for k, v in mineral_site_service.find_by_ids(ids).items()
    }


@router.head("/mineral-sites/{site_id}")
def has_site(
    site_id: InternalID,
    mineral_site_service: MineralSiteServiceDep,
):
    site_db_id = mineral_site_service.get_site_db_id(site_id)
    if site_db_id is None:
        return Response(status_code=status.HTTP_404_NOT_FOUND)
    return Response(status_code=status.HTTP_200_OK)


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
        return OutputPublicMineralSite.from_kgrel(mineral_site).to_dict()
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
    dedup_ids = mineral_site_service.update_same_as([g.sites for g in same_site_groups])
    return [{"id": dedup_id} for dedup_id in dedup_ids]


@router.post("/mineral-sites")
def create_site(
    create_site: Annotated[InputPublicMineralSite, Body()],
    mineral_site_service: MineralSiteServiceDep,
    user: CurrentUserDep,
):
    snapshot_id = get_snapshot_id()
    new_msi = create_site.to_kgrel(
        material_form_uri_to_conversion(snapshot_id),
        crs_uri_to_name(snapshot_id),
        source_uri_to_score(snapshot_id),
    )

    site_db_id = mineral_site_service.get_site_db_id(new_msi.ms.site_id)
    if site_db_id is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="The site already exists.",
        )

    mineral_site_service.create(user, new_msi)
    return OutputPublicMineralSite.from_kgrel(new_msi).to_dict()


@router.put("/mineral-sites/{site_id}")
def update_site(
    site_id: str,
    update_site: InputPublicMineralSite,
    mineral_site_service: MineralSiteServiceDep,
    user: CurrentUserDep,
    snapshot_id: Annotated[Optional[int], Query()] = None,
):
    kg_snapshot_id = get_snapshot_id()
    upd_msi = update_site.to_kgrel(
        material_form_uri_to_conversion(kg_snapshot_id),
        crs_uri_to_name(kg_snapshot_id),
        source_uri_to_score(kg_snapshot_id),
    )

    if site_id != upd_msi.ms.site_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The site_id in the request body does not match the site_id in the URL.",
        )

    site_db_id = mineral_site_service.get_site_db_id(site_id)
    if site_db_id is None:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="The site doesn't exist",
        )

    try:
        mineral_site_service.update(
            user, upd_msi.set_id(site_db_id), site_snapshot_id=snapshot_id
        )
    except ExpiredSnapshotIdError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(e),
        )
    except UnsupportOperationError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(e),
        )

    return OutputPublicMineralSite.from_kgrel(upd_msi).to_dict()


@lru_cache(maxsize=1)
def crs_uri_to_name(snapshot_id: str):
    return {crs.uri: crs.name for crs in get_crs(snapshot_id)}


@lru_cache(maxsize=1)
def material_form_uri_to_conversion(snapshot_id: str):
    return {mf.uri: mf.conversion for mf in get_material_forms(snapshot_id)}


@lru_cache(maxsize=1)
def source_uri_to_score(snapshot_id: str):
    return {source.uri: source.score for source in get_sources(snapshot_id)}
