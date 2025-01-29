from __future__ import annotations

from typing import Annotated, Literal, Optional

from fastapi import APIRouter, Body, HTTPException, Query, Response, status
from minmodkg.api.dependencies import (
    CurrentUserDep,
    MineralSiteServiceDep,
    RelSessionDep,
)
from minmodkg.api.models.public_mineral_site import (
    InputPublicMineralSite,
    OutputPublicMineralSite,
)
from minmodkg.models.kg.base import NS_MR
from minmodkg.models.kgrel.mineral_site import MineralSite
from minmodkg.services.mineral_site import (
    ExpiredSnapshotIdError,
    UnsupportOperationError,
)
from minmodkg.transformations import make_site_id
from minmodkg.typing import InternalID
from pydantic import BaseModel
from sqlalchemy import select

router = APIRouter(tags=["mineral_sites"])


class UpdateDedupLink(BaseModel):
    """A class represents the latest dedup links"""

    sites: list[InternalID]


@router.get("/mineral-sites/make-id")
def get_site_uri(
    username: str, source_id: str, record_id: str, return_uri: bool = False
):
    if return_uri:
        uri = NS_MR.uristr(make_site_id(username, source_id, record_id))
    else:
        uri = make_site_id(username, source_id, record_id)
    return Response(
        uri,
        media_type="text/plain",
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
    format: Literal["json"] = "json",
):
    mineral_site = mineral_site_service.find_by_id(site_id)
    if mineral_site is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="The requested site does not exist.",
        )
    if format == "json":
        return OutputPublicMineralSite.from_kgrel(mineral_site).to_dict()
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
    dedup_ids = mineral_site_service.update_same_as(
        current_user.get_uri(), [g.sites for g in same_site_groups]
    )
    return [{"id": dedup_id} for dedup_id in dedup_ids]


@router.post("/mineral-sites")
def create_site(
    create_site: Annotated[InputPublicMineralSite, Body()],
    mineral_site_service: MineralSiteServiceDep,
    user: CurrentUserDep,
):
    new_msi = create_site.to_kgrel(user.get_uri())

    site_db_id = mineral_site_service.get_site_db_id(new_msi.ms.site_id)
    if site_db_id is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="The site already exists.",
        )

    mineral_site_service.create(new_msi)
    return OutputPublicMineralSite.from_kgrel(new_msi).to_dict()


@router.put("/mineral-sites/{site_id}")
def update_site(
    site_id: str,
    update_site: InputPublicMineralSite,
    mineral_site_service: MineralSiteServiceDep,
    user: CurrentUserDep,
    snapshot_id: Annotated[Optional[int], Query()] = None,
):
    upd_msi = update_site.to_kgrel(user.get_uri())

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
            upd_msi.set_id(site_db_id), site_snapshot_id=snapshot_id
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
