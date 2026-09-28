from __future__ import annotations

from typing import Annotated, Optional

from fastapi import APIRouter, HTTPException, Query, status
from minmodkg.api.dependencies import (
    PaperServiceDep,
    norm_commodity,
    norm_country,
    norm_deposit_type,
    norm_state_or_province,
)
from minmodkg.api.models.public_mineral_site import OutputPublicMineralSite
from minmodkg.api.models.public_sample import OutputPublicSample
from minmodkg.models.kgrel.paper import Paper
from minmodkg.services.paper import PaperService
from minmodkg.typing import InternalID

router = APIRouter(tags=["papers"])


def paper_dict(paper: Paper) -> dict:
    d = paper.to_dict()
    d.pop("file", None)
    return d


def get_paper_or_404(paper_service: PaperService, paper_id: str) -> Paper:
    paper = paper_service.find_by_id(paper_id)
    if paper is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="The requested paper does not exist.",
        )
    return paper


@router.get("/papers")
def list_papers(
    paper_service: PaperServiceDep,
    commodity: Optional[str] = None,
    deposit_type: Optional[str] = None,
    country: Optional[str] = None,
    state_or_province: Optional[str] = None,
    dedup_site_id: Optional[InternalID] = None,
    site_id: Optional[InternalID] = None,
    limit: Annotated[int, Query(ge=0)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
    return_count: Annotated[bool, Query()] = False,
):
    """Search papers: those with a site matching every filter, each with its
    matching sites. Entity filters take a name or a MinMod id."""
    results, total = paper_service.find_papers(
        commodity=norm_commodity(commodity) if commodity is not None else None,
        deposit_type=(
            norm_deposit_type(deposit_type) if deposit_type is not None else None
        ),
        country=norm_country(country) if country is not None else None,
        state_or_province=(
            norm_state_or_province(state_or_province)
            if state_or_province is not None
            else None
        ),
        dedup_site_id=dedup_site_id,
        site_id=site_id,
        limit=limit,
        offset=offset,
        return_count=return_count,
    )
    items = [
        {
            **paper_dict(paper),
            "sites": [
                {
                    "id": site.site_id,
                    "record_id": site.record_id,
                    "name": site.name,
                    "dedup_site_id": site.dedup_site_id,
                }
                for site in sites
            ],
        }
        for paper, sites in results
    ]
    if return_count:
        return {"items": items, "total": total}
    return items


@router.get("/papers/{paper_id}")
def get_paper(paper_id: str, paper_service: PaperServiceDep):
    paper = get_paper_or_404(paper_service, paper_id)
    return {
        **paper_dict(paper),
        "sites": [
            {
                **OutputPublicMineralSite.from_kgrel(site).to_dict(),
                "sample_count": count,
            }
            for site, count in paper_service.get_sites(paper)
        ],
    }


@router.get("/papers/{paper_id}/samples")
def get_paper_samples(
    paper_id: str,
    paper_service: PaperServiceDep,
    site_id: Optional[InternalID] = None,
    limit: Annotated[int, Query(ge=0)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
):
    paper = get_paper_or_404(paper_service, paper_id)
    samples, total = paper_service.find_samples(
        paper, site_id=site_id, limit=limit, offset=offset
    )
    return {
        "total": total,
        "items": [OutputPublicSample.from_kgrel(s).to_dict() for s in samples],
    }
