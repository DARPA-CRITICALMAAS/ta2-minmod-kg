from __future__ import annotations

from fastapi import APIRouter
from minmodkg.transformations import make_site_uri

router = APIRouter(tags=["mineral_sites"])


@router.get("/mineral-sites/make-id")
def get_site_uri(source_id: str, record_id: str):
    return make_site_uri(source_id, record_id)


@router.post("/mineral-sites/site_<id>")
def upsert_site(site_id: str):
    """Update or insert a mineral site."""

    

    return {}
