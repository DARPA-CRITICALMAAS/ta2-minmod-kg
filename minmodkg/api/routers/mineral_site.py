from __future__ import annotations

from functools import lru_cache
from typing import Annotated, Literal

from fastapi import APIRouter, Body, HTTPException, Response, status
from loguru import logger
from minmodkg.api.dependencies import CurrentUserDep, get_snapshot_id
from minmodkg.api.models.user import is_system_user
from minmodkg.api.routers.predefined_entities import get_crs, get_material_forms
from minmodkg.config import MINMOD_KG
from minmodkg.misc.exceptions import DBError
from minmodkg.models.dedup_mineral_site import DedupMineralSite
from minmodkg.models.derived_mineral_site import DerivedMineralSite
from minmodkg.models.mineral_site import MineralSite
from minmodkg.transformations import make_site_uri
from minmodkg.typing import IRI, Triple
from rdflib import Graph, URIRef

router = APIRouter(tags=["mineral_sites"])


@router.get("/mineral-sites/make-id")
def get_site_uri(source_id: str, record_id: str):
    return make_site_uri(source_id, record_id)


@router.post("/mineral-sites/find_by_ids")
def get_sites(uris: Annotated[list[str], Body(embed=True, alias="ids")]):
    sites = []
    for uri in uris:
        sites.append(get_site_by_uri(uri))
    return sites


@router.get("/mineral-sites/{site_id}")
def get_site(site_id: str, format: Literal["json", "ttl"] = "json"):
    if format == "json":
        return get_site_by_uri(MINMOD_KG.ns.mr.uri(site_id))
    elif format == "ttl":
        uri = MINMOD_KG.ns.mr.uri(site_id)
        g_site = MineralSite.get_graph_by_uri(uri)
        g_derived_site = DerivedMineralSite.get_graph_by_uri(uri)
        return Response(
            content=(g_site + g_derived_site).serialize(format="ttl"),
            media_type="text/turtle",
        )
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid format.",
        )


def get_site_by_uri(uri: IRI | URIRef) -> dict:
    site = MineralSite.get_by_uri(uri)
    derived_site = DerivedMineralSite.get_by_uri(uri)
    out = site.to_json()
    out.update(derived_site.to_json())
    return out


@router.post("/mineral-sites")
def create_site(site: MineralSite, user: CurrentUserDep):
    """Create a mineral site."""
    # To safely update the data, we need to do it in a transaction
    # There are two places where we update the data:
    #   1. The mineral site itself
    #   2. The dedup mineral site. However, the dedup mineral site can only be updated
    #      by reading the data first. This is impossible to do via Restful API as we
    #      cannot explicitly control the transaction. We have to achieve this by implementing
    #      a custom lock mechanism. We will revisit this later.
    uri = make_site_uri(site.source_id, site.record_id)

    if MineralSite.has_uri(uri):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="The site already exists.",
        )

    # update controlled fields and convert the site to TTL
    site.update_derived_data(user.username)
    snapshot_id = get_snapshot_id()
    derived_site, partial_dedup_site = derive_data(
        site,
        material_form_uri_to_conversion(snapshot_id),
        crs_uri_to_name(snapshot_id),
    )

    # send the query
    triples = []
    triples = site.to_triples(triples)
    triples = derived_site.to_triples(triples)
    triples = partial_dedup_site.to_triples(triples)

    try:
        MINMOD_KG.insert(triples)
    except DBError as e:
        logger.error(
            "Failed to create site:\n\t- User: {}\n\t- Input: {}\n\t- Response: {}",
            user.username,
            site.model_dump_json(),
            e.message,
        )
        raise HTTPException(status_code=500, detail="Failed to create the site.") from e

    return get_site_by_uri(URIRef(uri))


@router.put("/mineral-sites/{site_id}")
def update_site(site_id: str, site: MineralSite, user: CurrentUserDep):
    uri = MineralSite.rdfdata.ns.mr.uri(site_id)
    if not MineralSite.has_uri(uri):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="The site does not exist.",
        )

    if any(is_system_user(user) for user in site.created_by):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Automated system is not allowed to update the site.",
        )

    if site.dedup_site_uri is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The updated site must have a dedup site URI.",
        )

    # update controlled fields and convert the site to TTL
    # the site must have no blank nodes as we want a fast way to compute the delta.
    site.update_derived_data(user.username)
    snapshot_id = get_snapshot_id()
    derived_site, partial_dedup_site = derive_data(
        site,
        material_form_uri_to_conversion(snapshot_id),
        crs_uri_to_name(snapshot_id),
    )

    # TODO: we can update current site & derived site because we have all of the data
    # for dedup site, we do not have all, so we have to refetch and compute the differences
    # manually
    ng = site.to_graph() + derived_site.to_graph()

    # start the transaction, we can read as much as we want but we can only write once
    with MINMOD_KG.transaction([uri, site.dedup_site_uri]).transaction():
        og = MineralSite.get_graph_by_uri(uri) + DerivedMineralSite.get_graph_by_uri(
            uri
        )
        del_triples, add_triples = get_site_changes(og, ng)
        # TODO: update dedup site
        # DedupMineralSite.get_by_uri(site.dedup_site_uri)
        MINMOD_KG.delete_insert(del_triples, add_triples)

    return get_site_by_uri(URIRef(uri))


def get_site_changes(
    current_site: Graph, new_site: Graph
) -> tuple[list[Triple], list[Triple]]:
    current_triples = set(current_site)
    new_triples = set(new_site)
    del_triples = [
        (s.n3(), p.n3(), o.n3()) for s, p, o in current_triples.difference(new_triples)
    ]
    add_triples = [
        (s.n3(), p.n3(), o.n3()) for s, p, o in new_triples.difference(current_triples)
    ]
    return del_triples, add_triples


def derive_data(
    site: MineralSite,
    material_form_conversion: dict[str, float],
    crss: dict[str, str],
) -> tuple[DerivedMineralSite, DedupMineralSite]:
    ns = MineralSite.rdfdata.ns

    if site.dedup_site_uri is None:
        site.dedup_site_uri = ns.mr.uristr(
            DedupMineralSite.get_id([ns.mr.id(site.uri)])
        )

    # get derived data
    derived_site = DerivedMineralSite.from_mineral_site(
        site, material_form_conversion, crss
    )
    partial_dedup_site = DedupMineralSite.from_derived_sites(
        [derived_site], ns.mr.id(site.dedup_site_uri)
    )
    return derived_site, partial_dedup_site


@lru_cache(maxsize=1)
def crs_uri_to_name(snapshot_id: str):
    return {crs.uri: crs.name for crs in get_crs(snapshot_id)}


@lru_cache(maxsize=1)
def material_form_uri_to_conversion(snapshot_id: str):
    return {mf.uri: mf.conversion for mf in get_material_forms(snapshot_id)}
