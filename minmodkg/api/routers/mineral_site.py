from __future__ import annotations

from datetime import datetime, timezone
from functools import lru_cache
from hashlib import sha256
from importlib.metadata import version
from pathlib import Path
from typing import Annotated, Callable

from drepr.main import convert
from drepr.models.resource import ResourceDataObject
from fastapi import APIRouter, Body, HTTPException, status
from libactor.cache import BackendFactory, cache
from loguru import logger
from minmodkg.api.dependencies import CurrentUserDep, get_snapshot_id
from minmodkg.api.routers.predefined_entities import get_crs, get_material_forms
from minmodkg.config import MNO_NS, MNR_NS, NS_MNO, NS_MNR, SPARQL_ENDPOINT
from minmodkg.misc import (
    Transaction,
    Triples,
    has_uri,
    sparql_construct,
    sparql_delete_insert,
    sparql_insert,
)
from minmodkg.models.crs import CRS
from minmodkg.models.dedup_mineral_site import DedupMineralSite
from minmodkg.models.derived_mineral_site import DerivedMineralSite
from minmodkg.models.material_form import MaterialForm
from minmodkg.models.mineral_site import MineralSite
from minmodkg.transformations import make_site_uri
from minmodkg.typing import IRI, Triple
from rdflib import RDF, Graph
from rdflib import Literal as RDFLiteral
from rdflib import URIRef

router = APIRouter(tags=["mineral_sites"])


@router.get("/mineral-sites/make-id")
def get_site_uri(source_id: str, record_id: str):
    return make_site_uri(source_id, record_id)


@router.post("/mineral-sites/find_by_ids")
def get_sites(uris: Annotated[list[str], Body(embed=True, alias="ids")]):
    sites = []
    for uri in uris:
        sites.append(get_site_by_uri(URIRef(uri)))
    return sites


@router.get("/mineral-sites/{site_id}")
def get_site(site_id: str):
    return get_site_by_uri(NS_MNR[site_id])


def get_site_by_uri(uri: URIRef) -> dict:
    g = get_site_as_graph(uri)
    # convert the graph into MineralSite
    site = MineralSite.from_graph(uri, g).model_dump(exclude_none=True)
    site.update(DerivedMineralSite.from_graph(uri, g).model_dump(exclude_none=True))
    return site


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

    if has_uri(uri):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="The site already exists.",
        )

    # when inferlink/sri create a site, the dedup site is not created yet
    if site.dedup_site_uri is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Creating dedup site is not supported yet.",
        )

    # update controlled fields and convert the site to TTL
    site.update_derived_data(user.username)
    snapshot_id = get_snapshot_id()
    g, triples = get_mineral_site_model()(
        site,
        material_form_uri_to_conversion(snapshot_id),
        crs_uri_to_name(snapshot_id),
    )

    # send the query
    resp = sparql_insert([g, triples])
    if resp.status_code != 200:
        logger.error(
            "Failed to create site:\n\t- User: {}\n\t- Input: {}\n\t- Response Code: {}\n\t- Response Message{}",
            user.username,
            site.model_dump_json(),
            resp.status_code,
            resp.text,
        )
        raise HTTPException(status_code=500, detail="Failed to create the site.")

    return get_site_by_uri(URIRef(uri))


@router.post("/mineral-sites/{site_id}")
def update_site(site_id: str, site: MineralSite, user: CurrentUserDep):
    uri = MNR_NS + site_id
    if not has_uri(uri):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="The site does not exist.",
        )

    if user.is_system():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Automated system is not allowed to update the site.",
        )

    assert site.dedup_site_uri is not None

    # update controlled fields and convert the site to TTL
    # the site must have no blank nodes as we want a fast way to compute the delta.
    site.update_derived_data(user.username)
    snapshot_id = get_snapshot_id()
    ng, ng_triples = get_mineral_site_model()(
        site,
        material_form_uri_to_conversion(snapshot_id),
        crs_uri_to_name(snapshot_id),
    )
    ng = ng_triples.to_graph(ng)

    # TODO: update the derived graph

    # start the transaction, we can read as much as we want but we can only write once
    with Transaction([uri]).transaction():
        og = get_site_as_graph(uri)
        del_triples, add_triples = get_site_changes(og, ng)
        sparql_delete_insert(del_triples, add_triples)

    return {"status": "success", "uri": uri}


def get_site_as_graph(site_uri: str, endpoint: str = SPARQL_ENDPOINT) -> Graph:
    query = (
        """
CONSTRUCT {
    ?s ?p ?o .
    ?u ?e ?v .
}
WHERE {
    ?s ?p ?o .
    OPTIONAL {
        ?s (!(owl:sameAs|rdf:type))+ ?u .
        ?u ?e ?v .
        # FILTER (?e NOT IN (owl:sameAs))
    }
    VALUES ?s { <%s> }
}
"""
        % site_uri
    )
    return sparql_construct(query, endpoint)


def get_dedup_site_as_graph(
    dedup_site_uri: str, endpoint: str = SPARQL_ENDPOINT
) -> Graph:
    query = (
        """
CONSTRUCT {
    ?s ?p ?o .
}
WHERE {
    ?s ?p ?o .
    VALUES ?s { <%s> }
}
"""
        % dedup_site_uri
    )
    return sparql_construct(query, endpoint)


def get_site_changes(current_site: Graph, new_site: Graph) -> tuple[Triples, Triples]:
    current_triples = set(current_site)
    new_triples = set(new_site)

    lock_pred = URIRef(MNO_NS + "lock")
    del_triples = Triples(
        triples=[
            (s.n3(), p.n3(), o.n3())
            for s, p, o in current_triples.difference(new_triples)
            if p != lock_pred
        ]
    )
    add_triples = Triples(
        triples=[
            (s.n3(), p.n3(), o.n3())
            for s, p, o in new_triples.difference(current_triples)
            if p != lock_pred
        ]
    )
    return del_triples, add_triples


@lru_cache()
def get_mineral_site_model() -> (
    Callable[[MineralSite, dict[str, float], dict[str, str]], tuple[Graph, Triples]]
):
    pkg_dir = Path(__file__).parent.parent.parent
    drepr_version = version("drepr-v2").strip()

    @cache(
        backend=BackendFactory.func.sqlite.pickle(
            dbdir=pkg_dir / "extractors",
            mem_persist=True,
        ),
        cache_ser_args={
            "repr_file": lambda x: f"drepr::{drepr_version}::" + file_ident(x)
        },
    )
    def make_program(repr_file: Path, prog_file: Path):
        convert(
            repr=repr_file,
            resources={},
            progfile=prog_file,
        )

    # fix me! this is problematic when the prog_file is deleted but the cache is not cleared
    make_program(
        repr_file=pkg_dir.parent / "extractors/mineral_site.yml",
        prog_file=pkg_dir / "extractors/mineral_site.py",
    )
    from minmodkg.extractors.mineral_site import main  # type: ignore

    def map(
        site: MineralSite,
        material_form_conversion: dict[str, float],
        crss: dict[str, str],
    ) -> tuple[Graph, Triples]:
        ttl_data = main(ResourceDataObject([site.get_drepr_resource()]))
        g = Graph()
        g.parse(data=ttl_data, format="turtle")
        triples = derive_data(site, material_form_conversion, crss)
        return g, triples

    return map


def derive_data(
    site: MineralSite,
    material_form_conversion: dict[str, float],
    crss: dict[str, str],
) -> Triples:
    assert site.dedup_site_uri is not None

    # get derived data
    derived_site = DerivedMineralSite.from_mineral_site(
        site, material_form_conversion, crss
    )
    triples = derived_site.get_shorten_triples()

    # add same as
    site_id = f"mnr:{derived_site.id}"
    mnr_ns_len = len(MNR_NS)
    for same_as in site.same_as:
        assert same_as.startswith(MNR_NS), same_as
        triples.append((site_id, ":same_as", f"mnr:{same_as[mnr_ns_len:]}"))

    # add dedup site information
    triples.extend(
        DedupMineralSite.from_derived_sites(
            [derived_site], site.dedup_site_uri
        ).get_shorten_triples()
    )
    return Triples(triples)


def file_ident(file: str | Path):
    file = Path(file).resolve()
    filehash = sha256(file.read_bytes()).hexdigest()
    return f"{file}::{filehash}"


@lru_cache(maxsize=1)
def crs_uri_to_name(snapshot_id: str):
    return {crs.uri: crs.name for crs in get_crs(snapshot_id)}


@lru_cache(maxsize=1)
def material_form_uri_to_conversion(snapshot_id: str):
    return {mf.uri: mf.conversion for mf in get_material_forms(snapshot_id)}
