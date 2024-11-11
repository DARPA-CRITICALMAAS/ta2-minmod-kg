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
from minmodkg.api.dependencies import CurrentUserDep
from minmodkg.api.models.mineral_site import MineralSite
from minmodkg.config import MNO_NS, MNR_NS, NS_MNO, NS_MNR, SPARQL_ENDPOINT
from minmodkg.misc import (
    Transaction,
    Triples,
    has_uri,
    sparql_construct,
    sparql_delete_insert,
    sparql_insert,
)
from minmodkg.transformations import make_site_uri
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
        g = get_site_as_graph(uri)
        # convert the graph into MineralSite
        site = MineralSite.from_graph(URIRef(uri), g).model_dump(exclude_none=True)
        site["uri"] = uri
        sites.append(site)
    return sites


@router.get("/mineral-sites/{site_id}")
def get_site(site_id: str):
    uri = NS_MNR[site_id]
    g = get_site_as_graph(uri)
    # convert the graph into MineralSite
    site = MineralSite.from_graph(uri, g).model_dump(exclude_none=True)
    site["id"] = site_id
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
    g = get_mineral_site_model()(site)

    # send the query
    resp = sparql_insert(g)
    if resp.status_code != 200:
        logger.error(
            "Failed to create site:\n\t- User: {}\n\t- Input: {}\n\t- Response Code: {}\n\t- Response Message{}",
            user.username,
            site.model_dump_json(),
            resp.status_code,
            resp.text,
        )
        raise HTTPException(status_code=500, detail="Failed to create the site.")

    return {"status": "success", "uri": uri}


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
    ng = get_mineral_site_model()(site)

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
def get_mineral_site_model() -> Callable[[MineralSite], Graph]:
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

    def map(site: MineralSite) -> Graph:
        ttl_data = main(ResourceDataObject([site.get_drepr_resource()]))
        g = Graph()
        g.parse(data=ttl_data, format="turtle")
        derive_data(g, site)
        return g

    return map


def derive_data(g: Graph, site: MineralSite):
    assert site.dedup_site_uri is not None

    # manually parse same as and dedup site
    site_uri = next(g.subjects(RDF.type, NS_MNO.MineralSite))
    dedup_site_uri = URIRef(site.dedup_site_uri)
    site_gtcom_uri_prefix = str(site_uri) + "__gt__"
    print(">>>", site.grade_tonnage)

    # add link to dedup site
    g.add((site_uri, NS_MNO.dedup_site, dedup_site_uri))
    # add same as links
    for same_as in site.same_as:
        g.add((site_uri, NS_MNR.same_as, URIRef(same_as)))
    # add grade tonnage
    for gtcom in site.grade_tonnage:
        assert gtcom.commodity.startswith(MNR_NS), gtcom.commodity
        gtnode_uri = URIRef(site_gtcom_uri_prefix + gtcom.commodity[len(MNR_NS) :])

        g.add((site_uri, NS_MNO.grade_tonnage, gtnode_uri))
        g.add((gtnode_uri, NS_MNO.commodity, URIRef(gtcom.commodity)))
        if gtcom.total_contained_metal is not None:
            g.add(
                (
                    gtnode_uri,
                    NS_MNO.total_contained_metal,
                    RDFLiteral(gtcom.total_contained_metal),
                )
            )
            g.add(
                (
                    gtnode_uri,
                    NS_MNO.total_tonnage,
                    RDFLiteral(gtcom.total_tonnage),
                )
            )
            g.add(
                (
                    gtnode_uri,
                    NS_MNO.total_grade,
                    RDFLiteral(gtcom.total_grade),
                )
            )

    # for the dedup site, we need to add back the site link and the commodity
    # TODO: fix me!!
    g.add((dedup_site_uri, RDF.type, NS_MNO.DedupMineralSite))
    g.add((dedup_site_uri, NS_MNO.site, site_uri))
    for gtcom in site.grade_tonnage:
        g.add((dedup_site_uri, NS_MNO.commodity, URIRef(gtcom.commodity)))
    return None


def file_ident(file: str | Path):
    file = Path(file).resolve()
    filehash = sha256(file.read_bytes()).hexdigest()
    return f"{file}::{filehash}"
