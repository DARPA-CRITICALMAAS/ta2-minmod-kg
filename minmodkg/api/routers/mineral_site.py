from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from hashlib import sha256
from importlib.metadata import version
from pathlib import Path
from time import time
from typing import Callable, Optional
from uuid import uuid4

from drepr.main import convert
from drepr.models.resource import ResourceDataObject
from fastapi import APIRouter, HTTPException, status
from libactor.cache import BackendFactory, cache
from loguru import logger
from minmodkg.api.dependencies import CurrentUserDep
from minmodkg.api.models.mineral_site import (
    MineralSite,
    MineralSiteCreate,
    MineralSiteUpdate,
)
from minmodkg.config import MNO_NS, MNR_NS, SPARQL_ENDPOINT, SPARQL_UPDATE_ENDPOINT
from minmodkg.misc import (
    Transaction,
    TransactionError,
    Triples,
    sparql,
    sparql_construct,
    sparql_insert,
    sparql_query,
)
from minmodkg.transformations import make_site_uri
from rdflib import Graph

router = APIRouter(tags=["mineral_sites"])


@router.get("/mineral-sites/make-id")
def get_site_uri(source_id: str, record_id: str):
    return make_site_uri(source_id, record_id)


@router.post("/mineral-sites")
def create_site(site: MineralSiteCreate, user: CurrentUserDep):
    """Create a mineral site."""
    # To safely update the data, we need to do it in a transaction
    # There are two places where we update the data:
    #   1. The mineral site itself
    #   2. The dedup mineral site. However, the dedup mineral site can only be updated
    #      by reading the data first. This is impossible to do via Restful API as we
    #      cannot explicitly control the transaction. We have to achieve this by implementing
    #      a custom lock mechanism. We will revisit this later.
    uri = make_site_uri(site.source_id, site.record_id)

    if has_site(uri):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="The site already exists.",
        )

    # convert the site to TTL
    full_site = MineralSite(
        **site.model_dump(exclude={"same_as", "modified_at"}),
        modified_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        created_by=MNR_NS + f"users/{user.username}",
    )
    g = get_mineral_site_model()(
        ResourceDataObject([full_site.model_dump(exclude_none=True)])
    )
    reluri = uri.replace(MNR_NS, "mnr:")

    # send the query
    resp = sparql_insert(
        [
            g,
            Triples(
                [
                    (reluri, "owl:sameAs", same_as.replace(MNR_NS, "mnr:"))
                    for same_as in site.same_as
                ]
            ),
        ],
    )
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
def update_site(site_id: str, site: MineralSiteUpdate, user: CurrentUserDep):
    uri = MNR_NS + site_id
    if not has_site(uri):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="The site does not exist.",
        )

    if user.is_system():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Automated system is not allowed to update the site.",
        )

    # transform the site to TTL
    full_site = MineralSite(
        **site.model_dump(exclude={"same_as", "modified_at"}),
        modified_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        created_by=MNR_NS + f"users/{user.username}",
    )
    ng = get_mineral_site_model()(
        ResourceDataObject([full_site.model_dump(exclude_none=True)])
    )

    return {"status": "success"}

    # ng_triples = {(str(s), str(p), str(o)) for s, p, o in ng}

    # the site must have no blank nodes as we want a fast way to compute the delta.

    # start the transaction, we can only have one update at a time
    with Transaction(uri).transaction():
        og = get_site(uri)
        og_triples = {(str(s), str(p), str(o)) for s, p, o in og}

        # compute the delta
        # og_triples.difference(ng_triples)

    # if update.source is not None:
    #     pass
    # else:
    #     # we directly update the site

    return {"status": "success", "uri": uri}


def has_site(site_uri: str, endpoint: str = SPARQL_ENDPOINT) -> bool:
    query = (
        """
    SELECT ?uri WHERE { 
        ?uri ?p ?o 
        VALUES ?uri { <%s> }
    }
    LIMIT 1"""
        % site_uri
    )
    qres = sparql_query(query, endpoint)
    return len(qres) > 0


def get_site(site_uri: str, endpoint: str = SPARQL_ENDPOINT) -> Graph:
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
        FILTER (?e NOT IN (owl:sameAs))
    }
    VALUES ?s { <%s> }
}
"""
        % site_uri
    )
    return sparql_construct(query, endpoint)


@lru_cache()
def get_mineral_site_model() -> Callable[[ResourceDataObject], Graph]:
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

    def map(resource: ResourceDataObject) -> Graph:
        ttl_data = main(resource)
        g = Graph()
        g.parse(data=ttl_data, format="turtle")
        return g

    return map


def file_ident(file: str | Path):
    file = Path(file).resolve()
    filehash = sha256(file.read_bytes()).hexdigest()
    return f"{file}::{filehash}"
