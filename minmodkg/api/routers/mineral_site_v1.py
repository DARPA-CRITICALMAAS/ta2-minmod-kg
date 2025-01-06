from __future__ import annotations

from functools import lru_cache
from typing import Annotated, Iterable, Literal

from fastapi import APIRouter, Body, HTTPException, Response, status
from loguru import logger
from minmodkg.api.dependencies import CurrentUserDep, get_snapshot_id
from minmodkg.api.models.user import UserBase, is_system_user, is_valid_user_uri
from minmodkg.api.routers.predefined_entities import get_crs, get_material_forms
from minmodkg.misc.exceptions import DBError
from minmodkg.misc.utils import mut_merge_graphs, norm_literal
from minmodkg.models.base import MINMOD_KG
from minmodkg.models.dedup_mineral_site import DedupMineralSite
from minmodkg.models.derived_mineral_site import DerivedMineralSite, GradeTonnage
from minmodkg.models.mineral_site import MineralSite
from minmodkg.transformations import make_site_uri
from minmodkg.typing import IRI, InternalID, Triple
from pydantic import BaseModel, Field
from rdflib import OWL, Graph, URIRef

router = APIRouter(tags=["mineral_sites"])


class UpdateDedupLink(BaseModel):
    """A class represents the latest dedup links"""

    sites: list[InternalID]

    @classmethod
    def get_dedup_sites(cls, site_uris: Iterable[URIRef]) -> set[URIRef]:
        resp = MINMOD_KG.query(
            "SELECT DISTINCT ?dms WHERE { ?ms md:dedup_site ?dms . VALUES ?ms { %s } }"
            % " ".join(f"<{uri}>" for uri in site_uris)
        )
        return {URIRef(row["dms"]) for row in resp}

    @classmethod
    def update_same_as(cls, groups: list[UpdateDedupLink]) -> list[DedupMineralSite]:
        ns = MINMOD_KG.ns
        dedup_site_predicate = ns.md.uri("dedup_site")
        md_commodity_predicate = ns.md.uri("commodity")

        # step 1: gather all objects that will be updated
        site_uris = set()
        for group in groups:
            for site in group.sites:
                site_uris.add(ns.mr.uri(site))

        old_dedup_site_uris = cls.get_dedup_sites(site_uris)
        all_uris = site_uris.union(old_dedup_site_uris)

        # step 2: lock the objects
        with MINMOD_KG.transaction(list(all_uris)).transaction():
            # step 3: check if this changes will affect sites that are not in the group
            # if so, we need to abort the transaction
            affected_site_uris = {
                URIRef(x["o"])
                for x in MINMOD_KG.query(
                    """
SELECT DISTINCT ?o
WHERE {
    ?s rdf:type mo:MineralSite .
    OPTIONAL { ?s owl:sameAs ?o . }
    OPTIONAL { ?o owl:sameAs ?s . }
    VALUES ?s { %s }
}
"""
                    % f" ".join(f"<{uri}>" for uri in site_uris)
                )
                if x["o"] is not None
            }

            if len(affected_site_uris.difference(site_uris)) > 0:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="The data is stale as someone updated it. Please refresh and try again.",
                )

            verified_old_dedup_site_uris = cls.get_dedup_sites(site_uris)
            if verified_old_dedup_site_uris != old_dedup_site_uris:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="The data is stale as someone updated it. Please refresh and try again.",
                )

            # step 4: gather previous data and compute the delta
            # we are going to:
            # 1. extract the same as links
            # 2. extract the dedup links
            old_dedup_sites = DedupMineralSite.get_by_uris(list(old_dedup_site_uris))
            old_dedup_sites_graph = mut_merge_graphs(
                [dms.to_graph() for dms in old_dedup_sites]
            )

            # extract all same as links and links from previous sites to the dedup sites
            old_sites_graph = MINMOD_KG.construct(
                """
CONSTRUCT {
    ?ms owl:sameAs ?ms1 .
    ?ms md:dedup_site ?dms .
    ?ms md:commodity ?commodity .
}
WHERE {
    OPTIONAL { ?ms owl:sameAs ?ms1 . }
    OPTIONAL { ?ms md:dedup_site ?dms . }

    %s

    OPTIONAL { ?derived_ms md:grade_tonnage/md:commodity ?commodity . }
    VALUES ?ms { %s }
}
                """
                % (
                    f'BIND (IRI(CONCAT("{ns.md.namespace}", SUBSTR(STR(?ms), {len(ns.mr.namespace)+1}))) as ?derived_ms)',
                    f" ".join(f"<{uri}>" for uri in site_uris),
                )
            )

            # assuming that md:dedup_site is always correctly determined by owl:sameAs
            old_site_triples = set()
            for p in [OWL.sameAs, dedup_site_predicate]:
                for s, o in old_sites_graph.subject_objects(p):
                    old_site_triples.add((s, p, o))

            new_site_triples = set()
            new_dedup_sites = []
            for group in groups:
                dedup_site_uri = ns.md.uri(DedupMineralSite.get_id(group.sites))
                group_site_uris = [ns.mr.uri(site) for site in group.sites]
                site_uri = group_site_uris[0]
                if len(group.sites) == 1:
                    new_site_triples.add((site_uri, OWL.sameAs, site_uri))
                new_site_triples.add((site_uri, dedup_site_predicate, dedup_site_uri))
                for target_site_uri in group_site_uris[1:]:
                    new_site_triples.add((site_uri, OWL.sameAs, target_site_uri))
                    new_site_triples.add(
                        (target_site_uri, dedup_site_predicate, dedup_site_uri)
                    )

                # calculate the new dedup site
                partial_new_derived_sites = []
                for site_id, site_uri in zip(group.sites, group_site_uris):
                    # TODO: we use commodity existing in the KG -- we have to
                    # ensure that we are not updating the commodity
                    partial_new_derived_sites.append(
                        DerivedMineralSite(
                            id=site_id,
                            grade_tonnage=[
                                GradeTonnage(commodity=ns.mr.id(str(commodity)))
                                for commodity in old_sites_graph.objects(
                                    site_uri, md_commodity_predicate
                                )
                            ],
                        )
                    )

                new_dedup_sites.append(
                    DedupMineralSite.from_derived_sites(
                        sites=partial_new_derived_sites,
                    ),
                )

            new_dedup_sites_graph = mut_merge_graphs(
                [dms.to_graph() for dms in new_dedup_sites]
            )

            old_triples = old_site_triples.union(iter(old_dedup_sites_graph))
            new_triples = new_site_triples.union(iter(new_dedup_sites_graph))

            nsmng = ns.rdflib_namespace_manager
            del_triples = [
                (s.n3(nsmng), p.n3(nsmng), o.n3(nsmng))
                for s, p, o in old_triples.difference(new_triples)
            ]
            add_triples = [
                (s.n3(nsmng), p.n3(nsmng), o.n3(nsmng))
                for s, p, o in new_triples.difference(old_triples)
            ]

            print(">>> same as del triples", del_triples)
            print(">>> same as add triples", add_triples)

            MINMOD_KG.delete_insert(del_triples, add_triples)
        return new_dedup_sites


class CreateMineralSite(MineralSite):
    created_by: IRI | list[IRI] = Field(default_factory=list)
    same_as: list[InternalID] = Field(default_factory=list)

    def to_mineral_site(self) -> MineralSite:
        if isinstance(self.created_by, str):
            self.created_by = [self.created_by]

        if not all(is_valid_user_uri(user) for user in self.created_by):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="The created_by field must be a valid user URI. E.g., https://minmod.isi.edu/users/u/<username>",
            )

        return MineralSite.model_construct(
            source_id=self.source_id,
            record_id=self.record_id,
            dedup_site_uri=self.dedup_site_uri,
            name=self.name,
            created_by=self.created_by,
            aliases=self.aliases,
            site_rank=self.site_rank,
            site_type=self.site_type,
            location_info=self.location_info,
            deposit_type_candidate=self.deposit_type_candidate,
            mineral_inventory=self.mineral_inventory,
            reference=self.reference,
            modified_at=self.modified_at,
        )


@router.get("/mineral-sites/make-id")
def get_site_uri(source_id: str, record_id: str):
    return make_site_uri(source_id, record_id)


@router.post("/mineral-sites/find_by_ids")
def get_sites(ids: Annotated[list[InternalID], Body(embed=True, alias="ids")]):
    mr = MINMOD_KG.ns.mr
    sites = {}
    for id in ids:
        uri = mr.uri(id)
        if not MineralSite.has_uri(uri):
            continue
        sites[id] = get_site_by_uri(uri)
    return sites


@router.post("/same-as")
def update_same_as(
    dedup_links: list[UpdateDedupLink],
    current_user: CurrentUserDep,
):
    return UpdateDedupLink.update_same_as(dedup_links)


@router.get("/mineral-sites/{site_id}")
def get_site(site_id: InternalID, format: Literal["json", "ttl"] = "json"):
    if not MineralSite.has_id(site_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="The site does not exist.",
        )
    if format == "json":
        return get_site_by_id(site_id)
    elif format == "ttl":
        g_site = MineralSite.get_graph_by_id(site_id)
        g_derived_site = DerivedMineralSite.get_graph_by_id(site_id)
        return Response(
            content=(g_site + g_derived_site).serialize(format="ttl"),
            media_type="text/turtle",
        )
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid format.",
        )


def get_site_by_id(id: InternalID) -> dict:
    site = MineralSite.get_by_id(id)
    derived_site = DerivedMineralSite.get_by_id(id)
    out = site.to_dict()
    out.update(derived_site.to_dict())
    return out


def get_site_by_uri(uri: IRI | URIRef) -> dict:
    site = MineralSite.get_by_uri(uri)
    derived_site = DerivedMineralSite.get_by_id(
        MineralSite.qbuilder.class_namespace.id(uri)
    )
    out = site.to_dict()
    out.update(derived_site.to_dict())
    return out


@router.post("/mineral-sites")
def create_site(
    create_site: Annotated[CreateMineralSite, Body()],
    user: CurrentUserDep,
):
    """Create a mineral site."""
    # To safely update the data, we need to do it in a transaction
    # There are two places where we update the data:
    #   1. The mineral site itself
    #   2. The dedup mineral site. However, the dedup mineral site can only be updated
    #      by reading the data first. This is impossible to do via Restful API as we
    #      cannot explicitly control the transaction. We have to achieve this by implementing
    #      a custom lock mechanism. We will revisit this later.
    site = create_site.to_mineral_site()
    same_as = create_site.same_as
    uri = make_site_uri(site.source_id, site.record_id)

    if MineralSite.has_uri(uri):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="The site already exists.",
        )

    # update controlled fields and convert the site to TTL
    site.update_derived_data(user)
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

    # add same as to other sites
    ns_manager = MINMOD_KG.ns.rdflib_namespace_manager
    _subj = URIRef(site.uri).n3(ns_manager)
    _pred = OWL.sameAs.n3(ns_manager)
    triples.extend(
        (
            _subj,
            _pred,
            MINMOD_KG.ns.mr.uri(same_as_id).n3(ns_manager),
        )
        for same_as_id in same_as
    )

    try:
        with MINMOD_KG.transaction([partial_dedup_site.uri]).transaction():
            # TODO: need to sure that all same as sites are having the same dedup site
            # and the dedup site is the same as the dedup site of the current site
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


class UpdateMineralSite:
    @staticmethod
    @router.put("/mineral-sites/{site_id}")
    def main(site_id: str, site: MineralSite, user: CurrentUserDep):
        site_uri = MineralSite.rdfdata.ns.mr.uri(site_id)
        if not MineralSite.has_uri(site_uri):
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

        with MINMOD_KG.transaction([site_uri, site.dedup_site_uri]).transaction():
            del_triples, add_triples = UpdateMineralSite.get_triples(
                site_id, site_uri, site, user
            )
            MINMOD_KG.delete_insert(del_triples, add_triples)

        return get_site_by_uri(URIRef(site_uri))

    @staticmethod
    def get_triples(site_id: str, site_uri: str, site: MineralSite, user: UserBase):
        # update controlled fields and convert the site to TTL
        # the site must have no blank nodes as we want a fast way to compute the delta.
        site.update_derived_data(user)
        snapshot_id = get_snapshot_id()
        derived_site, partial_dedup_site = derive_data(
            site,
            material_form_uri_to_conversion(snapshot_id),
            crs_uri_to_name(snapshot_id),
        )

        # we can update current site & derived site because we have all of the data
        # for dedup site, we do not have all, so we have to refetch and compute the differences
        # manually
        ng = site.to_graph() + derived_site.to_graph()

        og = MineralSite.get_graph_by_uri(
            site_uri
        ) + DerivedMineralSite.get_graph_by_id(site_id)

        del_triples, add_triples = get_site_changes(og, ng)
        # TODO: update dedup site
        # DedupMineralSite.get_by_uri(site.dedup_site_uri)
        return del_triples, add_triples


def get_site_changes(
    current_site: Graph, new_site: Graph
) -> tuple[list[Triple], list[Triple]]:
    ns_manager = MINMOD_KG.ns.rdflib_namespace_manager
    current_triples = {(s, p, norm_literal(o)) for s, p, o in current_site}
    new_triples = {(s, p, norm_literal(o)) for s, p, o in new_site}
    del_triples = [
        (s.n3(ns_manager), p.n3(ns_manager), o.n3(ns_manager))
        for s, p, o in current_triples.difference(new_triples)
    ]
    add_triples = [
        (s.n3(ns_manager), p.n3(ns_manager), o.n3(ns_manager))
        for s, p, o in new_triples.difference(current_triples)
    ]
    return del_triples, add_triples


def derive_data(
    site: MineralSite,
    material_form_conversion: dict[str, float],
    crss: dict[str, str],
) -> tuple[DerivedMineralSite, DedupMineralSite]:
    ns = MineralSite.rdfdata.ns
    dedup_ns = DedupMineralSite.qbuilder.class_namespace

    if site.dedup_site_uri is None:
        site.dedup_site_uri = dedup_ns.uristr(
            DedupMineralSite.get_id([ns.mr.id(site.uri)])
        )
    elif site.dedup_site_uri not in dedup_ns:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"The dedup site URI must be in the MD namespace. Got {site.dedup_site_uri}",
        )

    # get derived data
    derived_site = DerivedMineralSite.from_mineral_site(
        site, material_form_conversion, crss
    )
    partial_dedup_site = DedupMineralSite.from_derived_sites(
        [derived_site], dedup_ns.id(site.dedup_site_uri)
    )
    return derived_site, partial_dedup_site


@lru_cache(maxsize=1)
def crs_uri_to_name(snapshot_id: str):
    return {crs.uri: crs.name for crs in get_crs(snapshot_id)}


@lru_cache(maxsize=1)
def material_form_uri_to_conversion(snapshot_id: str):
    return {mf.uri: mf.conversion for mf in get_material_forms(snapshot_id)}
