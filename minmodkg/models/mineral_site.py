from __future__ import annotations

from datetime import datetime, timezone
from functools import cached_property, lru_cache
from importlib.metadata import version
from pathlib import Path
from typing import Callable, ClassVar, Optional

from drepr.main import convert
from drepr.models.resource import ResourceDataObject
from libactor.cache import BackendFactory, cache
from minmodkg.misc.rdf_store import (
    BaseRDFModel,
    BaseRDFQueryBuilder,
    norm_literal,
    norm_object,
    norm_uriref,
)
from minmodkg.misc.utils import file_ident
from minmodkg.models.candidate_entity import CandidateEntity
from minmodkg.models.location_info import LocationInfo
from minmodkg.models.mineral_inventory import MineralInventory
from minmodkg.models.reference import Reference
from minmodkg.transformations import make_site_uri
from minmodkg.typing import IRI, Triple
from pydantic import Field
from rdflib import Graph, URIRef
from rdflib.term import Node


class MineralSite(BaseRDFModel):
    source_id: str
    record_id: str | int
    dedup_site_uri: Optional[IRI] = None
    name: Optional[str] = None
    created_by: list[IRI] = Field(default_factory=list)
    aliases: list[str] = Field(default_factory=list)
    site_rank: Optional[str] = None
    site_type: Optional[str] = None
    same_as: list[IRI] = Field(default_factory=list)
    location_info: Optional[LocationInfo] = None
    deposit_type_candidate: list[CandidateEntity] = Field(default_factory=list)
    mineral_inventory: list[MineralInventory] = Field(default_factory=list)
    reference: list[Reference] = Field(default_factory=list)
    modified_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
    )

    class QueryBuilder(BaseRDFQueryBuilder):

        def __init__(self):
            ns = self.rdfdata.ns
            self.class_reluri = ns.mo.MineralSite
            self.fields = []

            for prop in ["source_id", "record_id", "created_by", "modified_at"]:
                self.fields.append(self.PropertyRule(ns.mo, prop))
            for prop in [
                "dedup_site_uri",
                "name",
                "aliases",
                "site_rank",
                "site_type",
                "same_as",
            ]:
                self.fields.append(self.PropertyRule(ns.mo, prop, is_optional=True))
            self.fields.extend(
                [
                    self.PropertyRule(
                        ns.mo,
                        "location_info",
                        is_optional=True,
                        target=LocationInfo.qbuilder,
                    ),
                    self.PropertyRule(
                        ns.mo,
                        "deposit_type_candidate",
                        is_optional=True,
                        target=CandidateEntity.qbuilder,
                    ),
                    self.PropertyRule(
                        ns.mo,
                        "mineral_inventory",
                        is_optional=True,
                        target=MineralInventory.qbuilder,
                    ),
                    self.PropertyRule(
                        ns.mo, "reference", is_optional=True, target=Reference.qbuilder
                    ),
                ]
            )

    qbuilder: ClassVar[QueryBuilder] = QueryBuilder()

    @cached_property
    def uri(self) -> URIRef:
        return URIRef(make_site_uri(self.source_id, self.record_id))

    @staticmethod
    def from_raw_site(raw_site: dict) -> MineralSite:
        """Convert raw mineral site stored in the Github Repository to MineralSite object.
        The input argument is not supposed to be reused after this function.
        """
        raw_site["created_by"] = [raw_site["created_by"]]
        return MineralSite.model_validate(raw_site)

    @classmethod
    def from_graph(cls, uid: Node, g: Graph):
        ns = cls.rdfdata.ns
        md = ns.md
        mo = ns.mo

        location_info = next(g.objects(uid, mo.uri("location_info")), None)
        if location_info is not None:
            location_info = LocationInfo.from_graph(location_info, g)

        return MineralSite(
            source_id=norm_literal(next(g.objects(uid, mo.uri("source_id")))),
            record_id=norm_literal(next(g.objects(uid, mo.uri("record_id")))),
            dedup_site_uri=norm_uriref(next(g.objects(uid, md.uri("dedup_site")))),
            name=norm_literal(next(g.objects(uid, ns.rdfs.uri("label")), None)),
            created_by=[
                norm_literal(val) for val in g.objects(uid, mo.uri("created_by"))
            ],
            aliases=[
                norm_literal(alias) for alias in g.objects(uid, ns.skos.uri("altLabel"))
            ],
            site_rank=norm_literal(next(g.objects(uid, mo.uri("site_rank")), None)),
            site_type=norm_literal(next(g.objects(uid, mo.uri("site_type")), None)),
            same_as=[str(same) for same in g.objects(uid, ns.owl.uri("sameAs"))],
            location_info=location_info,
            deposit_type_candidate=[
                CandidateEntity.from_graph(dep, g)
                for dep in g.objects(uid, mo.uri("deposit_type_candidate"))
            ],
            mineral_inventory=[
                MineralInventory.from_graph(inv, g)
                for inv in g.objects(uid, mo.uri("mineral_inventory"))
            ],
            reference=[
                Reference.from_graph(ref, g)
                for ref in g.objects(uid, mo.uri("reference"))
            ],
            # leverage the fact that ISO format is sortable
            modified_at=max(
                norm_literal(val) for val in g.objects(uid, mo.uri("modified_at"))
            ),
        )

    def to_graph(self) -> Graph:
        g = get_mineral_site_model()(self)
        if self.dedup_site_uri is not None:
            g.add(
                (
                    self.uri,
                    self.rdfdata.ns.md.uri("dedup_site"),
                    URIRef(self.dedup_site_uri),
                )
            )

        if len(self.same_as) > 0:
            same_as = self.rdfdata.ns.owl.uri("sameAs")
            for site in self.same_as:
                g.add((self.uri, same_as, URIRef(site)))
        return g

    def to_triples(self, triples: Optional[list[Triple]] = None) -> list[Triple]:
        g = self.to_graph()
        raise NotImplementedError()

    def update_derived_data(self, username: str):
        self.modified_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.created_by = [f"https://minmod.isi.edu/users/{username}"]
        return self

    def get_drepr_resource(self):
        obj = self.model_dump(exclude_none=True, exclude={"same_as"})
        obj["created_by"] = self.created_by[0]
        return obj


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

    def map(
        site: MineralSite,
    ) -> Graph:
        ttl_data = main(ResourceDataObject([site.get_drepr_resource()]))
        g = Graph(namespace_manager=MineralSite.rdfdata.ns.rdflib_namespace_manager)
        g.parse(data=ttl_data, format="turtle")
        return g

    return map