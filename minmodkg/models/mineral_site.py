from __future__ import annotations

from datetime import datetime, timezone
from functools import cached_property
from typing import Optional

from minmodkg.misc.rdf_store import BaseRDFModel, norm_literal, norm_object, norm_uriref
from minmodkg.misc.sparql import (
    rdflib_optional_literal_to_python,
    rdflib_optional_object_to_python,
    rdflib_optional_uriref_to_python,
)
from minmodkg.models.page_info import PageInfo
from minmodkg.transformations import make_site_uri
from minmodkg.typing import IRI
from pydantic import BaseModel, Field
from rdflib import OWL, RDFS, SKOS, Graph
from rdflib.term import Node


class CandidateEntity(BaseRDFModel):
    source: str
    confidence: float
    observed_name: Optional[str] = None
    normalized_uri: Optional[str] = None

    @classmethod
    def from_graph(cls, uid: Node, g: Graph) -> CandidateEntity:
        mo = cls.rdfdata.ns.mo

        return CandidateEntity(
            source=norm_literal(next(g.objects(uid, mo.uri("source")))),
            confidence=norm_literal(next(g.objects(uid, mo.uri("confidence")))),
            observed_name=norm_literal(
                next(g.objects(uid, mo.uri("observed_name")), None)
            ),
            normalized_uri=norm_uriref(
                next(g.objects(uid, mo.uri("normalized_uri")), None)
            ),
        )


class LocationInfo(BaseRDFModel):
    country: list[CandidateEntity] = Field(default_factory=list)
    state_or_province: list[CandidateEntity] = Field(default_factory=list)
    crs: Optional[CandidateEntity] = None
    location: Optional[str] = None

    @classmethod
    def from_graph(cls, uid: Node, g: Graph):
        mo = cls.rdfdata.ns.mo

        return LocationInfo(
            country=[
                CandidateEntity.from_graph(c, g)
                for c in g.objects(uid, mo.uri("country"))
            ],
            state_or_province=[
                CandidateEntity.from_graph(s, g)
                for s in g.objects(uid, mo.uri("state_or_province"))
            ],
            crs=norm_object(
                CandidateEntity,
                next(g.objects(uid, mo.uri("crs")), None),
                g,
            ),
            location=norm_literal(next(g.objects(uid, mo.uri("location")), None)),
        )


class Document(BaseRDFModel):

    doi: Optional[str] = None
    uri: Optional[str] = None
    title: Optional[str] = None

    @classmethod
    def from_graph(cls, id: Node, g: Graph):
        mo = cls.rdfdata.ns.mo
        return Document(
            doi=norm_literal(next(g.objects(id, mo.uri("doi")), None)),
            uri=norm_literal(next(g.objects(id, mo.uri("uri")), None)),
            title=norm_literal(next(g.objects(id, mo.uri("title")), None)),
        )


class Reference(BaseRDFModel):
    document: Document
    page_info: list[PageInfo] = Field(default_factory=list)
    comment: Optional[str] = None
    property: Optional[str] = None

    @classmethod
    def from_graph(cls, id: Node, g: Graph):
        mo = cls.rdfdata.ns.mo
        document = Document.from_graph(next(g.objects(id, mo.uri("document"))), g)
        page_info = [
            PageInfo.from_graph(pi, g) for pi in g.objects(id, mo.uri("page_info"))
        ]

        return Reference(
            document=document,
            page_info=page_info,
            comment=norm_literal(next(g.objects(id, mo.uri("comment")), None)),
            property=norm_literal(next(g.objects(id, mo.uri("property")), None)),
        )


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

    @cached_property
    def uri(self) -> IRI:
        return make_site_uri(self.source_id, self.record_id)

    @staticmethod
    def from_raw_site(raw_site: dict) -> MineralSite:
        """Convert raw mineral site stored in the Github Repository to MineralSite object.
        The input argument is not supposed to be reused after this function.
        """
        raw_site["created_by"] = [raw_site["created_by"]]
        return MineralSite.model_validate(raw_site)

    @classmethod
    def from_graph(cls, id: Node, g: Graph):
        ns = cls.rdfdata.ns
        md = ns.md
        mo = ns.mo

        location_info = next(g.objects(id, mo.uri("location_info")), None)
        if location_info is not None:
            location_info = LocationInfo.from_graph(location_info, g)

        return MineralSite(
            source_id=norm_literal(next(g.objects(id, mo.uri("source_id")))),
            record_id=norm_literal(next(g.objects(id, mo.uri("record_id")))),
            dedup_site_uri=norm_uriref(next(g.objects(id, md.uri("dedup_site")))),
            name=norm_literal(next(g.objects(id, RDFS.label), None)),
            created_by=[
                norm_literal(val) for val in g.objects(id, mo.uri("created_by"))
            ],
            aliases=[norm_literal(alias) for alias in g.objects(id, SKOS.altLabel)],
            site_rank=norm_literal(next(g.objects(id, mo.uri("site_rank")), None)),
            site_type=norm_literal(next(g.objects(id, mo.uri("site_type")), None)),
            same_as=[str(same) for same in g.objects(id, OWL.sameAs)],
            location_info=location_info,
            deposit_type_candidate=[
                CandidateEntity.from_graph(dep, g)
                for dep in g.objects(id, mo.uri("deposit_type_candidate"))
            ],
            mineral_inventory=[
                MineralInventory.from_graph(inv, g)
                for inv in g.objects(id, mo.uri("mineral_inventory"))
            ],
            reference=[
                Reference.from_graph(ref, g)
                for ref in g.objects(id, mo.uri("reference"))
            ],
            # leverage the fact that ISO format is sortable
            modified_at=max(
                norm_literal(val) for val in g.objects(id, mo.uri("modified_at"))
            ),
        )

    def update_derived_data(self, username: str):
        self.modified_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.created_by = [f"https://minmod.isi.edu/users/{username}"]
        return self

    def get_drepr_resource(self):
        obj = self.model_dump(
            exclude_none=True, exclude={"same_as", "dedup_site_id", "grade_tonnage"}
        )
        obj["created_by"] = self.created_by[0]
        return obj


class MineralInventory(BaseRDFModel):
    category: list[CandidateEntity] = Field(default_factory=list)
    commodity: CandidateEntity
    cutoff_grade: Optional[Measure] = None
    date: Optional[str] = None
    grade: Optional[Measure] = None
    material_form: Optional[CandidateEntity] = None
    ore: Optional[Measure] = None
    reference: Reference = Field()
    zone: Optional[str | int] = None

    @classmethod
    def from_graph(cls, id: Node, g: Graph):
        mo = cls.rdfdata.ns.mo
        return MineralInventory(
            category=[
                CandidateEntity.from_graph(cat, g)
                for cat in g.objects(id, mo.uri("category"))
            ],
            commodity=CandidateEntity.from_graph(
                next(g.objects(id, mo.uri("commodity"))), g
            ),
            cutoff_grade=norm_object(
                Measure, next(g.objects(id, mo.uri("cutoff_grade")), None), g
            ),
            date=norm_literal(next(g.objects(id, mo.uri("date")), None)),
            grade=norm_object(Measure, next(g.objects(id, mo.uri("grade")), None), g),
            material_form=norm_object(
                CandidateEntity, next(g.objects(id, mo.uri("material_form")), None), g
            ),
            ore=norm_object(Measure, next(g.objects(id, mo.uri("ore")), None), g),
            reference=Reference.from_graph(next(g.objects(id, mo.uri("reference"))), g),
            zone=norm_literal(next(g.objects(id, mo.uri("zone")), None)),
        )


class Measure(BaseRDFModel):
    value: Optional[float] = None
    unit: Optional[CandidateEntity] = None

    @classmethod
    def from_graph(cls, id: Node, g: Graph):
        mo = cls.rdfdata.ns.mo
        return Measure(
            value=norm_literal(next(g.objects(id, mo.uri("value")), None)),
            unit=norm_object(
                CandidateEntity, next(g.objects(id, mo.uri("unit")), None), g
            ),
        )

    def is_missing(self) -> bool:
        return (
            self.value is None or self.unit is None or self.unit.normalized_uri is None
        )
