from __future__ import annotations

from datetime import datetime, timezone
from functools import cached_property
from typing import Optional

from minmodkg.config import NS_MNO
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

norm_literal = rdflib_optional_literal_to_python
norm_uri = rdflib_optional_uriref_to_python
norm_object = rdflib_optional_object_to_python


class CandidateEntity(BaseModel):
    source: str
    confidence: float
    observed_name: Optional[str] = None
    normalized_uri: Optional[str] = None

    @staticmethod
    def from_graph(id: Node, g: Graph) -> CandidateEntity:
        return CandidateEntity(
            source=norm_literal(next(g.objects(id, NS_MNO.source))),
            confidence=norm_literal(next(g.objects(id, NS_MNO.confidence))),
            observed_name=norm_literal(next(g.objects(id, NS_MNO.observed_name), None)),
            normalized_uri=norm_uri(next(g.objects(id, NS_MNO.normalized_uri), None)),
        )


class LocationInfo(BaseModel):
    country: Optional[list[CandidateEntity]] = None
    state_or_province: Optional[list[CandidateEntity]] = None
    crs: Optional[CandidateEntity] = None
    location: Optional[str] = None

    @staticmethod
    def from_graph(id: Node, g: Graph):
        return LocationInfo(
            country=[
                CandidateEntity.from_graph(c, g) for c in g.objects(id, NS_MNO.country)
            ],
            state_or_province=[
                CandidateEntity.from_graph(s, g)
                for s in g.objects(id, NS_MNO.state_or_province)
            ],
            crs=norm_object(CandidateEntity, next(g.objects(id, NS_MNO.crs), None), g),
            location=norm_literal(next(g.objects(id, NS_MNO.location), None)),
        )


class Document(BaseModel):

    doi: Optional[str] = None
    uri: Optional[str] = None
    title: Optional[str] = None

    @staticmethod
    def from_graph(id: Node, g: Graph):
        return Document(
            doi=norm_literal(next(g.objects(id, NS_MNO.doi), None)),
            uri=norm_literal(next(g.objects(id, NS_MNO.uri), None)),
            title=norm_literal(next(g.objects(id, NS_MNO.title), None)),
        )


class Reference(BaseModel):
    document: Document
    page_info: list[PageInfo] = Field(default_factory=list)
    comment: Optional[str] = None
    property: Optional[str] = None

    @staticmethod
    def from_graph(id: Node, g: Graph):
        document = Document.from_graph(next(g.objects(id, NS_MNO.document)), g)
        page_info = [
            PageInfo.from_graph(pi, g) for pi in g.objects(id, NS_MNO.page_info)
        ]

        return Reference(
            document=document,
            page_info=page_info,
            comment=norm_literal(next(g.objects(id, NS_MNO.comment), None)),
            property=norm_literal(next(g.objects(id, NS_MNO.property), None)),
        )


class MineralSite(BaseModel):
    source_id: str
    record_id: str | int
    dedup_site_uri: Optional[str] = None
    name: Optional[str] = None
    created_by: list[str] = Field(default_factory=list)
    aliases: list[str] = Field(default_factory=list)
    site_rank: Optional[str] = None
    site_type: Optional[str] = None
    same_as: list[str] = Field(default_factory=list)
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

    @staticmethod
    def from_graph(id: Node, g: Graph):
        location_info = next(g.objects(id, NS_MNO.location_info), None)
        if location_info is not None:
            location_info = LocationInfo.from_graph(location_info, g)

        return MineralSite(
            source_id=norm_literal(next(g.objects(id, NS_MNO.source_id))),
            record_id=norm_literal(next(g.objects(id, NS_MNO.record_id))),
            dedup_site_uri=norm_uri(next(g.objects(id, NS_MNO.dedup_site))),
            name=norm_literal(next(g.objects(id, RDFS.label), None)),
            created_by=[norm_literal(val) for val in g.objects(id, NS_MNO.created_by)],
            aliases=[norm_literal(alias) for alias in g.objects(id, SKOS.altLabel)],
            site_rank=norm_literal(next(g.objects(id, NS_MNO.site_rank), None)),
            site_type=norm_literal(next(g.objects(id, NS_MNO.site_type), None)),
            same_as=[norm_uri(same) for same in g.objects(id, OWL.sameAs)],
            location_info=location_info,
            deposit_type_candidate=[
                CandidateEntity.from_graph(dep, g)
                for dep in g.objects(id, NS_MNO.deposit_type_candidate)
            ],
            mineral_inventory=[
                MineralInventory.from_graph(inv, g)
                for inv in g.objects(id, NS_MNO.mineral_inventory)
            ],
            reference=[
                Reference.from_graph(ref, g) for ref in g.objects(id, NS_MNO.reference)
            ],
            # leverage the fact that ISO format is sortable
            modified_at=max(
                norm_literal(val) for val in g.objects(id, NS_MNO.modified_at)
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


class MineralInventory(BaseModel):
    category: list[CandidateEntity] = Field(default_factory=list)
    commodity: CandidateEntity
    cutoff_grade: Optional[Measure] = None
    date: Optional[str] = None
    grade: Optional[Measure] = None
    material_form: Optional[CandidateEntity] = None
    ore: Optional[Measure] = None
    reference: Reference = Field()
    zone: Optional[str | int] = None

    @staticmethod
    def from_graph(id: Node, g: Graph):
        return MineralInventory(
            category=[
                CandidateEntity.from_graph(cat, g)
                for cat in g.objects(id, NS_MNO.category)
            ],
            commodity=norm_object(
                CandidateEntity, next(g.objects(id, NS_MNO.commodity), None), g
            ),
            contained_metal=norm_literal(
                next(g.objects(id, NS_MNO.contained_metal), None)
            ),
            cutoff_grade=norm_object(
                Measure, next(g.objects(id, NS_MNO.cutoff_grade), None), g
            ),
            date=norm_literal(next(g.objects(id, NS_MNO.date), None)),
            grade=norm_object(Measure, next(g.objects(id, NS_MNO.grade), None), g),
            material_form=norm_object(
                CandidateEntity, next(g.objects(id, NS_MNO.material_form), None), g
            ),
            ore=norm_object(Measure, next(g.objects(id, NS_MNO.ore), None), g),
            reference=Reference.from_graph(next(g.objects(id, NS_MNO.reference)), g),
            zone=norm_literal(next(g.objects(id, NS_MNO.zone), None)),
        )


class Measure(BaseModel):
    value: Optional[float] = None
    unit: Optional[CandidateEntity] = None

    @staticmethod
    def from_graph(id: Node, g: Graph):
        return Measure(
            value=norm_literal(next(g.objects(id, NS_MNO.value), None)),
            unit=norm_object(
                CandidateEntity, next(g.objects(id, NS_MNO.unit), None), g
            ),
        )

    def is_missing(self) -> bool:
        return (
            self.value is None or self.unit is None or self.unit.normalized_uri is None
        )
