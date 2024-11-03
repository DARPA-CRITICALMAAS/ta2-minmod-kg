from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from minmodkg.config import NS_MNO
from pydantic import BaseModel, Field
from rdflib import OWL, SKOS, Graph
from rdflib.term import Node


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

    doi: Optional[str]
    uri: str
    title: Optional[str]

    @staticmethod
    def from_graph(id: Node, g: Graph):
        return Document(
            doi=norm_literal(next(g.objects(id, NS_MNO.doi), None)),
            uri=norm_literal(next(g.objects(id, NS_MNO.uri))),
            title=norm_literal(next(g.objects(id, NS_MNO.title), None)),
        )


class BoundingBox(BaseModel):
    x_max: float
    x_min: float
    y_max: float
    y_min: float

    def to_enc_str(self):
        return f"BB:{self.x_max:.3f}_{self.x_min:.3f}_{self.y_max:.3f}_{self.y_min:.3f}"

    @staticmethod
    def from_graph(id: Node, g: Graph):
        return BoundingBox(
            x_max=norm_literal(next(g.objects(id, NS_MNO.x_max))),
            x_min=norm_literal(next(g.objects(id, NS_MNO.x_min))),
            y_max=norm_literal(next(g.objects(id, NS_MNO.y_max))),
            y_min=norm_literal(next(g.objects(id, NS_MNO.y_min))),
        )


class PageInfo(BaseModel):
    bounding_box: Optional[BoundingBox] = None
    page: int

    def to_enc_str(self):
        if self.bounding_box is None:
            return str(self.page)
        return f"PI:{self.page}|{self.bounding_box.to_enc_str()}"

    @staticmethod
    def from_graph(id: Node, g: Graph):
        return PageInfo(
            bounding_box=norm_object(
                BoundingBox, next(g.objects(id, NS_MNO.bounding_box), None), g
            ),
            page=norm_literal(next(g.objects(id, NS_MNO.page))),
        )


class Reference(BaseModel):
    document: Document
    page_info: list[PageInfo]
    comment: Optional[str]
    property: Optional[str]

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
    record_id: str
    name: Optional[str]
    created_by: list[str]
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

    @staticmethod
    def from_graph(id: Node, g: Graph):
        location_info = next(g.objects(id, NS_MNO.location_info), None)
        if location_info is not None:
            location_info = LocationInfo.from_graph(location_info, g)

        return MineralSite(
            source_id=norm_literal(next(g.objects(id, NS_MNO.source_id))),
            record_id=norm_literal(next(g.objects(id, NS_MNO.record_id))),
            name=norm_literal(next(g.objects(id, NS_MNO.name), None)),
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


class MineralInventory(BaseModel):
    category: list[CandidateEntity] = Field(default_factory=list)
    commodity: Optional[CandidateEntity] = None
    contained_metal: Optional[float] = None
    cutoff_grade: Optional[Measure] = None
    date: Optional[str] = None
    grade: Optional[Measure] = None
    material_form: Optional[CandidateEntity] = None
    ore: Optional[Measure] = None
    reference: Reference = Field()
    zone: Optional[str] = None

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


def norm_uri(val: Any) -> Any:
    return None if val is None else str(val)


def norm_literal(val: Any) -> Any:
    return None if val is None else val.value


def norm_object(cls: Any, id: Any, g: Graph) -> Any:
    return None if id is None else cls.from_graph(id, g)
