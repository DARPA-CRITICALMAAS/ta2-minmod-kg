from __future__ import annotations

from typing import ClassVar, Optional

from minmodkg.misc.rdf_store import norm_literal, norm_object
from minmodkg.models.base import MinModRDFModel, MinModRDFQueryBuilder
from minmodkg.models.candidate_entity import CandidateEntity
from minmodkg.models.reference import Reference
from pydantic import Field
from rdflib import Graph
from rdflib.term import Node


class Measure(MinModRDFModel):
    value: Optional[float] = None
    unit: Optional[CandidateEntity] = None

    class QueryBuilder(MinModRDFQueryBuilder):

        def __init__(self):
            ns = self.rdfdata.ns
            self.class_reluri = ns.mo.Measure
            self.fields = [
                self.PropertyRule(
                    ns.mo,
                    "value",
                    is_optional=True,
                ),
                self.PropertyRule(
                    ns.mo,
                    "unit",
                    is_optional=False,
                    target=CandidateEntity.qbuilder,
                ),
            ]

    qbuilder: ClassVar[QueryBuilder] = QueryBuilder()

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


class MineralInventory(MinModRDFModel):
    category: list[CandidateEntity] = Field(default_factory=list)
    commodity: CandidateEntity
    cutoff_grade: Optional[Measure] = None
    date: Optional[str] = None
    grade: Optional[Measure] = None
    material_form: Optional[CandidateEntity] = None
    ore: Optional[Measure] = None
    reference: Reference = Field()
    zone: Optional[str | int] = None

    class QueryBuilder(MinModRDFQueryBuilder):

        def __init__(self):
            ns = self.rdfdata.ns
            self.class_reluri = ns.mo.MineralSite
            self.fields = [
                self.PropertyRule(
                    ns.mo,
                    "category",
                    is_optional=True,
                    target=CandidateEntity.qbuilder,
                ),
                self.PropertyRule(
                    ns.mo,
                    "commodity",
                    is_optional=False,
                    target=CandidateEntity.qbuilder,
                ),
                self.PropertyRule(
                    ns.mo,
                    "reference",
                    is_optional=False,
                    target=Reference.qbuilder,
                ),
            ]

            for prop in ["cutoff_grade", "grade", "ore"]:
                self.fields.append(
                    self.PropertyRule(
                        ns.mo, prop, is_optional=True, target=Measure.qbuilder
                    )
                )
            for prop in ["date", "zone"]:
                self.fields.append(self.PropertyRule(ns.mo, prop, is_optional=True))

    qbuilder: ClassVar[QueryBuilder] = QueryBuilder()

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
