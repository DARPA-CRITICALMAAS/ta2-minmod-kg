from __future__ import annotations

from typing import ClassVar, Optional

from minmodkg.misc.rdf_store import norm_literal, norm_object
from minmodkg.models.base import MinModRDFModel, MinModRDFQueryBuilder
from minmodkg.models.candidate_entity import CandidateEntity
from pydantic import Field
from rdflib import Graph
from rdflib.term import Node


class LocationInfo(MinModRDFModel):
    country: list[CandidateEntity] = Field(default_factory=list)
    state_or_province: list[CandidateEntity] = Field(default_factory=list)
    crs: Optional[CandidateEntity] = None
    location: Optional[str] = None

    class QueryBuilder(MinModRDFQueryBuilder):

        def __init__(self):
            ns = self.rdfdata.ns
            self.class_reluri = ns.mo.LocationInfo
            self.fields = [
                self.PropertyRule(
                    ns.mo,
                    "country",
                    is_optional=True,
                    target=CandidateEntity.qbuilder,
                ),
                self.PropertyRule(
                    ns.mo,
                    "state_or_province",
                    is_optional=True,
                    target=CandidateEntity.qbuilder,
                ),
                self.PropertyRule(
                    ns.mo,
                    "crs",
                    is_optional=True,
                    target=CandidateEntity.qbuilder,
                ),
                self.PropertyRule(
                    ns.mo,
                    "location",
                    is_optional=True,
                ),
            ]

    qbuilder: ClassVar[QueryBuilder] = QueryBuilder()

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
