from __future__ import annotations

from typing import ClassVar, Optional

from minmodkg.misc.rdf_store import norm_literal, norm_uriref
from minmodkg.models.base import MinModRDFModel, MinModRDFQueryBuilder
from rdflib import Graph
from rdflib.term import Node


class CandidateEntity(MinModRDFModel):
    source: str
    confidence: float
    observed_name: Optional[str] = None
    normalized_uri: Optional[str] = None

    class QueryBuilder(MinModRDFQueryBuilder):

        def __init__(self):
            ns = self.rdfdata.ns
            self.class_reluri = ns.mo.CandidateEntity
            self.fields = [
                self.PropertyRule(ns.mo, "source"),
                self.PropertyRule(ns.mo, "confidence"),
                self.PropertyRule(ns.mo, "observed_name", is_optional=True),
                self.PropertyRule(ns.mo, "normalized_uri", is_optional=True),
            ]

    qbuilder: ClassVar[QueryBuilder] = QueryBuilder()

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
