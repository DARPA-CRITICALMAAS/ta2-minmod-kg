from __future__ import annotations

from typing import ClassVar, Optional

from minmodkg.misc.rdf_store import norm_literal, norm_object
from minmodkg.models.base import MinModRDFModel, MinModRDFQueryBuilder
from pydantic import Field
from rdflib import Graph
from rdflib.term import Node


class BoundingBox(MinModRDFModel):
    x_max: float
    x_min: float
    y_max: float
    y_min: float

    class QueryBuilder(MinModRDFQueryBuilder):

        def __init__(self):
            ns = self.rdfdata.ns
            self.class_reluri = ns.mo.BoundingBox
            self.fields = [
                self.PropertyRule(
                    ns.mo,
                    prop,
                )
                for prop in ["x_max", "x_min", "y_max", "y_min"]
            ]

    qbuilder: ClassVar[QueryBuilder] = QueryBuilder()

    def to_enc_str(self):
        return f"BB:{self.x_max:.3f}_{self.x_min:.3f}_{self.y_max:.3f}_{self.y_min:.3f}"

    @classmethod
    def from_graph(cls, uid: Node, g: Graph):
        mo = cls.rdfdata.ns.mo
        return BoundingBox(
            x_max=norm_literal(next(g.objects(uid, mo.uri("x_max")))),
            x_min=norm_literal(next(g.objects(uid, mo.uri("x_min")))),
            y_max=norm_literal(next(g.objects(uid, mo.uri("y_max")))),
            y_min=norm_literal(next(g.objects(uid, mo.uri("y_min")))),
        )


class PageInfo(MinModRDFModel):
    bounding_box: Optional[BoundingBox] = None
    page: int

    class QueryBuilder(MinModRDFQueryBuilder):

        def __init__(self):
            ns = self.rdfdata.ns
            self.class_reluri = ns.mo.PageInfo
            self.fields = [
                self.PropertyRule(
                    ns.mo,
                    "bounding_box",
                    is_optional=True,
                    target=BoundingBox.QueryBuilder(),
                ),
                self.PropertyRule(
                    ns.mo,
                    "page",
                ),
            ]

    qbuilder: ClassVar[QueryBuilder] = QueryBuilder()

    def to_enc_str(self):
        if self.bounding_box is None:
            return str(self.page)
        return f"PI:{self.page}|{self.bounding_box.to_enc_str()}"

    @classmethod
    def from_graph(cls, uid: Node, g: Graph):
        mo = cls.rdfdata.ns.mo
        return PageInfo(
            bounding_box=norm_object(
                BoundingBox, next(g.objects(uid, mo.uri("bounding_box")), None), g
            ),
            page=norm_literal(next(g.objects(uid, mo.uri("page")))),
        )


class Document(MinModRDFModel):

    doi: Optional[str] = None
    uri: Optional[str] = None
    title: Optional[str] = None

    class QueryBuilder(MinModRDFQueryBuilder):

        def __init__(self):
            ns = self.rdfdata.ns
            self.class_reluri = ns.mo.Document
            self.fields = [
                self.PropertyRule(
                    ns.mo,
                    prop,
                    is_optional=True,
                )
                for prop in ["doi", "uri", "title"]
            ]

    qbuilder: ClassVar[QueryBuilder] = QueryBuilder()

    @classmethod
    def from_graph(cls, id: Node, g: Graph):
        mo = cls.rdfdata.ns.mo
        return Document(
            doi=norm_literal(next(g.objects(id, mo.uri("doi")), None)),
            uri=norm_literal(next(g.objects(id, mo.uri("uri")), None)),
            title=norm_literal(next(g.objects(id, mo.uri("title")), None)),
        )


class Reference(MinModRDFModel):
    document: Document
    page_info: list[PageInfo] = Field(default_factory=list)
    comment: Optional[str] = None
    property: Optional[str] = None

    class QueryBuilder(MinModRDFQueryBuilder):

        def __init__(self):
            ns = self.rdfdata.ns
            self.class_reluri = ns.mo.Reference
            self.fields = [
                self.PropertyRule(
                    ns.mo,
                    "document",
                    target=Document.qbuilder,
                ),
                self.PropertyRule(
                    ns.mo,
                    "page_info",
                    is_optional=True,
                    target=PageInfo.qbuilder,
                ),
                self.PropertyRule(
                    ns.mo,
                    "comment",
                    is_optional=True,
                ),
                self.PropertyRule(
                    ns.mo,
                    "property",
                    is_optional=True,
                ),
            ]

    qbuilder: ClassVar[QueryBuilder] = QueryBuilder()

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
