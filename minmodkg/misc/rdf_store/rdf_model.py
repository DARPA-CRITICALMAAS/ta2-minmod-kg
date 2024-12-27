from __future__ import annotations

from abc import abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property, lru_cache
from time import time
from typing import (
    Annotated,
    Any,
    ClassVar,
    Iterable,
    Literal,
    Optional,
    Self,
    Sequence,
    TypeVar,
)
from uuid import uuid4

import httpx
from minmodkg.misc.exceptions import DBError, TransactionError
from minmodkg.misc.rdf_store.namespace import Namespace, SingleNS
from minmodkg.misc.rdf_store.triple_store import TripleStore
from minmodkg.misc.utils import group_by_key
from minmodkg.typing import IRI, InternalID, RelIRI, SPARQLMainQuery, Triple, Triples
from pydantic import BaseModel
from rdflib import OWL, RDF, RDFS, SKOS, XSD, Graph, URIRef
from rdflib.namespace import NamespaceManager
from rdflib.term import Literal as RDFLiteral
from rdflib.term import Node


@dataclass
class RDFMetadata:
    ns: Namespace
    store: TripleStore


class BaseRDFModel(BaseModel):
    rdfdata: ClassVar[RDFMetadata]
    qbuilder: ClassVar[BaseRDFQueryBuilder]

    @classmethod
    def from_dict(cls, json: dict) -> Self:
        return cls.model_validate(json)

    @classmethod
    def validate_dict(cls, json: dict) -> Self:
        return cls.model_validate(json)

    def to_dict(self) -> dict:
        return self.model_dump(exclude_none=True)

    @classmethod
    def from_graph(cls, uid: Node, g: Graph) -> Self:
        raise NotImplementedError()

    def to_triples(self, triples: Optional[list[Triple]] = None) -> list[Triple]:
        """Convert this model to list of triples. If the input `triples` is not None, append new triples to it and return the same input list"""
        raise NotImplementedError()

    def to_graph(self) -> Graph:
        ns = self.rdfdata.ns
        g = Graph()
        for s, p, o in self.to_triples():
            if s.startswith("http://") or s.startswith("https://"):
                subj = URIRef(s)
            else:
                prefix, name = s.split(":")
                subj = ns.namespaces[prefix].uri(name)

            if p.startswith("http://") or p.startswith("https://"):
                pred = URIRef(p)
            else:
                prefix, name = p.split(":")
                pred = ns.namespaces[prefix].uri(name)

            if o.startswith("http://") or o.startswith("https://"):
                obj = URIRef(o)
            elif o[0] == '"':
                # TODO: fix bug if we need to escape the quote
                obj = RDFLiteral(o[1:-1])
            elif o[0].isdigit() or o[0] == "-":
                obj = RDFLiteral(
                    o,
                    datatype=(
                        XSD.int
                        if (o.find(".") == -1 and o.find("e-") == -1)
                        else XSD.decimal
                    ),
                )
            else:
                prefix, name = o.split(":", 1)
                obj = ns.namespaces[prefix].uri(name)

            g.add((subj, pred, obj))
        return g

    @classmethod
    def has_uri(cls, uri: IRI | URIRef) -> bool:
        return cls.rdfdata.store.has(uri)

    @classmethod
    def has_id(cls, id: InternalID) -> bool:
        return cls.rdfdata.store.has(cls.qbuilder.class_namespace.uri(id))

    @classmethod
    def get_by_id(cls, id: InternalID) -> Self:
        uri = cls.qbuilder.class_namespace.uri(id)
        return cls.from_graph(URIRef(uri), cls.get_graph_by_uri(uri))

    @classmethod
    def get_by_uri(cls, uri: IRI | URIRef) -> Self:
        return cls.from_graph(URIRef(uri), cls.get_graph_by_uri(uri))

    @classmethod
    def get_by_uris(cls, uris: Sequence[IRI | URIRef]) -> list[Self]:
        g = cls.get_graph_by_uris(uris)
        return [cls.from_graph(URIRef(uri), g) for uri in uris]

    @classmethod
    def get_graph_by_id(cls, id: InternalID) -> Graph:
        return cls.get_graph_by_uri(cls.qbuilder.class_namespace.uri(id))

    @classmethod
    def get_graph_by_uri(cls, uri: IRI | URIRef) -> Graph:
        query = cls.qbuilder.create_get_by_uri(uri)
        return cls.rdfdata.store.construct(query)

    @classmethod
    def get_graph_by_uris(cls, uris: Sequence[IRI | URIRef]) -> Graph:
        query = cls.qbuilder.create_get_by_uris(uris)
        return cls.rdfdata.store.construct(query)

    @classmethod
    def remove_irrelevant_triples(cls, g: Graph):
        """Keep only triples that are relevant to an instance of this class. This function keeps RDF.type triples, because they are necessary to determine the class of the instance.

        Note that this function is initially used in detect del/add triples to update KG, as if we do not careful
        when loading graphs, we may load extra triples and delete them accidentally.
        However, we decide to temporary not use it as we may have some values to delete unwanted triples to keep the
        KG clean.
        """
        Class = cls.qbuilder.class_namespace.rel2abs(cls.qbuilder.class_reluri)

        # retrieve the subject of this class first
        (subj,) = list(g.subjects(RDF.type, Class))
        raise NotImplementedError()


class BaseRDFQueryBuilder:
    rdfdata: ClassVar[RDFMetadata]
    class_namespace: SingleNS
    class_reluri: RelIRI
    fields: list[PropertyRule]

    class HPg:
        """Hierarchical Paragraph.

        In this class, any child text share the same indent level, however, the child HPg has a higher indent level.
        """

        def __init__(
            self, children: Optional[list[BaseRDFQueryBuilder.HPg | str] | str] = None
        ):
            if children is None:
                children = []
            elif isinstance(children, str):
                children = [children]
            self.children: list[BaseRDFQueryBuilder.HPg | str] = children

        def to_str(self, tab_size: int = 2, indent: int = 0):
            out = []
            for child in self.children:
                if isinstance(child, str):
                    out.append(" " * indent + child)
                else:
                    out.append(child.to_str(tab_size=tab_size, indent=indent + 2))
            return "\n".join(out)

        def extend(self, another: BaseRDFQueryBuilder.HPg):
            self.children.extend(another.children)
            return self

    @dataclass
    class PropertyRule:
        ns: SingleNS
        name: str
        is_optional: bool = False
        target: Optional[BaseRDFQueryBuilder] = None

        def construct(self, source_var: str):
            """Return the CONSTRUCT part for this property"""
            HPg = BaseRDFQueryBuilder.HPg
            target_var = source_var + "__" + self.name
            property = self.ns[self.name]
            if self.target is None:
                return HPg(f"?{source_var} {property} ?{target_var} .")
            else:
                return HPg(
                    [
                        f"?{source_var} {property} ?{target_var} .",
                        self.target.construct(target_var),
                    ]
                )

        def match(self, source_var: str):
            """Return the match part for this property, the match part is used in WHERE clause"""
            HPg = BaseRDFQueryBuilder.HPg
            target_var = source_var + "__" + self.name
            property = self.ns[self.name]

            if self.target is None:
                if self.is_optional:
                    return HPg(
                        [
                            "OPTIONAL {",
                            HPg(f"?{source_var} {property} ?{target_var} ."),
                            "}",
                        ]
                    )
                else:
                    return HPg(f"?{source_var} {property} ?{target_var} .")
            else:
                if self.is_optional:
                    return HPg(
                        [
                            "OPTIONAL {",
                            HPg(
                                [
                                    f"?{source_var} {property} ?{target_var} .",
                                    self.target.match(target_var),
                                ]
                            ),
                            "}",
                        ]
                    )
                else:
                    return HPg(
                        [
                            f"?{source_var} {property} ?{target_var} .",
                            self.target.match(target_var),
                        ]
                    )

    def get_default_source_var(self):
        return "u"

    @lru_cache()
    def construct(self, source_var: Optional[str] = None) -> HPg:
        """Return the CONSTRUCT part for this class"""
        source_var = source_var or self.get_default_source_var()
        out = self.HPg()
        out.extend(
            self.HPg(f"?{source_var} {self.rdfdata.ns.rdf.type} {self.class_reluri} .")
        )
        for field in self.fields:
            out.extend(field.construct(source_var))
        return out

    @lru_cache()
    def where(self, source_var: str) -> HPg:
        source_var = source_var or self.get_default_source_var()
        return self.HPg(
            f"?{source_var} {self.rdfdata.ns.rdf.type} {self.class_reluri} ."
        ).extend(self.match(source_var))

    @lru_cache()
    def match(self, source_var: str) -> HPg:
        """Return the match part to match this object as if it is target of another object

        For example:
            # outer query
            ?ms :deposit_type_candidate ?can_ent

            # inner query (match_target for ?can_ent)
            ?can_ent :source ?source .
            ?can_ent :target ?target .
            ?can_ent :observed_name ?type .
            ?can_ent :normalized_uri ?uri .
        """
        out = self.HPg()
        for field in self.fields:
            out.extend(field.match(source_var))
        return out

    def create_get_by_uri(self, uri: IRI | URIRef) -> str:
        source_var = self.get_default_source_var()
        return "CONSTRUCT {\n%s\n} WHERE {\n%s\n  VALUES ?%s { <%s> }\n}" % (
            self.construct(source_var).to_str(indent=2),
            self.where(source_var).to_str(indent=2),
            source_var,
            uri,
        )

    def create_get_by_uris(self, uris: Sequence[IRI | URIRef]) -> str:
        source_var = self.get_default_source_var()
        return "CONSTRUCT {\n%s\n} WHERE {\n%s\n  VALUES ?%s { %s }\n}" % (
            self.construct(source_var).to_str(indent=2),
            self.where(source_var).to_str(indent=2),
            source_var,
            " ".join(f"<{uri}>" for uri in uris),
        )
