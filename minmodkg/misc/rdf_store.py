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
from minmodkg.misc.utils import group_by_key
from minmodkg.typing import IRI, SPARQLMainQuery, Triple, Triples
from pydantic import BaseModel
from rdflib import OWL, RDF, RDFS, SKOS, XSD, Graph, URIRef
from rdflib.namespace import NamespaceManager
from rdflib.term import Literal as RDFLiteral
from rdflib.term import Node


@dataclass
class RDFMetadata:
    ns: Namespace
    store: RDFStore


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
                    o, datatype=XSD.int if o.find(".") == -1 else XSD.float
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
    def get_by_uri(cls, uri: IRI | URIRef) -> Self:
        return cls.from_graph(URIRef(uri), cls.get_graph_by_uri(uri))

    @classmethod
    def get_by_uris(cls, uris: Sequence[IRI | URIRef]) -> list[Self]:
        g = cls.get_graph_by_uris(uris)
        return [cls.from_graph(URIRef(uri), g) for uri in uris]

    @classmethod
    def get_graph_by_uri(cls, uri: IRI | URIRef) -> Graph:
        query = cls.qbuilder.create_get_by_uri(uri)
        return cls.rdfdata.store.construct(query)

    @classmethod
    def get_graph_by_uris(cls, uris: Sequence[IRI | URIRef]) -> Graph:
        query = cls.qbuilder.create_get_by_uris(uris)
        return cls.rdfdata.store.construct(query)


class BaseRDFQueryBuilder:
    rdfdata: ClassVar[RDFMetadata]
    class_reluri: str
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

    def create_get_by_uri(self, uri: str | URIRef) -> str:
        source_var = self.get_default_source_var()
        return "CONSTRUCT {\n%s\n} WHERE {\n%s\n  VALUES ?%s { <%s> }\n}" % (
            self.construct(source_var).to_str(indent=2),
            self.where(source_var).to_str(indent=2),
            source_var,
            uri,
        )

    def create_get_by_uris(self, uris: Sequence[str | URIRef]) -> str:
        source_var = self.get_default_source_var()
        return "CONSTRUCT {\n%s\n} WHERE {\n%s\n  VALUES ?%s { %s }\n}" % (
            self.construct(source_var).to_str(indent=2),
            self.where(source_var).to_str(indent=2),
            source_var,
            " ".join(f"<{uri}>" for uri in uris),
        )


@dataclass
class SingleNS:
    alias: str
    namespace: str

    def __post_init__(self):
        assert self.namespace.endswith("/") or self.namespace.endswith(
            "#"
        ), f"Namespace {self.namespace} should end with / or #"

    def id(self, uri: str | URIRef) -> str:
        return uri[len(self.namespace) :]

    def uri(self, name: str) -> URIRef:
        return URIRef(self.namespace + name)

    def uristr(self, name: str) -> str:
        return self.namespace + name

    def __getattr__(self, name: str):
        return self.alias + ":" + name

    def __getitem__(self, name: str):
        return self.alias + ":" + name


class Namespace:
    def __init__(self, ns_cfg: dict):
        self.mr = SingleNS("mr", ns_cfg["mr"])
        self.mo = SingleNS("mo", ns_cfg["mo"])
        self.md = SingleNS("md", ns_cfg["mo-derived"])
        self.dcterms = SingleNS("dcterms", "http://purl.org/dc/terms/")
        self.rdf = SingleNS("rdf", str(RDF))
        self.rdfs = SingleNS("rdfs", str(RDFS))
        self.xsd = SingleNS("xsd", str(XSD))
        self.owl = SingleNS("owl", str(OWL))
        self.gkbi = SingleNS("gkbi", "https://geokb.wikibase.cloud/entity/")
        self.gkbt = SingleNS("gkbt", "https://geokb.wikibase.cloud/prop/direct/")
        self.geo = SingleNS("geo", "http://www.opengis.net/ont/geosparql#")
        self.skos = SingleNS("skos", str(SKOS))

        self.namespaces = {
            x.alias: x for x in self.__dict__.values() if isinstance(x, SingleNS)
        }

    def iter(self) -> Iterable[SingleNS]:
        return self.namespaces.values()

    def get_by_alias(self, alias: str) -> SingleNS:
        return self.namespaces[alias]

    @cached_property
    def rdflib_namespace_manager(self) -> NamespaceManager:
        nsmanager = NamespaceManager(Graph(), bind_namespaces="none")
        for ns in self.iter():
            nsmanager.bind(ns.alias, ns.namespace)
        return nsmanager


class RDFStore:
    """Responsible for namespace & querying endpoint"""

    def __init__(self, namespace: Namespace, query_endpoint: str, update_endpoint: str):
        self.ns = namespace
        self.query_endpoint = query_endpoint
        self.update_endpoint = update_endpoint

        # parts needed to make SPARQL queries
        self.prefix_part = (
            "\n".join(f"PREFIX {x.alias}: <{x.namespace}>" for x in self.ns.iter())
            + "\n\n"
        )

    def transaction(self, objects: Sequence[IRI | URIRef], timeout_sec: float = 300):
        return Transaction(self, objects, timeout_sec)

    def has(self, uri: IRI | URIRef):
        return (
            len(
                self.query(
                    "select 1 where { <%s> ?p ?o } LIMIT 1" % uri,
                )
            )
            > 0
        )

    def query(self, query: str, keys: Optional[list[str]] = None) -> list[dict]:
        response = self._sparql(query, self.query_endpoint, type="query")
        qres = response.json()["results"]["bindings"]

        if len(qres) == 0:
            return []

        sample = {}
        for res in qres:
            for key in res:
                if key not in sample:
                    sample[key] = res[key]
        if keys is not None:
            for key in keys:
                if key not in sample:
                    sample[key] = {"type": "bnode"}

        output = [{} for _ in range(len(qres))]
        for key, val in sample.items():
            if val["type"] in {"uri", "bnode"}:
                for i, row in enumerate(qres):
                    if key in row:
                        output[i][key] = row[key]["value"]
                    else:
                        output[i][key] = None
            else:
                assert val["type"] == "literal", val["type"]
                if val.get("datatype") in {
                    None,
                    "http://www.opengis.net/ont/geosparql#wktLiteral",
                }:
                    for i, row in enumerate(qres):
                        if key in row:
                            output[i][key] = row[key]["value"]
                        else:
                            output[i][key] = None
                elif val["datatype"] in {
                    "http://www.w3.org/2001/XMLSchema#integer",
                    "http://www.w3.org/2001/XMLSchema#int",
                }:
                    for i, row in enumerate(qres):
                        if key in row:
                            try:
                                output[i][key] = int(row[key]["value"])
                            except ValueError:
                                # we think it's an integer but it's not, so we don't attempt to parse this column
                                for i, row in enumerate(qres):
                                    if key in row:
                                        output[i][key] = row[key]["value"]
                                    else:
                                        output[i][key] = None
                                break
                        else:
                            output[i][key] = None
                elif val["datatype"] in {
                    "http://www.w3.org/2001/XMLSchema#decimal",
                    "http://www.w3.org/2001/XMLSchema#double",
                    "http://www.w3.org/2001/XMLSchema#float",
                }:
                    for i, row in enumerate(qres):
                        if key in row:
                            output[i][key] = float(row[key]["value"])
                        else:
                            output[i][key] = None
                elif val["datatype"] == "http://www.w3.org/2001/XMLSchema#date":
                    for i, row in enumerate(qres):
                        if key in row:
                            rowval = row[key]["value"]
                            try:
                                output[i][key] = datetime.strptime(rowval, "%Y-%m-%d")
                            except ValueError:
                                output[i][key] = datetime.strptime(rowval, "%Y-%m")
                        else:
                            output[i][key] = None
                elif val["datatype"] == "http://www.w3.org/2001/XMLSchema#boolean":
                    for i, row in enumerate(qres):
                        if key in row:
                            output[i][key] = row[key]["value"] == "true"
                        else:
                            output[i][key] = None
                else:
                    raise NotImplementedError(val)
        return output

    def construct(self, query: str):
        resp = self._sparql(query, self.query_endpoint, type="query")
        g = Graph()
        g.parse(data=resp.text, format="turtle")
        return g

    def delete(
        self,
        query: str | Triples,
    ):
        if not isinstance(query, str):
            parts = ["DELETE DATA {"]
            parts.extend((f"\n{s} {p} {o}." for s, p, o in query))
            parts.append("\n}")
            query = "".join(parts)

        return self._sparql(query, self.update_endpoint, type="update")

    def insert(
        self,
        query: str | Triples | Graph,
    ):
        if not isinstance(query, str):
            parts = ["INSERT DATA {"]
            if isinstance(query, Graph):
                ns_manager = self.ns.rdflib_namespace_manager
                parts.extend(
                    f"\n{s.n3(ns_manager)} {p.n3(ns_manager)} {o.n3(ns_manager)} ."
                    for s, p, o in query
                )
            else:
                parts.extend((f"\n{s} {p} {o} ." for s, p, o in query))
            parts.append("\n}")
            query = "".join(parts)

        return self._sparql(query, self.update_endpoint, type="update")

    def delete_insert(
        self,
        delete: str | Triples,
        insert: str | Triples,
    ):
        parts = ["DELETE {"]
        if not isinstance(delete, str):
            parts.extend((f"\n{s} {p} {o}." for s, p, o in delete))
        else:
            parts.append(delete)
        parts.append("\n} INSERT {")

        if not isinstance(insert, str):
            parts.extend((f"\n{s} {p} {o}." for s, p, o in insert))
        else:
            parts.append(insert)
        parts.append("\n} WHERE {}")

        return self._sparql("".join(parts), self.update_endpoint, type="update")

    def _sparql(
        self,
        query: SPARQLMainQuery,
        endpoint: str,
        type: Literal["query", "update"] = "query",
    ):
        """Execute a SPARQL query and ensure the response is successful"""
        response = httpx.post(
            url=endpoint,
            data={type: self.prefix_part + query},
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/sparql-results+json",  # Requesting JSON format
            },
            verify=False,
            timeout=None,
        )
        if response.status_code != 200:
            raise DBError(
                f"Failed to execute SPARQL query. Status code: {response.status_code}. Response: {response.text}",
                response,
            )
        return response


class Transaction:
    """Steps to perform a transaction:

    t10 -> insert the lock
    t20 -> check if there is another lock.
    t30 -> perform the query/update (update query can only be done once -- because no rollback)
    t40 -> release the lcok
    """

    def __init__(
        self,
        kg: RDFStore,
        objects: Sequence[IRI | URIRef],
        timeout_sec: float = 300,
    ):
        self.objects: list[IRI] = []
        value_filter_parts = []

        for obj in objects:
            assert obj.startswith("http://") or obj.startswith("https://"), obj
            self.objects.append(str(obj))
            value_filter_parts.append(f"<{obj}>")

        self.value_query = " ".join(value_filter_parts)
        self.timeout_sec = timeout_sec
        self.lock = None

        self.kg = kg
        assert self.kg.ns.mo.alias == "mo", "Our query assume mo has alias `mo`"

    @contextmanager
    def transaction(self):
        self.insert_lock()
        try:
            if not self.does_lock_success():
                raise TransactionError(
                    "The objects are being edited by another one. Please try again later."
                )
            # yield so the caller can perform the transaction
            yield
        finally:
            self.remove_lock()

    def insert_lock(self):
        assert self.lock is None
        self.lock = f"{str(uuid4())}::{time() + self.timeout_sec}"
        self.kg.insert(
            [(f"<{obj}>", "mo:lock", f'"{self.lock}"') for obj in self.objects],
        )

    def does_lock_success(self):
        lst = self.kg.query(
            """
    SELECT ?source ?lock
    WHERE {
        ?source mo:lock ?lock 
        VALUES ?source { %s }
    }"""
            % self.value_query,
            keys=["source", "lock"],
        )

        obj2locks: dict[str, list[str]] = group_by_key(lst, key="source", value="lock")
        if len(obj2locks) != len(self.objects):
            return False

        now = time()
        for obj in self.objects:
            if obj not in obj2locks:
                return False
            locks = [
                lock
                for lock in obj2locks[obj]
                if lock == self.lock or float(lock.split("::")[1]) >= now
            ]
            if len(locks) != 1 or locks[0] != self.lock:
                return False

        return True

    def remove_lock(self):
        self.kg.delete(
            [(f"<{obj}>", "mo:lock", f'"{self.lock}"') for obj in self.objects]
        )


def norm_literal(value: Annotated[Any, RDFLiteral]) -> Any:
    return None if value is None else (value.value or str(value))


def norm_uriref(value: Annotated[Any, URIRef]) -> Optional[URIRef]:
    return None if value is None else value


M = TypeVar("M", bound=BaseRDFModel)


def norm_object(clz: type[M], id: Optional[Node], g: Graph) -> Optional[M]:
    return None if id is None else clz.from_graph(id, g)
