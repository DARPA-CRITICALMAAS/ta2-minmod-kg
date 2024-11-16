from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import (
    Annotated,
    Any,
    ClassVar,
    Generic,
    Iterable,
    Literal,
    Optional,
    Self,
    TypeVar,
)

import httpx
from minmodkg.typing import SPARQLMainQuery, T, Triple, Triples, V
from pydantic import BaseModel
from rdflib import OWL, RDF, RDFS, SKOS, XSD, Graph, URIRef
from rdflib.term import Literal as RDFLiteral
from rdflib.term import Node


@dataclass
class RDFMetadata:
    ns: Namespace
    store: RDFStore


class BaseRDFModel(BaseModel):
    rdfdata: ClassVar[RDFMetadata]

    @classmethod
    def from_json(cls, json: dict) -> Self:
        return cls.model_validate(json)

    @classmethod
    def validate_json(cls, json: dict) -> Self:
        return cls.model_validate(json)

    def to_json(self) -> dict:
        return self.model_dump(exclude_none=True)

    @classmethod
    def from_graph(cls, uid: Node, g: Graph) -> Self:
        raise NotImplementedError()

    def to_triples(self) -> list[Triple]:
        raise NotImplementedError()


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

    def __getattr__(self, name: str):
        return self.alias + ":" + name

    def __getitem__(self, name: str):
        return self.alias + ":" + name


class Namespace:
    def __init__(self, ns_cfg: dict):
        self.mr = SingleNS("mr", ns_cfg["mr"])
        self.mo = SingleNS("mo", ns_cfg["mo"])
        self.md = SingleNS("md", ns_cfg["mr-derived"])
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


class RDFStore:
    """Responsible for namespace & querying endpoint"""

    def __init__(self, namespace: Namespace, query_endpoint: str, update_endpoint: str):
        self.ns = namespace
        self.query_endpoint = query_endpoint
        self.update_endpoint = update_endpoint

        # parts needed to make SPARQL queries
        self.prefix_part = "\n".join(
            f"PREFIX {x.alias}: <{x.namespace}>" for x in self.ns.iter()
        )

    def has(self, rel_uri: str):
        return (
            len(
                self.query(
                    "select 1 where { %s ?p ?o } LIMIT 1" % rel_uri,
                )
            )
            > 0
        )

    def query(self, query: str, keys: Optional[list[str]] = None) -> list[dict]:
        response = self._sparql(query, self.query_endpoint, type="query")
        if response.status_code != 200:
            raise Exception(response.text)

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
        query: str | Triples,
    ):
        if not isinstance(query, str):
            parts = ["INSERT DATA {"]
            parts.extend((f"\n{s} {p} {o}." for s, p, o in query))
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
        return response


class Transaction: ...


def norm_literal(value: Annotated[Any, RDFLiteral]) -> Any:
    return None if value is None else (value.value or str(value))


def norm_uriref(value: Annotated[Any, URIRef]) -> Optional[URIRef]:
    return None if value is None else value


M = TypeVar("M", bound=BaseRDFModel)


def norm_object(clz: type[M], id: Node, g: Graph) -> Optional[M]:
    return None if id is None else clz.from_graph(id, g)
