from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from time import time
from typing import Literal, Optional, Sequence
from uuid import uuid4

import httpx
from minmodkg.config import MNO_NS, MNR_NS, SPARQL_ENDPOINT, SPARQL_UPDATE_ENDPOINT
from minmodkg.misc.utils import group_by_key
from rdflib import Graph

Triple = tuple[str, str, str]


@dataclass
class Triples:
    triples: Sequence[Triple] | set[Triple]


def sparql(query: str, endpoint: str, type: Literal["query", "update"] = "query"):
    """Run SPARQL query/update on the given endpoint. This function has been tested on Apache Jena Fuseki"""
    final_query = (
        """
    PREFIX dcterms: <http://purl.org/dc/terms/>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX : <https://minmod.isi.edu/ontology/>
    PREFIX mnr: <https://minmod.isi.edu/resource/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX gkbi: <https://geokb.wikibase.cloud/entity/>
    PREFIX gkbt: <https://geokb.wikibase.cloud/prop/direct/>
    PREFIX geo: <http://www.opengis.net/ont/geosparql#>
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    \n"""
        + query
    )
    # send query
    response = httpx.post(
        url=endpoint,
        data={type: final_query},
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/sparql-results+json",  # Requesting JSON format
        },
        verify=False,
        timeout=None,
    ).raise_for_status()
    return response


def sparql_construct(query: str, endpoint: str = SPARQL_ENDPOINT) -> Graph:
    resp = sparql(query, endpoint, type="query")
    g = Graph()
    g.parse(data=resp.text, format="turtle")
    return g


def sparql_delete_insert(
    delete: str | Graph | Triples | Sequence[Triples | Graph],
    insert: str | Graph | Triples | Sequence[Triples | Graph],
    endpoint: str = SPARQL_UPDATE_ENDPOINT,
):
    parts = ["DELETE {"]
    if not isinstance(delete, str):
        sub = serialize_triples(delete)
        parts.extend(sub)
    else:
        parts.append(delete)
    parts.append("\n} INSERT {")

    if not isinstance(insert, str):
        sub = serialize_triples(insert)
        parts.extend(sub)
    else:
        parts.append(insert)
    parts.append("\n} WHERE {}")
    return sparql("".join(parts), endpoint, type="update")


def sparql_delete(
    query: str | Graph | Triples | Sequence[Triples | Graph],
    endpoint: str = SPARQL_UPDATE_ENDPOINT,
):
    if not isinstance(query, str):
        query_parts = ["DELETE DATA {"]
        query_parts.extend(serialize_triples(query))
        query_parts.append("\n}")
        query = "".join(query_parts)

    return sparql(query, endpoint, type="update")


def sparql_insert(
    query: str | Graph | Triples | Sequence[Triples | Graph],
    endpoint: str = SPARQL_UPDATE_ENDPOINT,
):
    if not isinstance(query, str):
        query_parts = ["INSERT DATA {"]
        query_parts.extend(serialize_triples(query))
        query_parts.append("\n}")
        query = "".join(query_parts)

    return sparql(query, endpoint, type="update")


def sparql_query(
    query: str,
    endpoint=SPARQL_ENDPOINT,
    keys: Optional[list[str]] = None,
) -> list[dict]:
    response = sparql(query, endpoint, type="query")

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


def serialize_triples(query: Graph | Triples | Sequence[Triples | Graph]) -> list[str]:
    parts = []
    if isinstance(query, Graph):
        for s, p, o in query:
            parts.append(f"\n{s.n3()} {p.n3()} {o.n3()} .")
    elif isinstance(query, Triples):
        for s, p, o in query.triples:
            parts.append(f"\n{s} {p} {o} .")
    elif not isinstance(query, str):
        assert isinstance(query, Sequence)
        for g in query:
            if isinstance(g, Graph):
                for s, p, o in g:
                    parts.append(f"\n{s.n3()} {p.n3()} {o.n3()} .")
            else:
                assert isinstance(g, Triples)
                for s, p, o in g.triples:
                    parts.append(f"\n{s} {p} {o} .")
    return parts


def has_uri(uri: str, endpoint: str = SPARQL_ENDPOINT) -> bool:
    query = (
        """
    SELECT ?uri WHERE { 
        ?uri ?p ?o 
        VALUES ?uri { <%s> }
    }
    LIMIT 1"""
        % uri
    )
    qres = sparql_query(query, endpoint)
    return len(qres) > 0


class Transaction:
    """Steps to perform a transaction:

    t10 -> insert the lock
    t20 -> check if there is another lock.
    t30 -> perform the query/update (update query can only be done once -- because no rollback)
    t40 -> release the lcok
    """

    def __init__(
        self,
        objects: list[str],
        query_endpoint: str = SPARQL_ENDPOINT,
        update_endpoint: str = SPARQL_UPDATE_ENDPOINT,
        timeout_sec: float = 300,
    ):
        self.objects = []
        value_filter_parts = []

        for obj in objects:
            if obj.startswith("http://") or obj.startswith("https://"):
                self.objects.append(obj)
                value_filter_parts.append(f"<{obj}>")
            elif obj.startswith("mnr:"):
                self.objects.append(MNR_NS + obj[4:])
                value_filter_parts.append(obj)
            elif obj.startswith("mno:"):
                self.objects.append(MNO_NS + obj[4:])
                value_filter_parts.append(obj)
            else:
                raise ValueError("Invalid object " + obj)

        self.value_query = " ".join(value_filter_parts)
        self.query_endpoint = query_endpoint
        self.update_endpoint = update_endpoint
        self.timeout_sec = timeout_sec
        self.lock = None

    @contextmanager
    def transaction(self):
        self.insert_lock()
        if not self.does_lock_success():
            raise Exception(
                "The objects are being edited by another one. Please try again later."
            )
        # yield so the caller can perform the transaction
        yield
        self.remove_lock()

    def insert_lock(self):
        assert self.lock is None
        self.lock = f"{str(uuid4())}::{time() + self.timeout_sec}"
        sparql_insert(
            Triples([(f"<{obj}>", ":lock", f'"{self.lock}"') for obj in self.objects]),
            endpoint=self.update_endpoint,
        )

    def does_lock_success(self):
        lst = sparql_query(
            """
    SELECT ?source ?lock
    WHERE {
        ?source :lock ?lock 
        VALUES ?source { %s }
    }"""
            % self.value_query,
            keys=["source", "lock"],
            endpoint=self.query_endpoint,
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
        sparql_delete(
            Triples([(f"<{obj}>", ":lock", f'"{self.lock}"') for obj in self.objects]),
            endpoint=self.update_endpoint,
        )
