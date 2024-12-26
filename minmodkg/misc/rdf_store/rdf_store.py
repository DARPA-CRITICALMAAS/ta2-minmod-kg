from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from time import time
from typing import Literal, Optional, Sequence
from uuid import uuid4

import httpx
from minmodkg.misc.exceptions import DBError, TransactionError
from minmodkg.misc.rdf_store.namespace import Namespace
from minmodkg.misc.utils import group_by_key
from minmodkg.typing import IRI, SPARQLMainQuery, Triples
from rdflib import Graph, URIRef


class RDFStore:
    """Responsible for namespace & querying endpoint"""

    def __init__(self, namespace: Namespace):
        self.ns = namespace

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
        response = self._sparql_query(query)
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
        resp = self._sparql_query(query)
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

        return self._sparql_update(query)

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

        return self._sparql_update(query)

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

        return self._sparql_update("".join(parts))

    def _sparql_query(self, query: SPARQLMainQuery):
        raise NotImplementedError()

    def _sparql_update(self, query: SPARQLMainQuery):
        raise NotImplementedError()


class Sparql11RDFStore(RDFStore):
    def __init__(self, namespace: Namespace, query_endpoint: str, update_endpoint: str):
        super().__init__(namespace)
        self.query_endpoint = query_endpoint
        self.update_endpoint = update_endpoint

    def _sparql_query(self, query: SPARQLMainQuery):
        return self._sparql(query, self.query_endpoint, "application/sparql-query")

    def _sparql_update(self, query: SPARQLMainQuery):
        return self._sparql(query, self.update_endpoint, "application/sparql-update")

    def _sparql(self, query: SPARQLMainQuery, endpoint: str, content_type: str):
        """Execute a SPARQL query and ensure the response is successful"""
        response = httpx.post(
            url=endpoint,
            headers={
                "Content-Type": content_type,
                "Accept": "application/sparql-results+json",  # Requesting JSON format
            },
            content=self.prefix_part + query,
            timeout=None,
        )
        if response.status_code != 200:
            raise DBError(
                f"Failed to execute SPARQL query. Status code: {response.status_code}. Response: {response.text}",
                response,
            )
        return response


class FusekiRDFStore(RDFStore):
    def __init__(self, namespace: Namespace, query_endpoint: str, update_endpoint: str):
        super().__init__(namespace)
        self.query_endpoint = query_endpoint
        self.update_endpoint = update_endpoint

    def _sparql_query(self, query: SPARQLMainQuery):
        response = httpx.post(
            url=self.query_endpoint,
            data={"query": self.prefix_part + query},
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/sparql-results+json",  # Requesting JSON format
            },
            timeout=None,
        )
        if response.status_code != 200:
            raise DBError(
                f"Failed to execute SPARQL query. Status code: {response.status_code}. Response: {response.text}",
                response,
            )
        return response

    def _sparql_update(self, query: SPARQLMainQuery):
        response = httpx.post(
            url=self.update_endpoint,
            data={"update": self.prefix_part + query},
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/sparql-results+json",  # Requesting JSON format
            },
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
