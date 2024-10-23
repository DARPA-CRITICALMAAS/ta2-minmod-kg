from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Optional, Sequence

import httpx
from minmodkg.config import SPARQL_ENDPOINT, SPARQL_UPDATE_ENDPOINT
from rdflib import Graph

Triple = tuple[str, str, str]


@dataclass
class Triples:
    triples: list[Triple]


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
        timeout=None
    ).raise_for_status()
    return response


def sparql_construct(query: str) -> Graph:
    raise NotImplementedError()


def sparql_insert(
    query: str | Graph | Triples | Sequence[Triples | Graph],
    endpoint: str = SPARQL_UPDATE_ENDPOINT,
):
    if not isinstance(query, str):
        query_parts = ["INSERT DATA {"]

        if isinstance(query, Graph):
            for s, p, o in query:
                query_parts.append(f"\n{s.n3()} {p.n3()} {o.n3()} .")
        elif isinstance(query, Triples):
            for s, p, o in query.triples:
                query_parts.append(f"\n{s} {p} {o} .")
        else:
            assert isinstance(query, Sequence)
            for g in query:
                if isinstance(g, Graph):
                    for s, p, o in g:
                        query_parts.append(f"\n{s.n3()} {p.n3()} {o.n3()} .")
                else:
                    assert isinstance(g, Triples)
                    for s, p, o in g.triples:
                        query_parts.append(f"\n{s} {p} {o} .")

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
