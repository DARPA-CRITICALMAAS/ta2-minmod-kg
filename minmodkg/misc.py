from __future__ import annotations

from datetime import datetime
from typing import Any, Optional, TypeVar

import pandas as pd
import requests
from shapely.errors import WKTReadingError
from shapely.geometry import GeometryCollection
from shapely.wkt import dumps, loads

DEFAULT_ENDPOINT = "https://minmod.isi.edu/sparql"
MNR_NS = "https://minmod.isi.edu/resource/"
V = TypeVar("V")


class UnconvertibleUnitError(Exception):
    pass


def group_by_key(output: list[dict], key: str) -> dict[str, list[dict]]:
    groups = {}
    for row in output:
        val = row[key]
        if val not in groups:
            groups[val] = []
        groups[val].append(row)
    return groups


def group_by_attr(output: list[V], attr: str) -> dict[str, list[V]]:
    groups = {}
    for row in output:
        val = getattr(row, attr)
        if val not in groups:
            groups[val] = []
        groups[val].append(row)
    return groups


def send_sparql_query(query, endpoint=DEFAULT_ENDPOINT):
    final_query = (
        """
    PREFIX dcterms: <http://purl.org/dc/terms/>
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
    response = requests.post(
        url=endpoint,
        data={"query": final_query},
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/sparql-results+json",  # Requesting JSON format
        },
        verify=False,  # Set to False to bypass SSL verification as per the '-k' in curl
    )
    return response


def run_sparql_query(
    query,
    endpoint=DEFAULT_ENDPOINT,
    keys: Optional[list[str]] = None,
) -> list[dict]:
    response = send_sparql_query(query, endpoint)

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
                        output[i][key] = int(row[key]["value"])
                    else:
                        output[i][key] = None
            elif val["datatype"] in {
                "http://www.w3.org/2001/XMLSchema#decimal",
                "http://www.w3.org/2001/XMLSchema#double",
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
            else:
                raise NotImplementedError(val)
    return output


def assert_isinstance(x: Any, cls: type[V]) -> V:
    assert isinstance(x, cls)
    return x


def merge_wkt(series):
    geometries = []
    for wkt in series:
        if pd.notna(wkt) and isinstance(wkt, str):
            try:
                geometry = loads(wkt)
                geometries.append(geometry)
            except Exception as e:
                print(f"Warning: Error loading WKT: {e} for WKT: {wkt}, skipping entry")

    if len(geometries) == 1:
        # return the single geometry directly
        return dumps(geometries[0])
    elif len(geometries) > 1:
        # return a GEOMETRYCOLLECTION if there are multiple geometries
        return dumps(GeometryCollection(geometries))
    else:
        # return None if there are no valid geometries
        return None
