from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any, Generic, Mapping, Optional, TypeVar, Union

import pandas as pd
import requests
import shapely.ops
from pyproj import Transformer
from shapely.errors import WKTReadingError
from shapely.geometry import GeometryCollection
from shapely.wkt import dumps, loads

DEFAULT_ENDPOINT = "https://minmod.isi.edu/sparql"
MNR_NS = "https://minmod.isi.edu/resource/"
V = TypeVar("V")


class UnconvertibleUnitError(Exception):
    pass


def batch(size: int, *vars, return_tuple: bool = False):
    """Batch the variables into batches of size. When vars is a single variable,
    it will return a list of batched values instead of list of tuple of batched values.

    If we want to batch a single variable to a list of tuple of batched values, set
    return_tuple to True.
    """
    output = []
    n = len(vars[0])
    if len(vars) == 1 and not return_tuple:
        for i in range(0, n, size):
            output.append(vars[0][i : i + size])
    else:
        for i in range(0, n, size):
            output.append(tuple(var[i : i + size] for var in vars))
    return output


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


def merge_wkts(
    lst: list[tuple[int, Optional[str], str]], min_rank: Optional[int] = None
) -> tuple[str, str]:
    """Merge a list of WKTS with potentially different CRS into a single WKT"""
    if min_rank is None:
        min_rank = max(x[0] for x in lst)
    norm_lst: list[tuple[str, str]] = [
        (crs or "EPSG:4326", wkt) for rank, crs, wkt in lst if rank >= min_rank
    ]
    all_crs = set(x[0] for x in norm_lst)
    if len(all_crs) == 0:
        norm_crs = ""
    elif len(all_crs) == 1:
        norm_crs = all_crs.pop()
    else:
        if "EPSG:4326" in all_crs:
            norm_crs = "EPSG:4326"
        else:
            norm_crs = all_crs.pop()

        # we convert everything to norm_crs
        norm_lst = [(crs, reproject_wkt(wkt, crs, norm_crs)) for crs, wkt in norm_lst]

    # all CRS are the same
    wkts = sorted({x[1] for x in norm_lst})
    if len(wkts) > 1:
        wkt = merge_wkt(wkts)
        if wkt is None:
            wkt = ""
    else:
        wkt = wkts[0]
    return norm_crs, wkt


def reproject_wkt(wkt: str, from_crs: str, to_crs: str) -> str:
    assert from_crs.startswith("EPSG:"), from_crs
    assert to_crs.startswith("EPSG:"), to_crs

    if from_crs == to_crs:
        return wkt

    transformer = Transformer.from_crs(
        int(from_crs[len("EPSG:") :]), int(to_crs[len("EPSG:") :])
    )

    return dumps(shapely.ops.transform(transformer.transform, loads(wkt)))


class LongestPrefixIndex(Generic[V]):
    """Given a list of prefixes, we want to find the longest predefined prefixes of a given string."""

    def __init__(
        self,
        index: dict[str, LongestPrefixIndex | str],
        start: int,
        end: int,
    ) -> None:
        self.index = index
        self.start = start
        self.end = end

    @staticmethod
    def create(prefixes: list[str]):
        sorted_prefixes = sorted(prefixes, key=lambda x: len(x), reverse=True)
        if len(sorted_prefixes) == 0:
            raise Exception("No prefix provided")
        return LongestPrefixIndex._create(sorted_prefixes, 0)

    @staticmethod
    def _create(sorted_prefixes: list[str], start: int):
        shortest_ns = sorted_prefixes[-1]
        index = LongestPrefixIndex({}, start, len(shortest_ns))

        if index.start == index.end:
            index.index[""] = shortest_ns
            subindex = LongestPrefixIndex._create(sorted_prefixes[:-1], index.end)
            for key, node in subindex.index.items():
                assert key not in index.index
                index.index[key] = node
            index.end = subindex.end
            return index

        tmp = defaultdict(list)
        for i, prefix in enumerate(sorted_prefixes):
            key = prefix[index.start : index.end]
            tmp[key].append(i)

        for key, lst_prefix_idx in tmp.items():
            if len(lst_prefix_idx) == 1:
                index.index[key] = sorted_prefixes[lst_prefix_idx[0]]
            else:
                index.index[key] = LongestPrefixIndex._create(
                    [sorted_prefixes[i] for i in lst_prefix_idx], index.end
                )
        return index

    def get(self, s: str) -> Optional[str]:
        """Get prefix of a string. Return None if it is not found"""
        key = s[self.start : self.end]
        if key in self.index:
            prefix = self.index[key]
            if isinstance(prefix, LongestPrefixIndex):
                return prefix.get(s)
            return prefix if s.startswith(prefix) else None

        if "" in self.index:
            prefix = self.index[""]
            assert isinstance(prefix, str)
            return prefix if s.startswith(prefix) else None

        return None

    def __str__(self):
        """Readable version of the index"""
        stack: list[tuple[int, str, Union[str, LongestPrefixIndex]]] = list(
            reversed([(0, k, v) for k, v in self.index.items()])
        )
        out = []

        while len(stack) > 0:
            depth, key, value = stack.pop()
            indent = "    " * depth
            if isinstance(value, str):
                out.append(indent + "`" + key + "`: " + value + "\n")
            else:
                out.append(indent + "`" + key + "`:" + "\n")
                for k, v in value.index.items():
                    stack.append((depth + 1, k, v))

        return "".join(out)

    def to_dict(self):
        return {
            k: v.to_dict() if isinstance(v, LongestPrefixIndex) else v
            for k, v in self.index.items()
        }
