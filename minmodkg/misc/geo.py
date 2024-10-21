from __future__ import annotations

from typing import Optional

import pandas as pd
import shapely.ops
from pyproj import Transformer
from shapely.geometry import GeometryCollection
from shapely.wkt import dumps, loads


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
