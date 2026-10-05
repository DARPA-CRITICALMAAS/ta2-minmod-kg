from __future__ import annotations

import shapely.wkt
from minmodkg.misc import LongestPrefixIndex, reproject_wkt
from minmodkg.misc.geo import reproject_geometry
from shapely.geometry import Point


def test_longest_prefix_index():
    index = LongestPrefixIndex.create(
        [
            "article::",
            "article::http://example.com/",
            "article::http://example.com/1",
            "databases::http://usgs.gov/",
        ]
    )
    assert (
        index.get("article::http://example.com/10") == "article::http://example.com/1"
    )

    assert index.get("article::http://example.com/2") == "article::http://example.com/"
    assert index.get("article::http://abc.com") == "article::"
    assert index.get("databases::http://usgs.gov/1") == "databases::http://usgs.gov/"
    assert index.get("databases::http://mrdata") is None
    assert index.get("mining-report::") is None


# Musick mine, Bohemia district, Oregon (DOGAMI record ID_17360), as stored in
# NAD83(CORS96) / Oregon Lambert (ft). Oregon spans lon -124.71..-116.45 and
# lat 41.98..46.30.
OREGON_LAMBERT_FT = "POINT (741725.31 674269.18)"


def test_reproject_to_wgs84_returns_lon_lat():
    # WKT is (x=lon, y=lat) on both sides; EPSG:4326's own axis order is
    # (lat, lon), which used to come back swapped.
    out = shapely.wkt.loads(reproject_wkt(OREGON_LAMBERT_FT, "EPSG:2994", "EPSG:4326"))
    assert -124.71 <= out.x <= -116.45 and 41.98 <= out.y <= 46.30

    out = reproject_geometry(
        shapely.wkt.loads(OREGON_LAMBERT_FT), "EPSG:2994", "EPSG:4326"
    )
    assert -124.71 <= out.x <= -116.45 and 41.98 <= out.y <= 46.30


def test_reproject_same_crs_returns_input_unchanged():
    wkt = "POINT (-122.6539 43.5792)"
    assert reproject_wkt(wkt, "EPSG:4326", "EPSG:4326") == wkt

    point = Point(-122.6539, 43.5792)
    assert reproject_geometry(point, "EPSG:4326", "EPSG:4326") is point
