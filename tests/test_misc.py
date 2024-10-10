from __future__ import annotations

from minmodkg.misc import LongestPrefixIndex


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
