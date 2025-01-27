from __future__ import annotations

from minmodkg.models.kgrel.custom_types.site_and_score import (
    ExpertCmpKey,
    SiteScore,
    SystemCmpKey,
)


def test_site_score():
    assert SiteScore(1.0, ExpertCmpKey(True, 5)) > SiteScore(1.0, ExpertCmpKey(True, 4))
    assert SiteScore(1.0, ExpertCmpKey(True, 5)) < SiteScore(1.0, ExpertCmpKey(True, 6))
    assert SiteScore(1.0, ExpertCmpKey(True, 5)) > SiteScore(
        0.1, SystemCmpKey(False, "mrds", "1001", 6)
    )

    assert sorted(
        [
            SiteScore(0.8, SystemCmpKey(False, "ni-43-101", "1002", 6)),
            SiteScore(0.8, SystemCmpKey(False, "ni-43-101", "1002", 8)),
            SiteScore(0.8, SystemCmpKey(False, "ni-43-101", "1003", 2)),
            SiteScore(0.8, SystemCmpKey(False, "ni-43-101", "1003", 1)),
            SiteScore(1.0, ExpertCmpKey(True, 5)),
        ],
        reverse=True,
    ) == [
        SiteScore(1.0, ExpertCmpKey(True, 5)),
        SiteScore(0.8, SystemCmpKey(False, "ni-43-101", "1003", 2)),
        SiteScore(0.8, SystemCmpKey(False, "ni-43-101", "1003", 1)),
        SiteScore(0.8, SystemCmpKey(False, "ni-43-101", "1002", 8)),
        SiteScore(0.8, SystemCmpKey(False, "ni-43-101", "1002", 6)),
    ]
