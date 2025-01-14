from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from minmodkg.config import DEFAULT_SOURCE_SCORE
from minmodkg.models_v2.kgrel.user_extra import is_system_user
from minmodkg.typing import InternalID

if TYPE_CHECKING:
    from minmodkg.models_v2.kgrel.mineral_site import MineralSite


@dataclass(order=True)
class SiteScore:
    score: float
    timestamp: int  # timestamp in nanoseconds

    def to_dict(self):
        return {
            "score": self.score,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, d):
        return cls(score=d["score"], timestamp=d["timestamp"])

    def is_from_user(self):
        return self.score == 1.0

    @classmethod
    def get_score(cls, site: MineralSite):
        score = site.source_score
        if score is None or score < 0:
            score = DEFAULT_SOURCE_SCORE
        assert 0 <= score <= 1.0
        if any(not is_system_user(x) for x in site.created_by):
            # expert get the highest priority
            return SiteScore(1.0, site.modified_at)
        return SiteScore(min(score, 0.99), site.modified_at)


@dataclass
class SiteAndScore:
    site_id: InternalID
    score: SiteScore

    def to_dict(self):
        return {"site_id": self.site_id, "score": self.score.to_dict()}

    @classmethod
    def from_dict(cls, d):
        return cls(d["site_id"], SiteScore.from_dict(d["score"]))
