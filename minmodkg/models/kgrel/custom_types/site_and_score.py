from __future__ import annotations

from collections import namedtuple
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, NamedTuple

from minmodkg.config import DEFAULT_SOURCE_SCORE
from minmodkg.models.kgrel.user_extra import is_system_user
from minmodkg.typing import InternalID

if TYPE_CHECKING:
    from minmodkg.models.kgrel.mineral_site import MineralSite


class ExpertCmpKey(NamedTuple):
    is_expert: Literal[True]
    timestamp: int  # in nanoseconds


class SystemCmpKey(NamedTuple):
    is_expert: Literal[False]
    source_id: str
    record_id: str
    timestamp: int  # in nanoseconds


@dataclass(order=True)
class SiteScore:
    score: float
    suborder: ExpertCmpKey | SystemCmpKey

    def to_dict(self):
        return {
            "score": self.score,
            "suborder": tuple(self.suborder),
        }

    @classmethod
    def from_dict(cls, d):
        if d["suborder"][0]:
            suborder = ExpertCmpKey(*d["suborder"])
        else:
            suborder = SystemCmpKey(*d["suborder"])
        return cls(score=d["score"], suborder=suborder)

    def is_from_user(self):
        return self.suborder.is_expert

    @classmethod
    def get_score(cls, site: MineralSite):
        score = site.source_score
        if score is None or score < 0:
            score = DEFAULT_SOURCE_SCORE
        assert 0 <= score <= 1.0
        if not is_system_user(site.created_by):
            # expert get the highest priority
            return SiteScore(1.0, ExpertCmpKey(True, site.modified_at))
        return SiteScore(
            min(score, 0.99),
            SystemCmpKey(False, site.source_id, site.record_id, site.modified_at),
        )


@dataclass
class SiteAndScore:
    site_id: InternalID
    score: SiteScore

    def to_dict(self):
        return {"site_id": self.site_id, "score": self.score.to_dict()}

    @classmethod
    def from_dict(cls, d):
        return cls(d["site_id"], SiteScore.from_dict(d["score"]))
