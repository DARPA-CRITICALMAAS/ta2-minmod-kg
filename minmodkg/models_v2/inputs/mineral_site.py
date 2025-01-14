from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import cached_property
from typing import TYPE_CHECKING, Annotated, Optional, Union

from minmodkg.misc.utils import (
    extend_unique,
    format_datetime,
    format_nanoseconds,
    makedict,
)
from minmodkg.models_v2.inputs.candidate_entity import CandidateEntity
from minmodkg.models_v2.inputs.location_info import LocationInfo
from minmodkg.models_v2.inputs.mineral_inventory import MineralInventory
from minmodkg.models_v2.inputs.reference import Reference
from minmodkg.transformations import make_site_uri
from minmodkg.typing import InternalID
from rdflib import URIRef

if TYPE_CHECKING:
    from minmodkg.models_v2.kgrel.mineral_site import MineralSite as RelMineralSite


@dataclass
class MineralSite:
    source_id: str
    record_id: Union[str, int]
    name: Optional[str] = None
    aliases: list[str] = field(default_factory=list)
    site_rank: Optional[str] = None
    site_type: Optional[str] = None
    location_info: Optional[LocationInfo] = None
    deposit_type_candidate: list[CandidateEntity] = field(default_factory=list)
    mineral_inventory: list[MineralInventory] = field(default_factory=list)
    reference: list[Reference] = field(default_factory=list)

    created_by: list[str] = field(default_factory=list)
    modified_at: Annotated[str, "Datetime with %Y-%m-%dT%H:%M:%S.%fZ format"] = field(
        default_factory=lambda: format_datetime(datetime.now(timezone.utc))
    )

    @cached_property
    def uri(self) -> URIRef:
        return URIRef(make_site_uri(self.source_id, self.record_id))

    @cached_property
    def id(self) -> InternalID:
        return make_site_uri(self.source_id, self.record_id, namespace="")

    def to_dict(self):
        return makedict.without_none_or_empty_list(
            (
                ("source_id", self.source_id),
                ("record_id", self.record_id),
                ("name", self.name),
                ("aliases", self.aliases),
                ("site_rank", self.site_rank),
                ("site_type", self.site_type),
                (
                    "location_info",
                    (
                        self.location_info.to_dict()
                        if self.location_info is not None
                        else None
                    ),
                ),
                (
                    "deposit_type_candidate",
                    [x.to_dict() for x in self.deposit_type_candidate],
                ),
                ("mineral_inventory", [x.to_dict() for x in self.mineral_inventory]),
                ("reference", [x.to_dict() for x in self.reference]),
                ("created_by", self.created_by),
                ("modified_at", self.modified_at),
            )
        )

    @classmethod
    def from_dict(cls, d: dict):
        return cls(
            source_id=d["source_id"],
            record_id=d["record_id"],
            name=d.get("name"),
            aliases=d.get("aliases", []),
            site_rank=d.get("site_rank"),
            site_type=d.get("site_type"),
            location_info=(
                LocationInfo.from_dict(d["location_info"])
                if d.get("location_info")
                else None
            ),
            deposit_type_candidate=[
                CandidateEntity.from_dict(x)
                for x in d.get("deposit_type_candidate", [])
            ],
            mineral_inventory=[
                MineralInventory.from_dict(x) for x in d.get("mineral_inventory", [])
            ],
            reference=[Reference.from_dict(x) for x in d.get("reference", [])],
            created_by=(
                d["created_by"]
                if isinstance(d["created_by"], list)
                else [d["created_by"]]
            ),
            modified_at=d["modified_at"],
        )

    def merge_mut(self, other: MineralSite):
        """Merge another site into this site almost as how knowledge graph does. This behaves similar to convert these sites into triples and merge the triples
        except that field that is expected to be single value will not be overwritten.
        """
        if self.name is None:
            self.name = other.name
        self.aliases = extend_unique(self.aliases, other.aliases)
        if self.site_rank is None:
            self.site_rank = other.site_rank
        if self.site_type is None:
            self.site_type = other.site_type
        if self.location_info is None:
            self.location_info = other.location_info
        elif other.location_info is not None:
            self.location_info.merge_mut(other.location_info)

        self.deposit_type_candidate = extend_unique(
            self.deposit_type_candidate,
            other.deposit_type_candidate,
            key_fn=CandidateEntity.to_tuple,
        )

        self.mineral_inventory.extend(other.mineral_inventory)
        self.reference = Reference.dedup(self.reference + other.reference)

        self.created_by = extend_unique(self.created_by, other.created_by)
        self.modified_at = max(self.modified_at, other.modified_at)

    @classmethod
    def from_kgrel(cls, site: RelMineralSite):
        return cls(
            source_id=site.source_id,
            record_id=site.record_id,
            name=site.name,
            aliases=site.aliases,
            site_rank=site.rank,
            site_type=site.type,
            location_info=(
                LocationInfo(
                    country=site.location.country,
                    state_or_province=site.location.state_or_province,
                    crs=site.location.crs,
                    location=site.location.coordinates,
                )
                if site.location is not None
                else None
            ),
            deposit_type_candidate=site.deposit_type_candidates,
            mineral_inventory=site.inventories,
            reference=site.reference,
            created_by=site.created_by,
            modified_at=format_nanoseconds(site.modified_at),
        )
