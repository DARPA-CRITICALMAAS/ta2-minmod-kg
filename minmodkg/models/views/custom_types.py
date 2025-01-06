from __future__ import annotations

from dataclasses import dataclass, field
from typing import Annotated, Literal, Optional

import orjson
from minmodkg.misc.utils import extend_unique
from minmodkg.typing import InternalID
from sqlalchemy import Text, TypeDecorator


class DataclassType(TypeDecorator):
    """SqlAlchemy Type decorator to serialize dataclasses"""

    impl = Text
    cache_ok = True

    def __init__(self, cls):
        super().__init__()
        self.cls = cls

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        return orjson.dumps(value, option=orjson.OPT_SERIALIZE_DATACLASS)

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        result = orjson.loads(value)
        return self.cls.from_dict(result)


@dataclass
class ComputedLocation:
    lat: Optional[Annotated[float, "Latitude"]] = None
    lon: Optional[Annotated[float, "Longitude"]] = None
    country: list[InternalID] = field(default_factory=list)
    state_or_province: list[InternalID] = field(default_factory=list)

    def is_empty(self):
        return (
            self.lat is None
            and self.lon is None
            and len(self.country) == 0
            and len(self.state_or_province) == 0
        )

    def to_dict(self):
        return {
            "lat": self.lat,
            "lon": self.lon,
            "country": self.country,
            "state_or_province": self.state_or_province,
        }

    def combine(self, other: Optional[ComputedLocation]):
        if other is None:
            return self

        if self.lat is None and other.lat is not None:
            self.lat = other.lat
        if self.lon is None and other.lon is not None:
            self.lon = other.lon

        self.country = extend_unique(self.country, other.country)
        self.state_or_province = extend_unique(
            self.state_or_province, other.state_or_province
        )
        return self


@dataclass
class Event:
    type: Literal["site:add", "site:update", "same-as:update"]
    args: dict
