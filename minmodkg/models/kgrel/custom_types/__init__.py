from __future__ import annotations

import orjson
from minmodkg.models.kgrel.custom_types.dedup_site_deposit_type import (
    DedupMineralSiteDepositType,
)
from minmodkg.models.kgrel.custom_types.location import (
    GeoCoordinate,
    Location,
    LocationView,
)
from minmodkg.models.kgrel.custom_types.ref_value import (
    RefGeoCoordinate,
    RefListID,
    RefValue,
)
from minmodkg.models.kgrel.custom_types.site_and_score import SiteAndScore, SiteScore
from sqlalchemy import LargeBinary, TypeDecorator


class DataclassType(TypeDecorator):
    """SqlAlchemy Type decorator to serialize dataclasses"""

    impl = LargeBinary
    cache_ok = True

    def __init__(self, cls):
        super().__init__()
        self.cls = cls

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        return orjson.dumps(value.to_dict())

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        result = orjson.loads(value)
        return self.cls.from_dict(result)


class ListDataclassType(TypeDecorator):
    """SqlAlchemy Type decorator to serialize dataclasses"""

    impl = LargeBinary
    cache_ok = True

    def __init__(self, cls):
        super().__init__()
        self.cls = cls

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        return orjson.dumps([x.to_dict() for x in value])

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        result = orjson.loads(value)
        return [self.cls.from_dict(x) for x in result]


__all__ = [
    "DataclassType",
    "ListDataclassType",
    "Location",
    "LocationView",
    "GeoCoordinate",
    "RefValue",
    "RefGeoCoordinate",
    "RefListID",
    "DedupMineralSiteDepositType",
    "SiteAndScore",
    "SiteScore",
]
