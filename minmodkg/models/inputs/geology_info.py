from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from minmodkg.misc.utils import makedict
from minmodkg.typing import NotEmptyStr


@dataclass
class RockType:
    unit: Optional[NotEmptyStr] = None
    type: Optional[NotEmptyStr] = None

    def to_dict(self):
        return makedict.without_none(
            (
                ("unit", self.unit),
                ("type", self.type),
            )
        )

    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            unit=data.get("unit"),
            type=data.get("type"),
        )


@dataclass
class GeologyInfo:
    alternation: Optional[NotEmptyStr] = None
    concentration_process: Optional[NotEmptyStr] = None
    ore_control: Optional[NotEmptyStr] = None
    host_rock: Optional[RockType] = None
    associated_rock: Optional[RockType] = None
    structure: Optional[NotEmptyStr] = None
    tectonic: Optional[NotEmptyStr] = None

    def to_dict(self):
        return makedict.without_none(
            (
                ("alternation", self.alternation),
                ("concentration_process", self.concentration_process),
                ("ore_control", self.ore_control),
                (
                    "host_rock",
                    self.host_rock.to_dict() if self.host_rock is not None else None,
                ),
                (
                    "associated_rock",
                    (
                        self.associated_rock.to_dict()
                        if self.associated_rock is not None
                        else None
                    ),
                ),
                ("structure", self.structure),
                ("tectonic", self.tectonic),
            )
        )

    @classmethod
    def from_dict(cls, d: dict):
        return cls(
            alternation=d.get("alternation"),
            concentration_process=d.get("concentration_process"),
            ore_control=d.get("ore_control"),
            host_rock=(
                RockType.from_dict(d["host_rock"]) if "host_rock" in d else None
            ),
            associated_rock=(
                RockType.from_dict(d["associated_rock"])
                if "associated_rock" in d
                else None
            ),
            structure=d.get("structure"),
            tectonic=d.get("tectonic"),
        )
