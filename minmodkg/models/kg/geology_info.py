from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Optional

from minmodkg.misc.rdf_store.rdf_model import Property, RDFModel
from minmodkg.misc.utils import makedict
from minmodkg.models.kg.base import NS_MO
from minmodkg.typing import NotEmptyStr


@dataclass
class RockType(RDFModel):
    unit: Annotated[Optional[NotEmptyStr], Property(ns=NS_MO, name="unit")] = None
    type: Annotated[Optional[NotEmptyStr], Property(ns=NS_MO, name="type")] = None

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
class GeologyInfo(RDFModel):
    alternation: Annotated[
        Optional[NotEmptyStr], Property(ns=NS_MO, name="alternation")
    ] = None
    concentration_process: Annotated[
        Optional[NotEmptyStr], Property(ns=NS_MO, name="concentration_process")
    ] = None
    ore_control: Annotated[
        Optional[NotEmptyStr], Property(ns=NS_MO, name="ore_control")
    ] = None
    host_rock: Annotated[
        Optional[RockType],
        Property(ns=NS_MO, name="host_rock", is_object_property=True),
    ] = None
    associated_rock: Annotated[
        Optional[RockType],
        Property(ns=NS_MO, name="associated_rock", is_object_property=True),
    ] = None
    structure: Annotated[
        Optional[NotEmptyStr], Property(ns=NS_MO, name="structure")
    ] = None
    tectonic: Annotated[Optional[NotEmptyStr], Property(ns=NS_MO, name="tectonic")] = (
        None
    )

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
