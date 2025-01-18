from __future__ import annotations

from dataclasses import dataclass, field
from typing import Annotated, Optional

from minmodkg.misc.rdf_store.rdf_model import Property, RDFModel, Subject
from minmodkg.misc.utils import makedict
from minmodkg.models.kg.base import NS_MO
from minmodkg.models.kg.candidate_entity import CandidateEntity
from minmodkg.models.kg.measure import Measure
from minmodkg.models.kg.reference import Reference


@dataclass
class MineralInventory(RDFModel):
    __subj__ = Subject(ns=NS_MO, name="MineralInventory")

    commodity: Annotated[
        CandidateEntity, Property(ns=NS_MO, name="commodity", is_object_property=True)
    ]
    reference: Annotated[
        Reference, Property(ns=NS_MO, name="reference", is_object_property=True)
    ]
    date: Annotated[Optional[str], Property(ns=NS_MO, name="date")] = None

    category: Annotated[
        list[CandidateEntity],
        Property(ns=NS_MO, name="category", is_list=True, is_object_property=True),
    ] = field(default_factory=list)
    grade: Annotated[
        Optional[Measure], Property(ns=NS_MO, name="grade", is_object_property=True)
    ] = None
    cutoff_grade: Annotated[
        Optional[Measure],
        Property(ns=NS_MO, name="cutoff_grade", is_object_property=True),
    ] = None
    material_form: Annotated[
        Optional[CandidateEntity],
        Property(ns=NS_MO, name="material_form", is_object_property=True),
    ] = None
    ore: Annotated[
        Optional[Measure], Property(ns=NS_MO, name="ore", is_object_property=True)
    ] = None
    zone: Annotated[Optional[str], Property(ns=NS_MO, name="zone")] = None

    def to_dict(self):
        return makedict.without_none_or_empty_list(
            (
                ("reference", self.reference.to_dict()),
                ("commodity", self.commodity.to_dict()),
                ("category", [ent.to_dict() for ent in self.category]),
                ("grade", self.grade.to_dict() if self.grade else None),
                (
                    "material_form",
                    self.material_form.to_dict() if self.material_form else None,
                ),
                ("ore", self.ore.to_dict() if self.ore else None),
                (
                    "cutoff_grade",
                    self.cutoff_grade.to_dict() if self.cutoff_grade else None,
                ),
                ("date", self.date),
                ("zone", self.zone),
            )
        )

    @classmethod
    def from_dict(cls, d):
        return cls(
            reference=Reference.from_dict(d["reference"]),
            commodity=CandidateEntity.from_dict(d["commodity"]),
            category=[CandidateEntity.from_dict(ent) for ent in d.get("category", [])],
            grade=Measure.from_dict(d["grade"]) if d.get("grade") else None,
            material_form=(
                CandidateEntity.from_dict(d["material_form"])
                if d.get("material_form")
                else None
            ),
            ore=Measure.from_dict(d["ore"]) if d.get("ore") else None,
            cutoff_grade=(
                Measure.from_dict(d["cutoff_grade"]) if d.get("cutoff_grade") else None
            ),
            date=d.get("date"),
            zone=d.get("zone"),
        )
