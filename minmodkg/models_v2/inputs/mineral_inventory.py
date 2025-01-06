from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from minmodkg.misc.utils import makedict
from minmodkg.models_v2.inputs.candidate_entity import CandidateEntity
from minmodkg.models_v2.inputs.measure import Measure
from minmodkg.models_v2.inputs.reference import Reference


@dataclass
class MineralInventory:
    commodity: CandidateEntity
    reference: Reference
    date: Optional[str] = None

    category: list[CandidateEntity] = field(default_factory=list)
    grade: Optional[Measure] = None
    cutoff_grade: Optional[Measure] = None
    material_form: Optional[CandidateEntity] = None
    ore: Optional[Measure] = None
    zone: Optional[str | int] = None

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
