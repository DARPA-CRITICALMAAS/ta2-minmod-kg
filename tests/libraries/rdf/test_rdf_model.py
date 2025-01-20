from __future__ import annotations

from minmodkg.libraries.rdf.rdf_model import RDFModel
from minmodkg.models.kg.candidate_entity import CandidateEntity
from minmodkg.typing import NotEmptyStr
from rdflib import XSD


def test_parse_type_hint():
    assert RDFModel._parse_type_hint(str) == {
        "is_object": False,
        "is_list": False,
        "datatype": XSD.string,
    }
    assert RDFModel._parse_type_hint(NotEmptyStr) == {
        "is_object": False,
        "is_list": False,
        "datatype": XSD.string,
    }
    assert RDFModel._parse_type_hint(list[NotEmptyStr]) == {
        "is_object": False,
        "is_list": True,
        "datatype": XSD.string,
    }
    assert RDFModel._parse_type_hint(list[float]) == {
        "is_object": False,
        "is_list": True,
        "datatype": XSD.decimal,
    }
    assert RDFModel._parse_type_hint(list[CandidateEntity]) == {
        "is_object": True,
        "is_list": True,
        "datatype": None,
        "target": CandidateEntity,
    }
