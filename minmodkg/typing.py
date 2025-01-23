from __future__ import annotations

from typing import Annotated, Sequence, TypeVar

from minmodkg.misc.utils import CleanedNotEmptyStr as CleanedNotEmptyStrDeser
from minmodkg.misc.utils import NotEmptyStr as NotEmptyStrDeser

T = TypeVar("T")
V = TypeVar("V")

NamespaceAlias = Annotated[str, "A shorter name for the corresponding namespace"]
NotEmptyStr = Annotated[str, NotEmptyStrDeser()]
CleanedNotEmptyStr = Annotated[str, CleanedNotEmptyStrDeser()]
IRI = Annotated[str, "Internationalized Resource Identifier"]
URN = Annotated[str, "Uniform Resource Name"]
RelIRI = Annotated[str, "Relative Internationalized Resource Identifier"]
InternalID = Annotated[
    str,
    "internal MinMod ID (e.g., Q578) - together with `https://minmod.isi.edu/resource/` prefix, it creates the URI of a resource in the MinMod KG",
]
Triple = Annotated[
    tuple[str, str, str],
    "A triple that by joining with space (`[0] [1] [2]`) is a valid triple in N3 format",
]
Triples = Sequence[Triple]
SPARQLMainQuery = Annotated[
    str, "The main part of a SPARQL query that does not include prefixes"
]
