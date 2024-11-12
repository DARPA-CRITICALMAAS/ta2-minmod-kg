from __future__ import annotations

from typing import Annotated

from pydantic.networks import HttpUrl

IRI = Annotated[str, "Internationalized Resource Identifier"]
InternalID = Annotated[
    str,
    "internal MinMod ID (e.g., Q578) - together with `https://minmod.isi.edu/resource/` prefix, it creates the URI of a resource in the MinMod KG",
]
Triple = tuple[str, str, str]
