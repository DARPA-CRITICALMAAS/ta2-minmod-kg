from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional

from minmodkg.misc.utils import makedict
from minmodkg.models_v2.inputs.candidate_entity import CandidateEntity
from minmodkg.models_v2.inputs.measure import Measure


@dataclass
class Document:
    doi: Optional[str] = None
    uri: Optional[str] = None
    title: Optional[str] = None

    def to_dict(self):
        return makedict.without_none(
            (
                ("doi", self.doi),
                ("uri", self.uri),
                ("title", self.title),
            )
        )

    @classmethod
    def from_dict(cls, d):
        return cls(
            doi=d.get("doi"),
            uri=d.get("uri"),
            title=d.get("title"),
        )

    def clone(self):
        return Document(
            doi=self.doi,
            uri=self.uri,
            title=self.title,
        )

    def get_key(self):
        """Return a key that can uniquely identify this document in the MinMod KG"""
        if self.doi is not None:
            return "doi::" + self.doi
        if self.uri is not None:
            return "uri::" + self.uri
        raise ValueError("Document must have either DOI or URI")

    def merge_mut(self, other: Document):
        if self.doi is None:
            self.doi = other.doi
        if self.uri is None:
            self.uri = other.uri
        if self.title is None:
            self.title = other.title

    @staticmethod
    def dedup(docs: list[Document]) -> dict[int, Document]:
        # the first step is to merge by URI
        uri2doc = {}
        merged_docs = []
        for doc in docs:
            if doc.uri is None:
                merged_docs.append(doc)
            else:
                if doc.uri not in uri2doc:
                    uri2doc[doc.uri] = doc.clone()
                else:
                    uri2doc[doc.uri].merge_mut(doc)
        merged_docs.extend(uri2doc.values())

        doi2doc = {}
        new_merged_docs = []
        for doc in merged_docs:
            if doc.doi is None:
                new_merged_docs.append(doc)
            else:
                if doc.doi not in doi2doc:
                    doi2doc[doc.doi] = doc.clone()
                else:
                    doi2doc[doc.doi].merge_mut(doc)
        new_merged_docs.extend(doi2doc.values())

        uri2doc = {d.uri: d for d in new_merged_docs if d.uri is not None}
        doi2doc = {d.doi: d for d in new_merged_docs if d.doi is not None}

        output = {}
        for i, doc in enumerate(docs):
            if doc.uri is not None:
                assert doc.uri in uri2doc
                output[i] = uri2doc[doc.uri]
            else:
                assert doc.doi is not None and doc.doi in doi2doc
                output[i] = doi2doc[doc.doi]
        return output


@dataclass
class BoundingBox:
    x_max: float
    x_min: float
    y_max: float
    y_min: float

    def to_dict(self):
        return {
            "x_max": self.x_max,
            "x_min": self.x_min,
            "y_max": self.y_max,
            "y_min": self.y_min,
        }

    @classmethod
    def from_dict(cls, d):
        return cls(
            x_max=d["x_max"],
            x_min=d["x_min"],
            y_max=d["y_max"],
            y_min=d["y_min"],
        )

    def to_tuple(self):
        return (self.x_max, self.x_min, self.y_max, self.y_min)


@dataclass
class PageInfo:
    page: int
    bounding_box: Optional[BoundingBox] = None

    def to_dict(self):
        return makedict.without_none(
            (
                ("page", self.page),
                (
                    "bounding_box",
                    self.bounding_box.to_dict() if self.bounding_box else None,
                ),
            )
        )

    @classmethod
    def from_dict(cls, d):
        return cls(
            page=d["page"],
            bounding_box=(
                BoundingBox.from_dict(d.get("bounding_box"))
                if d.get("bounding_box")
                else None
            ),
        )

    def to_tuple(self):
        return (
            self.page,
            self.bounding_box.to_tuple() if self.bounding_box is not None else None,
        )


@dataclass
class Reference:
    document: Document
    page_info: list[PageInfo] = field(default_factory=list)
    comment: Optional[str] = None
    property: Optional[str] = None

    def to_dict(self):
        return makedict.without_none_or_empty_list(
            (
                ("document", self.document.to_dict()),
                ("page_info", [pi.to_dict() for pi in self.page_info]),
                ("comment", self.comment),
                ("property", self.property),
            )
        )

    @classmethod
    def from_dict(cls, d):
        return cls(
            document=Document.from_dict(d["document"]),
            page_info=[PageInfo.from_dict(pi) for pi in d.get("page_info", [])],
            comment=d.get("comment"),
            property=d.get("property"),
        )

    def to_tuple(self):
        return (
            self.document.get_key(),
            tuple(sorted((pi.to_tuple() for pi in self.page_info))),
            self.comment,
            self.property,
        )

    @staticmethod
    def dedup(refs: list[Reference]) -> list[Reference]:
        """Deduplicate references"""
        docs = Document.dedup([ref.document for ref in refs])
        new_refs = {}
        for i, ref in enumerate(refs):
            newref = Reference(
                document=docs[i],
                page_info=ref.page_info,
                comment=ref.comment,
                property=ref.property,
            )
            new_refs[newref.to_tuple()] = newref
        return list(new_refs.values())
