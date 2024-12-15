from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from minmodkg.misc.prefix_index import LongestPrefixIndex
from minmodkg.misc.rdf_store import norm_literal
from minmodkg.misc.utils import filter_duplication
from minmodkg.models.base import MinModRDFModel
from minmodkg.transformations import get_source_uri
from minmodkg.typing import IRI, URN, InternalID, Triple
from rdflib import Graph
from rdflib import Literal as RDFLiteral
from rdflib import URIRef


class SourceConfig(MinModRDFModel):
    id: InternalID
    prefix: str
    name: str
    description: str
    score: float
    connection: Optional[str] = None

    @classmethod
    def from_graph(cls, uid: URIRef, g: Graph) -> SourceConfig:
        mo = cls.rdfdata.ns.mo
        rdfs = cls.rdfdata.ns.rdfs
        return cls(
            id=uid,
            prefix=norm_literal(next(g.objects(uid, mo.uri("prefix")))),
            name=norm_literal(next(g.objects(uid, rdfs.uri("label")))),
            description=norm_literal(next(g.objects(uid, rdfs.uri("comment")))),
            score=norm_literal(next(g.objects(uid, mo.uri("score")))),
            connection=norm_literal(next(g.objects(uid, mo.uri("connection")), None)),
        )


@dataclass
class SourceFactory:
    sources: dict[str, SourceConfig]
    index: LongestPrefixIndex

    def get_config(self, source_id: str) -> Optional[SourceConfig]:
        prefix = self.index.get(source_id)
        if prefix is not None:
            return self.sources[prefix]
        return None

    def get_source(self, source_id: str) -> Source:
        """Create source from source id"""
        cfg = self.get_config(source_id)
        uri = get_source_uri(source_id)

        if cfg is None:
            return Source(
                uri=uri,
                id=source_id,
                name=source_id,
                score=-1,
                connection=None,
            )
        return Source(
            uri=uri,
            id=source_id,
            name=cfg.name,
            score=cfg.score,
            connection=cfg.connection,
        )

    @staticmethod
    def from_configs(source_configs: list[SourceConfig]) -> SourceFactory:
        prefixes = filter_duplication((src.prefix for src in source_configs))
        assert len(source_configs) == len(prefixes)
        index = LongestPrefixIndex.create(prefixes)

        return SourceFactory({src.prefix: src for src in source_configs}, index)


class Source(MinModRDFModel):
    uri: IRI
    id: URN
    name: str
    score: float
    connection: Optional[str] = None

    def to_triples(self, triples: Optional[list[Triple]] = None) -> list[Triple]:
        if triples is None:
            triples = []

        ns = self.rdfdata.ns
        mo = ns.mo

        subj = f"<{self.uri}>"

        triples.append((subj, ns.rdf.type, mo.uri("Source").n3()))
        triples.append((subj, ns.rdfs.label, RDFLiteral(self.name).n3()))
        triples.append((subj, mo["id"], RDFLiteral(self.id).n3()))
        triples.append((subj, mo.score, RDFLiteral(self.score).n3()))
        if self.connection is not None:
            triples.append((subj, mo.connection, RDFLiteral(self.connection).n3()))

        return triples
