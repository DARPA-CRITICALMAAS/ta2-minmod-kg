from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Literal, Optional

import numpy as np
import strsim
from rdflib import RDFS, SKOS, Graph, URIRef


@dataclass
class Doc:
    id: str
    labels: list[str]
    props: dict[str, str]


class EntityLinking:
    instances = {}

    def __init__(self, data_file: Path | str, format: str):
        self.data_file = Path(data_file)
        self.g = Graph()
        self.g.parse(self.data_file, format=format)

        self.docs: list[Doc] = []
        for subj in self.g.subjects(unique=True):
            if not isinstance(subj, URIRef):
                continue
            labels = [str(x) for x in self.g.objects(subj, RDFS.label, unique=True)]
            labels.extend(
                (str(x) for x in self.g.objects(subj, SKOS.altLabel, unique=True))
            )
            if len(labels) == 0:
                continue

            self.docs.append(
                Doc(
                    id=str(subj),
                    labels=labels,
                    props={
                        str(pred): str(obj)
                        for pred, obj in self.g.predicate_objects(subj)
                        if pred != RDFS.label
                    },
                )
            )
        self.id2doc = {doc.id: doc for doc in self.docs}
        self.feat_extractor = FeatExtractor()

    @staticmethod
    def get_instance(
        entity_dir: Path | str,
        name: Literal[
            "crs",
            "country",
            "state_or_province",
            "commodity",
            "unit",
            "material_form",
            "category",
        ],
    ) -> EntityLinking:
        entity_dir = Path(entity_dir)
        if name not in EntityLinking.instances:
            mno = "https://minmod.isi.edu/ontology/"
            mnr = "https://minmod.isi.edu/resource/"
            if name == "crs":
                linker = EntityLinking(entity_dir / "epsg.ttl", "turtle")
            elif name == "state_or_province":
                linker = EntityLinking(entity_dir / "state_or_province.ttl", "turtle")
                country_linker = EntityLinking.get_instance(entity_dir, "country")
                name2country = {doc.labels[0]: doc for doc in country_linker.docs}
                for doc in linker.docs:
                    doc.props[f"{mno}country"] = name2country[
                        doc.props[f"{mno}country"]
                    ].id
            elif name in ["commodity", "unit", "country", "material_form", "category"]:
                linker = EntityLinking(entity_dir / f"{name}.ttl", "turtle")
            else:
                raise ValueError(f"Unknown entity type: {name}")

            EntityLinking.instances[name] = linker
        return EntityLinking.instances[name]

    def link(
        self, query: str, has_props: Optional[dict[str, str]] = None
    ) -> Optional[tuple[Doc, float]]:
        if has_props is None:
            has_props = {}

        scores = [
            (i, self.feat_extractor.extract(query, doc.labels).mean())
            for i, doc in enumerate(self.docs)
            if all(doc.props.get(k) == v for k, v in has_props.items())
        ]

        if len(scores) == 0:
            return None

        i, score = max(scores, key=lambda x: x[1])
        return self.docs[i], float(score)


class FeatExtractor:
    def __init__(self):
        self.chartok = strsim.CharacterTokenizer()
        self.charseqtok = strsim.WhitespaceCharSeqTokenizer()

    def extract(self, text: str, labels: Iterable[str]) -> np.ndarray:
        feat = np.zeros((7,), dtype=np.float64)
        for label in labels:
            feat = np.maximum(feat, self.extract_pairwise_features_v2(text, label))
        return feat

    def extract_pairwise_features_v2(self, text: str, entity_label: str):
        text_t1 = self.chartok.tokenize(text)
        entity_label_t1 = self.chartok.tokenize(entity_label)

        text_t2 = self.charseqtok.tokenize(text)
        entity_label_t2 = self.charseqtok.tokenize(entity_label)

        text_t3 = self.charseqtok.unique_tokenize(text)
        entity_label_t3 = self.charseqtok.unique_tokenize(entity_label)

        out2 = [
            strsim.levenshtein_similarity(text_t1, entity_label_t1),
            strsim.jaro_winkler_similarity(text_t1, entity_label_t1),
            strsim.monge_elkan_similarity(text_t2, entity_label_t2),
            (
                sym_monge_score := strsim.symmetric_monge_elkan_similarity(
                    text_t2, entity_label_t2
                )
            ),
            (hyjac_score := strsim.hybrid_jaccard_similarity(text_t3, entity_label_t3)),
            does_ordinal_match(text, entity_label, sym_monge_score, 0.7),
            does_ordinal_match(text, entity_label, hyjac_score, 0.7),
        ]
        return out2


def does_ordinal_match(s1: str, s2: str, sim: float, threshold: float) -> float:
    """Test for strings containing ordinal categorical values such as Su-30 vs Su-25, 29th Awards vs 30th Awards.

    Args:
        s1: Cell Label
        s2: Entity Label
    """
    if sim < threshold:
        return 0.4

    digit_tokens_1 = re.findall(r"\d+", s1)
    digit_tokens_2 = re.findall(r"\d+", s2)

    if digit_tokens_1 == digit_tokens_2:
        return 1.0

    if len(digit_tokens_1) == 0 or len(digit_tokens_2) == 0:
        return 0.4

    return 0.0


if __name__ == "__main__":
    pass
