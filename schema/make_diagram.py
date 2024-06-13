from collections import defaultdict
from dataclasses import asdict, dataclass, make_dataclass
from graphlib import TopologicalSorter
from itertools import chain
from pathlib import Path
from typing import Union

import erdantic as erd
from pydantic import BaseModel, Field, create_model
from rdflib import OWL, RDF, RDFS, XSD, Graph, Namespace, URIRef

mno = Namespace("https://minmod.isi.edu/ontology/")
geo = Namespace("http://www.opengis.net/ont/geosparql#")
hasSubPropertyOf = mno.hasSubPropertyOf


def get_all_values(g: Graph, s: URIRef, p: URIRef, subItemOf: URIRef):
    output = list(g.objects(s, p, unique=True))
    for parent in g.objects(s, subItemOf):
        assert isinstance(parent, URIRef)
        output.extend(get_all_values(g, parent, p, subItemOf))
    return output


def load_ontology():
    ontology_file = Path(__file__).parent / "ontology.ttl"
    g = Graph()
    g.parse(ontology_file, format="turtle")

    # run basic inference to inherit domain and range
    all_props = list(g.subjects(RDF.type, OWL.ObjectProperty, unique=True)) + list(
        g.subjects(RDF.type, OWL.DatatypeProperty, unique=True)
    )
    for prop in all_props:
        assert isinstance(prop, URIRef)
        for attr in [RDFS.domain, RDFS.range]:
            existing_vals = set(g.objects(prop, attr, unique=True))
            for val in get_all_values(g, prop, attr, RDFS.subPropertyOf):
                if val not in existing_vals:
                    g.add((prop, attr, val))

    # add hasSubPropertyOf
    for s, p, o in list(g.triples((None, RDFS.subPropertyOf, None))):
        g.add((o, hasSubPropertyOf, s))
    return g


def make_er_diagram():
    g = load_ontology()

    models = {}

    # classes = list(g.subjects(RDF.type, OWL.Class))
    classes = [
        mno.EvidenceLayer,
        mno.Document,
        mno.BoundingBox,
        mno.PageInfo,
        mno.Reference,
        # mno.MappableCriteria,
    ]

    edges: dict[URIRef, list[URIRef]] = defaultdict(list)
    for subj in g.subjects(RDF.type, OWL.Class):
        assert isinstance(subj, URIRef)
        for prop in g.subjects(RDFS.domain, subj):
            if (prop, RDF.type, OWL.DatatypeProperty) in g:
                continue
            for obj in g.objects(prop, RDFS.range, unique=True):
                assert isinstance(obj, URIRef)
                edges[subj].append(obj)

    ts = TopologicalSorter(edges)
    for subj in ts.static_order():
        clsname = subj[len(mno) :]
        fields = []
        for prop in g.subjects(RDFS.domain, subj):
            assert isinstance(prop, URIRef)
            if (prop, hasSubPropertyOf, None) in g:
                continue
            propname = prop[len(mno) :]
            if (prop, RDF.type, OWL.DatatypeProperty) in g:
                ranges = set()
                for obj in g.objects(prop, RDFS.range, unique=True):
                    assert isinstance(obj, URIRef)
                    if obj == XSD.string:
                        ranges.add(str)
                    elif obj == XSD.integer:
                        ranges.add(int)
                    elif obj == XSD.decimal:
                        ranges.add(float)
                    elif obj == geo.wktLiteral:
                        ranges.add("WktLiteral")
                    else:
                        raise NotImplementedError()
                    ranges.add(obj)
                fields.append((propname, str))
            else:
                ranges = [
                    models[str(obj)[len(mno) :]]
                    for obj in g.objects(prop, RDFS.range, unique=True)
                ]
                fields.append(
                    (
                        propname,
                        Union[tuple(ranges)],
                    )
                )

        models[clsname] = make_dataclass(clsname, fields, bases=(BaseModel,))

    graph = erd.create(*list(models.values()))
    graph.draw(out=Path(__file__).parent / "er_diagram.png")
    return


if __name__ == "__main__":
    make_er_diagram()
