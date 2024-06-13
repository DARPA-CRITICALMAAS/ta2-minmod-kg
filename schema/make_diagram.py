from dataclasses import asdict, dataclass, make_dataclass
from itertools import chain
from pathlib import Path

import erdantic as erd
from pydantic import BaseModel, Field, create_model
from rdflib import OWL, RDF, RDFS, Graph, Namespace, URIRef

mno = Namespace("https://minmod.isi.edu/ontology/")
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
    for cls in classes:
        assert isinstance(cls, URIRef)
        clsname = cls[len(mno) :]
        fields = []
        for prop in g.subjects(RDFS.domain, cls):
            assert isinstance(prop, URIRef)
            if (prop, hasSubPropertyOf, None) in g:
                continue
            propname = prop[len(mno) :]
            if (prop, RDF.type, OWL.DatatypeProperty) in g:
                fields.append((propname, str))
            else:
                for obj in g.objects(prop, RDFS.range, unique=True):
                    objname = obj[len(mno) :]
                    fields.append((propname, models[objname][0]))

        # model = make_dataclass(clsname, fields, bases=(BaseModel,))
        model = create_model(
            clsname,
            **{propname: (fieldtype, Field()) for propname, fieldtype in fields},
        )
        models[clsname] = (model, fields)

    graph = erd.create(*[model for model, fields in models.values() if len(fields) > 0])
    graph.draw(out=Path(__file__).parent / "er_diagram.png")
    return


if __name__ == "__main__":
    make_er_diagram()
