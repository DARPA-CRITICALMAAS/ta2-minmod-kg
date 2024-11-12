from __future__ import annotations

from collections import defaultdict

from minmodkg.schema.make_diagram import (
    get_class_in_order,
    get_domain_to_props,
    hasSubPropertyOf,
    load_ontology,
    mno,
    read_a_list,
    schema_dir,
)
from rdflib import OWL, RDF, RDFS, SH, BNode, Graph, Namespace, URIRef

mnos = Namespace("https://minmod.isi.edu/ontology-shape/")


def make_shapes(skip_parent_class: bool = True):
    """We do not"""
    g = load_ontology()
    sg = Graph()
    sg.bind("sh", SH)
    sg.bind("mnos", mnos)

    for prefix, ns in g.namespaces():
        sg.bind(prefix, ns)

    props, domain2props = get_domain_to_props(g)

    for subj in get_class_in_order(g, props, domain2props):
        clsname = subj[len(mno) :]
        prop2restriction = defaultdict(list)
        for obj in g.objects(subj, RDFS.subClassOf):
            if (obj, RDF.type, OWL.Restriction) not in g:
                continue

            (prop,) = list(g.objects(obj, OWL.onProperty))
            prop2restriction[prop].append(obj)

        prop_targets = []
        for prop in domain2props.get(subj, []):
            assert isinstance(prop, URIRef)
            if (prop, hasSubPropertyOf, None) in g:
                continue

            targets = props[prop]["range"]
            for restriction in prop2restriction[prop]:
                if (restriction, OWL.allValuesFrom, None) in g:
                    (val,) = list(g.objects(restriction, OWL.allValuesFrom))
                    if isinstance(val, URIRef):
                        vals = [val]
                    else:
                        vals = read_a_list(g, val)
                    targets = [type for type in targets if type in vals]
                    assert len(targets) > 0

            targets = [t for t in targets if t in mno]
            if len(targets) == 0:
                continue

            prop_targets.append(
                (
                    prop,
                    targets,
                )
            )

        if len(prop_targets) == 0:
            # has no target links, we skip it as drepr validate for literal values
            continue

        if skip_parent_class and len(list(g.subjects(RDFS.subClassOf, subj))) > 0:
            # skip parent classes -- only plot the leaf classes
            continue

        subj_shp = mnos[clsname + "Shape"]
        sg.add((subj_shp, RDF.type, SH.NodeShape))
        sg.add((subj_shp, SH.targetClass, subj))

        for prop, targets in prop_targets:
            constraint = BNode()
            sg.add((subj_shp, SH.property, constraint))
            sg.add((constraint, SH.path, prop))
            for target in targets:
                sg.add((constraint, SH["class"], target))
            sg.add((constraint, SH.severity, SH.Violation))

    sg.serialize(schema_dir / "shapes.ttl", format="ttl")


if __name__ == "__main__":
    make_shapes()
