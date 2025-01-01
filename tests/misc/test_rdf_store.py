from __future__ import annotations

from time import sleep

import pytest
from minmodkg.misc.rdf_store import TripleStore
from minmodkg.misc.rdf_store.virtuoso import VirtuosoDB
from rdflib import RDF, Literal, URIRef


class TestTripleStore:

    def test_insert(self, kg: TripleStore):
        mo = kg.ns.mo
        kg.insert(
            [
                (
                    "<https://mrdata.usgs.gov/mrds>",
                    "mo:uri",
                    '"https://mrdata.usgs.gov/mrds"',
                ),
                ("<https://mrdata.usgs.gov/mrds>", "rdf:type", "mo:Document"),
            ]
        )
        g = kg.construct(
            "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o . VALUES ?s { <https://mrdata.usgs.gov/mrds> } }"
        )
        triples = {
            (
                URIRef("https://mrdata.usgs.gov/mrds"),
                mo.uri("uri"),
                Literal("https://mrdata.usgs.gov/mrds"),
            ),
            (
                URIRef("https://mrdata.usgs.gov/mrds"),
                RDF.type,
                mo.uri("Document"),
            ),
        }
        if isinstance(kg, VirtuosoDB):
            with pytest.raises(AssertionError):
                assert set(g) == triples
        else:
            assert set(g) == triples


class TestBaseTransaction:
    @pytest.fixture(scope="class", autouse=True)
    def setup(self, kg: TripleStore):
        kg.insert(
            [
                ("mr:Eagle", "rdf:type", "mo:MineralSite"),
                ("mr:Eagle", "rdfs:label", '"Eagle Mine"'),
                ("mr:Frog", "rdf:type", "mo:MineralSite"),
                ("mr:Frog", "rdfs:label", '"Frog Mine"'),
            ]
        )


class TestTransaction__InsertLock(TestBaseTransaction):
    def test(self, kg: TripleStore):
        transaction = kg.transaction([kg.ns.mr.uri("Eagle"), kg.ns.mr.uri("Frog")])
        transaction.insert_lock()
        assert transaction.does_lock_success()


class TestTransaction__DoesLockSuccess(TestBaseTransaction):
    def test(self, kg: TripleStore):
        transaction = kg.transaction(
            objects=[kg.ns.mr.uri("Eagle"), kg.ns.mr.uri("Frog")]
        )
        transaction.insert_lock()

        lst = kg.query(
            """
    SELECT ?source ?lock
    WHERE {
        ?source mo:lock ?lock 
        VALUES ?source { %s }
    }"""
            % transaction.value_query,
            keys=["source", "lock"],
        )

        assert lst == [
            {
                "source": "https://minmod.isi.edu/resource/Eagle",
                "lock": transaction.lock,
            },
            {
                "source": "https://minmod.isi.edu/resource/Frog",
                "lock": transaction.lock,
            },
        ]


class TestTransaction__RemoveLock(TestBaseTransaction):
    def test(self, kg: TripleStore):
        transaction = kg.transaction([kg.ns.mr.uri("Eagle"), kg.ns.mr.uri("Frog")])
        transaction.insert_lock()
        assert transaction.does_lock_success()
        transaction.remove_lock()
        lst = kg.query(
            """
    SELECT ?source ?lock
    WHERE {
        ?source mo:lock ?lock 
        VALUES ?source { %s }
    }"""
            % transaction.value_query,
            keys=["source", "lock"],
        )
        assert len(lst) == 0


class TestTransaction__InsertLock__FailScenario1(TestBaseTransaction):
    def test(self, kg: TripleStore):
        trans1 = kg.transaction([kg.ns.mr.uri("Eagle"), kg.ns.mr.uri("Frog")])
        trans2 = kg.transaction([kg.ns.mr.uri("Eagle"), kg.ns.mr.uri("Frog")])

        trans1.insert_lock()
        assert trans1.does_lock_success()
        trans2.insert_lock()
        assert not trans2.does_lock_success()


class TestTransaction__InsertLock__FailScenario2(TestBaseTransaction):
    def test(self, kg: TripleStore):
        trans1 = kg.transaction([kg.ns.mr.uri("Eagle"), kg.ns.mr.uri("Frog")])
        trans2 = kg.transaction([kg.ns.mr.uri("Eagle"), kg.ns.mr.uri("Frog")])

        trans1.insert_lock()
        trans2.insert_lock()
        assert not trans1.does_lock_success()
        assert not trans2.does_lock_success()


class TestTransaction__InsertLock__FailScenario3(TestBaseTransaction):
    def test(self, kg: TripleStore):
        trans1 = kg.transaction(
            objects=[kg.ns.mr.uri("Eagle"), kg.ns.mr.uri("Frog")], timeout_sec=1.5
        )  # expired after 1.5 sec

        trans1.insert_lock()
        assert trans1.does_lock_success()

        trans2 = kg.transaction([kg.ns.mr.uri("Eagle"), kg.ns.mr.uri("Frog")])
        trans2.insert_lock()
        # trans2 should fail because the transaction is not expired
        assert not trans2.does_lock_success()
        sleep(1.5)  # wait for the transaction to expire
        assert trans2.does_lock_success()
