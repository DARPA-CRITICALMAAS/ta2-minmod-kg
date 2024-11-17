from __future__ import annotations

from time import sleep

import pytest
from minmodkg.misc.rdf_store import RDFStore, Transaction


class TestBaseTransaction:
    @pytest.fixture(scope="class", autouse=True)
    def setup(self, kg: RDFStore):
        kg.insert(
            [
                ("mr:Eagle", "rdf:type", "mo:MineralSite"),
                ("mr:Eagle", "rdfs:label", '"Eagle Mine"'),
                ("mr:Frog", "rdf:type", "mo:MineralSite"),
                ("mr:Frog", "rdfs:label", '"Frog Mine"'),
            ]
        )


class TestTransaction__InsertLock(TestBaseTransaction):
    def test(self, kg: RDFStore):
        transaction = kg.transaction([kg.ns.mr.uri("Eagle"), kg.ns.mr.uri("Frog")])
        transaction.insert_lock()
        assert transaction.does_lock_success()


class TestTransaction__DoesLockSuccess(TestBaseTransaction):
    def test(self, kg: RDFStore):
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
    def test(self, kg: RDFStore):
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
    def test(self, kg: RDFStore):
        trans1 = kg.transaction([kg.ns.mr.uri("Eagle"), kg.ns.mr.uri("Frog")])
        trans2 = kg.transaction([kg.ns.mr.uri("Eagle"), kg.ns.mr.uri("Frog")])

        trans1.insert_lock()
        assert trans1.does_lock_success()
        trans2.insert_lock()
        assert not trans2.does_lock_success()


class TestTransaction__InsertLock__FailScenario2(TestBaseTransaction):
    def test(self, kg: RDFStore):
        trans1 = kg.transaction([kg.ns.mr.uri("Eagle"), kg.ns.mr.uri("Frog")])
        trans2 = kg.transaction([kg.ns.mr.uri("Eagle"), kg.ns.mr.uri("Frog")])

        trans1.insert_lock()
        trans2.insert_lock()
        assert not trans1.does_lock_success()
        assert not trans2.does_lock_success()


class TestTransaction__InsertLock__FailScenario3(TestBaseTransaction):
    def test(self, kg: RDFStore):
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