from __future__ import annotations

from time import sleep

import pytest
from minmodkg.misc.sparql import Transaction, Triples, sparql_insert, sparql_query


class TestBaseTransaction:
    @pytest.fixture(scope="class", autouse=True)
    def setup(self, kg):
        sparql_insert(
            Triples(
                [
                    ("mnr:Eagle", "rdf:type", ":MineralSite"),
                    ("mnr:Eagle", "rdfs:label", '"Eagle Mine"'),
                    ("mnr:Frog", "rdf:type", ":MineralSite"),
                    ("mnr:Frog", "rdfs:label", '"Frog Mine"'),
                ]
            )
        )


class TestTransaction__InsertLock(TestBaseTransaction):
    def test(self):
        transaction = Transaction(objects=["mnr:Eagle", "mnr:Frog"])
        transaction.insert_lock()
        assert transaction.does_lock_success()


class TestTransaction__DoesLockSuccess(TestBaseTransaction):
    def test(self):
        transaction = Transaction(objects=["mnr:Eagle", "mnr:Frog"])
        transaction.insert_lock()

        lst = sparql_query(
            """
    SELECT ?source ?lock
    WHERE {
        ?source :lock ?lock 
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
    def test(self):
        transaction = Transaction(objects=["mnr:Eagle", "mnr:Frog"])
        transaction.insert_lock()
        assert transaction.does_lock_success()
        transaction.remove_lock()
        lst = sparql_query(
            """
    SELECT ?source ?lock
    WHERE {
        ?source :lock ?lock 
        VALUES ?source { %s }
    }"""
            % transaction.value_query,
            keys=["source", "lock"],
        )
        assert len(lst) == 0


class TestTransaction__InsertLock__FailScenario1(TestBaseTransaction):
    def test(self):
        trans1 = Transaction(objects=["mnr:Eagle", "mnr:Frog"])
        trans2 = Transaction(objects=["mnr:Eagle", "mnr:Frog"])

        trans1.insert_lock()
        assert trans1.does_lock_success()
        trans2.insert_lock()
        assert not trans2.does_lock_success()


class TestTransaction__InsertLock__FailScenario2(TestBaseTransaction):
    def test(self):
        trans1 = Transaction(objects=["mnr:Eagle", "mnr:Frog"])
        trans2 = Transaction(objects=["mnr:Eagle", "mnr:Frog"])

        trans1.insert_lock()
        trans2.insert_lock()
        assert not trans1.does_lock_success()
        assert not trans2.does_lock_success()


class TestTransaction__InsertLock__FailScenario3(TestBaseTransaction):
    def test(self):
        trans1 = Transaction(
            objects=["mnr:Eagle", "mnr:Frog"], timeout_sec=1.5
        )  # expired after 1.5 sec

        trans1.insert_lock()
        assert trans1.does_lock_success()

        trans2 = Transaction(objects=["mnr:Eagle", "mnr:Frog"])
        trans2.insert_lock()
        # trans2 should fail because the transaction is not expired
        assert not trans2.does_lock_success()
        sleep(1.5)  # wait for the transaction to expire
        assert trans2.does_lock_success()
