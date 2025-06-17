from __future__ import annotations

import pytest
from minmodkg.validators import mineral_site_deser


class TestMineralSiteParser:

    def test_empty_record_id(self):
        raw = {
            "source_id": "https://minmod.isi.edu/users/a/bvu",
            "record_id": "",
            "created_by": "https://minmod.isi.edu/users/a/bvu",
            "dedup_site_uri": "https://minmod.isi.edu/derived/dedup_site__api-cdr-land-v1-docs-documents__02005ed95f9c1202261006876bc4b7cd8be3ead60226f6f0f22ccf558beafeb64d__inferlink",
            "name": "THE SELKIRK PROJECT",
            "aliases": [],
            "mineral_form": [],
            "deposit_type_candidate": [],
            "mineral_inventory": [],
            "reference": [
                {
                    "document": {
                        "uri": "https://minmod.isi.edu/users/a/bvu",
                        "title": "Unpublished document by Binh Vu for THE SELKIRK PROJECT (Nickel)",
                    },
                    "pageInfo": [],
                }
            ],
        }

        with pytest.raises(ValueError):
            ms = mineral_site_deser(raw)

    def test_missing_required_fields(self):
        raw = {
            "source_id": "https://minmod.isi.edu/users/a/bvu",
            # missing record_id
            "created_by": "https://minmod.isi.edu/users/a/bvu",
        }

        with pytest.raises(ValueError):
            mineral_site_deser(raw)

    def test_empty_reference_list(self):
        raw = {
            "source_id": "https://minmod.isi.edu/users/a/bvu",
            "record_id": "test_record_123",
            "created_by": "https://minmod.isi.edu/users/a/bvu",
            "name": "Test Site",
            "reference": [],
        }

        with pytest.raises(ValueError):
            mineral_site_deser(raw)

    def test_valid_minimal_site(self):
        raw = {
            "source_id": "https://minmod.isi.edu/users/a/bvu",
            "record_id": "test_record_123",
            "created_by": "https://minmod.isi.edu/users/a/bvu",
            "name": "Test Site",
            "reference": [
                {
                    "document": {
                        "uri": "https://minmod.isi.edu/users/a/bvu",
                        "title": "Test Document",
                    },
                    "pageInfo": [],
                }
            ],
        }

        ms = mineral_site_deser(raw)
        assert ms.record_id == "test_record_123"
        assert ms.name == "Test Site"

    def test_none_values_for_optional_fields(self):
        raw = {
            "source_id": "https://minmod.isi.edu/users/a/bvu",
            "record_id": "test_record_123",
            "created_by": "https://minmod.isi.edu/users/a/bvu",
            "name": None,
            "aliases": None,
            "reference": [
                {
                    "document": {
                        "uri": "https://minmod.isi.edu/users/a/bvu",
                        "title": "Test Document",
                    },
                    "pageInfo": [],
                }
            ],
        }

        with pytest.raises(ValueError):
            mineral_site_deser(raw)
