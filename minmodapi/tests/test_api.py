from __future__ import annotations

from uuid import uuid4

from minmodapi import MinModAPI, merge_deposit_type


def test_add_mineral_site():
    api = MinModAPI("http://localhost:8000")
    site_data = {
        "source_id": "mining-report::https://api.cdr.land/v1/docs/documents",
        "record_id": "test:" + str(uuid4()),
        "name": "Vare\u0161 Polymetallic Project",
        "location_info": {
            "crs": {
                "normalized_uri": "https://minmod.isi.edu/resource/Q701",
                "observed_name": "EPSG:4326",
                "confidence": 1.0,
                "source": "Inferlink Extraction v3",
            },
            "country": [
                {
                    "normalized_uri": "https://minmod.isi.edu/resource/Q1027",
                    "observed_name": "Bosnia and Herzegovina",
                    "confidence": 1.0,
                    "source": "Inferlink Extraction v3",
                }
            ],
            "state_or_province": [],
        },
        "mineral_inventory": [],
        "deposit_type_candidate": [],
        "modified_at": "2025-01-06T17:22:53.006220Z",
        "created_by": "https://minmod.isi.edu/users/s/inferlink",
        "reference": [
            {
                "document": {
                    "title": "NI 43-101 Technical Report for the Vares Project in Bosnia & Herzegovina dated April 2020.pdf",
                    "uri": "https://api.cdr.land/v1/docs/documents/02a03f5899d5a791fb606a7178e8b785ca9943aab34b4c81c29599ea6bb9c86cba",
                }
            }
        ],
    }

    site_ident = api.upsert_mineral_site(
        site_data,
        apply_update=merge_deposit_type,
        verbose=True,
    )
    assert site_ident is not None

    server_site_data = api.get_site(site_ident.site_id)
    gold_site_data = site_data.copy()
    gold_site_data.update(
        id=site_ident.site_id,
        created_by=server_site_data["created_by"],
        snapshot_id=server_site_data["snapshot_id"],
        dedup_site_uri=server_site_data["dedup_site_uri"],
        modified_at=server_site_data["modified_at"],
    )
    for key in ["deposit_type_candidate", "mineral_inventory"]:
        gold_site_data.pop(key)
    gold_site_data["location_info"].pop("state_or_province")

    assert server_site_data == gold_site_data
    assert (
        site_ident.get_browse_link()
        == "http://localhost:8000/resource/" + site_ident.site_id
    )
    assert (
        site_ident.get_api_link()
        == "http://localhost:8000/api/v1/mineral-sites/" + site_ident.site_id
    )


def test_update_mineral_site_deposit_type_first():
    api = MinModAPI("http://localhost:8000")
    # sri add deposit type first
    site_data_1 = {
        "source_id": "mining-report::https://api.cdr.land/v1/docs/documents",
        "record_id": "test:" + str(uuid4()),
        "mineral_inventory": [],
        "deposit_type_candidate": [
            {
                "observed_name": "Peralkaline igneous HFSE- REE",
                "confidence": 0.6209266126155853,
                "normalized_uri": "https://minmod.isi.edu/resource/Q472",
                "source": "algorithm predictions, SRI deposit type classification, v2.1, 20241031",
            }
        ],
        "modified_at": "2025-01-06T17:22:53.006220Z",
        "created_by": ["https://minmod.isi.edu/users/s/sri"],
        "reference": [
            {
                "document": {
                    "uri": "https://api.cdr.land/v1/docs/documents/02a03f5899d5a791fb606a7178e8b785ca9943aab34b4c81c29599ea6bb9c86cba",
                }
            }
        ],
    }

    site_ident_1 = api.upsert_mineral_site(
        site_data_1,
        apply_update=merge_deposit_type,
        verbose=True,
    )
    assert site_ident_1 is not None

    server_site_data = api.get_site(site_ident_1.site_id)
    gold_site_data = site_data_1.copy()
    gold_site_data.update(
        id=site_ident_1.site_id,
        created_by=server_site_data["created_by"],
        snapshot_id=server_site_data["snapshot_id"],
        dedup_site_uri=server_site_data["dedup_site_uri"],
        modified_at=server_site_data["modified_at"],
    )
    for key in ["mineral_inventory"]:
        gold_site_data.pop(key)

    assert server_site_data == gold_site_data
    assert (
        site_ident_1.get_browse_link()
        == "http://localhost:8000/resource/" + site_ident_1.site_id
    )
    assert (
        site_ident_1.get_api_link()
        == "http://localhost:8000/api/v1/mineral-sites/" + site_ident_1.site_id
    )

    # then inferlink add other information to the site
    site_data_2 = {
        "source_id": "mining-report::https://api.cdr.land/v1/docs/documents",
        "record_id": site_data_1["record_id"],
        "name": "Vare\u0161 Polymetallic Project",
        "location_info": {
            "crs": {
                "normalized_uri": "https://minmod.isi.edu/resource/Q701",
                "observed_name": "EPSG:4326",
                "confidence": 1.0,
                "source": "Inferlink Extraction v3",
            },
            "country": [
                {
                    "normalized_uri": "https://minmod.isi.edu/resource/Q1027",
                    "observed_name": "Bosnia and Herzegovina",
                    "confidence": 1.0,
                    "source": "Inferlink Extraction v3",
                }
            ],
            "state_or_province": [],
        },
        "mineral_inventory": [
            {
                "commodity": {
                    "observed_name": "copper",
                    "confidence": 1,
                    "source": "Inferlink Extraction v3",
                    "normalized_uri": "https://minmod.isi.edu/resource/Q538",
                },
                "category": [
                    {
                        "normalized_uri": "https://minmod.isi.edu/resource/Inferred",
                        "observed_name": "inferred",
                        "confidence": 1.0,
                        "source": "Inferlink Extraction v3",
                    }
                ],
                "ore": {"value": 8219.0},
                "grade": {
                    "unit": {
                        "normalized_uri": "https://minmod.isi.edu/resource/Q201",
                        "observed_name": "percent",
                        "confidence": 1,
                        "source": "Inferlink Extraction v3",
                    },
                    "value": 0.5,
                },
                "reference": {
                    "document": {
                        "title": "NI 43-101 Technical Report for the McCreedy West Project in North America dated August 2003.pdf",
                        "uri": "https://api.cdr.land/v1/docs/documents/0233825877b41a6e348ee6a443763c95b0d99d28f14408b40bb09cc64cf99ab675",
                    },
                    "page_info": [{"page": 10}],
                },
                "date": "2003-08",
                "zone": "footwall vein complexes",
            }
        ],
        "deposit_type_candidate": [
            {
                "observed_name": "Footwall Vein Deposits",
                "normalized_uri": "https://minmod.isi.edu/resource/Q425",
                "source": "Inferlink Extraction v3",
                "confidence": 0.5,
            },
        ],
        "modified_at": "2025-01-06T17:22:53.006220Z",
        "created_by": "https://minmod.isi.edu/users/s/inferlink",
        "reference": [
            {
                "document": {
                    "title": "NI 43-101 Technical Report for the Vares Project in Bosnia & Herzegovina dated April 2020.pdf",
                    "uri": "https://api.cdr.land/v1/docs/documents/02a03f5899d5a791fb606a7178e8b785ca9943aab34b4c81c29599ea6bb9c86cba",
                }
            }
        ],
    }

    site_ident_2 = api.upsert_mineral_site(
        site_data_2,
        apply_update=merge_deposit_type,
        verbose=True,
    )
    assert site_ident_2 is not None
    assert site_ident_1.site_id == site_ident_2.site_id

    server_site_data = api.get_site(site_ident_2.site_id)
    gold_site_data = site_data_2.copy()
    gold_site_data.update(
        id=site_ident_2.site_id,
        created_by=server_site_data["created_by"],
        snapshot_id=server_site_data["snapshot_id"],
        dedup_site_uri=server_site_data["dedup_site_uri"],
        modified_at=server_site_data["modified_at"],
        deposit_type_candidate=[
            site_data_1["deposit_type_candidate"][0],
            site_data_2["deposit_type_candidate"][0],
        ],
        grade_tonnage=server_site_data["grade_tonnage"],
    )
    gold_site_data["location_info"].pop("state_or_province")
    assert server_site_data == gold_site_data


def test_update_mineral_site_deposit_type_later():
    api = MinModAPI("http://localhost:8000")
    # inferlink add information to the site first
    site_data_1 = {
        "source_id": "mining-report::https://api.cdr.land/v1/docs/documents",
        "record_id": "test:" + str(uuid4()),
        "name": "Vare\u0161 Polymetallic Project",
        "location_info": {
            "crs": {
                "normalized_uri": "https://minmod.isi.edu/resource/Q701",
                "observed_name": "EPSG:4326",
                "confidence": 1.0,
                "source": "Inferlink Extraction v3",
            },
            "country": [
                {
                    "normalized_uri": "https://minmod.isi.edu/resource/Q1027",
                    "observed_name": "Bosnia and Herzegovina",
                    "confidence": 1.0,
                    "source": "Inferlink Extraction v3",
                }
            ],
            "state_or_province": [],
        },
        "mineral_inventory": [
            {
                "commodity": {
                    "observed_name": "copper",
                    "confidence": 1,
                    "source": "Inferlink Extraction v3",
                    "normalized_uri": "https://minmod.isi.edu/resource/Q538",
                },
                "category": [
                    {
                        "normalized_uri": "https://minmod.isi.edu/resource/Inferred",
                        "observed_name": "inferred",
                        "confidence": 1.0,
                        "source": "Inferlink Extraction v3",
                    }
                ],
                "ore": {"value": 8219.0},
                "grade": {
                    "unit": {
                        "normalized_uri": "https://minmod.isi.edu/resource/Q201",
                        "observed_name": "percent",
                        "confidence": 1,
                        "source": "Inferlink Extraction v3",
                    },
                    "value": 0.5,
                },
                "reference": {
                    "document": {
                        "title": "NI 43-101 Technical Report for the McCreedy West Project in North America dated August 2003.pdf",
                        "uri": "https://api.cdr.land/v1/docs/documents/0233825877b41a6e348ee6a443763c95b0d99d28f14408b40bb09cc64cf99ab675",
                    },
                    "page_info": [{"page": 10}],
                },
                "date": "2003-08",
                "zone": "footwall vein complexes",
            }
        ],
        "deposit_type_candidate": [
            {
                "observed_name": "Footwall Vein Deposits",
                "normalized_uri": "https://minmod.isi.edu/resource/Q425",
                "source": "Inferlink Extraction v3",
                "confidence": 0.5,
            },
        ],
        "modified_at": "2025-01-06T17:22:53.006220Z",
        "created_by": "https://minmod.isi.edu/users/s/inferlink",
        "reference": [
            {
                "document": {
                    "title": "NI 43-101 Technical Report for the Vares Project in Bosnia & Herzegovina dated April 2020.pdf",
                    "uri": "https://api.cdr.land/v1/docs/documents/02a03f5899d5a791fb606a7178e8b785ca9943aab34b4c81c29599ea6bb9c86cba",
                }
            }
        ],
    }

    site_ident_1 = api.upsert_mineral_site(
        site_data_1,
        apply_update=merge_deposit_type,
        verbose=True,
    )
    assert site_ident_1 is not None

    server_site_data = api.get_site(site_ident_1.site_id)
    gold_site_data = site_data_1.copy()
    gold_site_data.update(
        id=site_ident_1.site_id,
        created_by=server_site_data["created_by"],
        snapshot_id=server_site_data["snapshot_id"],
        dedup_site_uri=server_site_data["dedup_site_uri"],
        modified_at=server_site_data["modified_at"],
        deposit_type_candidate=[
            site_data_1["deposit_type_candidate"][0],
        ],
        grade_tonnage=server_site_data["grade_tonnage"],
    )
    gold_site_data["location_info"].pop("state_or_province")
    assert server_site_data == gold_site_data

    # sri add deposit type first
    site_data_3 = {
        "source_id": "mining-report::https://api.cdr.land/v1/docs/documents",
        "record_id": site_data_1["record_id"],
        "mineral_inventory": [],
        "deposit_type_candidate": [
            {
                "observed_name": "Peralkaline igneous HFSE- REE",
                "confidence": 0.6209266126155853,
                "normalized_uri": "https://minmod.isi.edu/resource/Q472",
                "source": "algorithm predictions, SRI deposit type classification, v2.1, 20241031",
            }
        ],
        "modified_at": "2025-01-06T17:22:53.006220Z",
        "created_by": ["https://minmod.isi.edu/users/s/sri"],
        "reference": [
            {
                "document": {
                    "uri": "https://api.cdr.land/v1/docs/documents/02a03f5899d5a791fb606a7178e8b785ca9943aab34b4c81c29599ea6bb9c86cba",
                }
            }
        ],
    }

    site_ident_2 = api.upsert_mineral_site(
        site_data_3,
        apply_update=merge_deposit_type,
        verbose=True,
    )
    assert site_ident_2 is not None
    assert site_ident_1.site_id == site_ident_2.site_id

    server_site_data = api.get_site(site_ident_2.site_id)
    gold_site_data = site_data_3.copy()
    gold_site_data.update(
        id=site_ident_2.site_id,
        name=site_data_1["name"],
        created_by=server_site_data["created_by"],
        snapshot_id=server_site_data["snapshot_id"],
        dedup_site_uri=server_site_data["dedup_site_uri"],
        modified_at=server_site_data["modified_at"],
        deposit_type_candidate=[
            site_data_1["deposit_type_candidate"][0],
            site_data_3["deposit_type_candidate"][0],
        ],
        location_info=site_data_1["location_info"],
    )
    for key in ["mineral_inventory"]:
        gold_site_data.pop(key)

    assert server_site_data == gold_site_data
