from jsonschema import validate, ValidationError
import pytest

count_schema = {
    "type": "object",
    "properties": {
        "total": {"type": "integer"},
    },
    "required": ["total"],
}

count_by_commodity_schema = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "commodity_uri": {"type": "string"},
            "commodity_label": {"type": "string"},
            "total": {"type": "integer"},
        },
        "required": ["commodity_uri", "commodity_label", "total"],
    },
}


def test_document_count(client):
    response = client.get("/api/v1/documents/count")
    assert response.status_code == 200
    response_json = response.json()
    validate(instance=response_json, schema=count_schema)


def test_mineral_sites_count(client):
    response = client.get("/api/v1/mineral-sites/count")
    assert response.status_code == 200
    response_json = response.json()
    validate(instance=response_json, schema=count_schema)


def test_mineral_inventories_count(client):
    response = client.get("/api/v1/mineral-inventories/count")
    assert response.status_code == 200
    response_json = response.json()
    validate(instance=response_json, schema=count_schema)


def test_document_count_by_commodity(client):
    response = client.get("/api/v1/documents/count-by-commodity")
    assert response.status_code == 200
    response_json = response.json()
    validate(instance=response_json, schema=count_by_commodity_schema)


def test_mineral_sites_count_by_commodity(client):
    response = client.get("/api/v1/mineral-sites/count-by-commodity")
    assert response.status_code == 200
    response_json = response.json()
    validate(instance=response_json, schema=count_by_commodity_schema)


def test_mineral_inventories_count_by_commodity(client):
    response = client.get("/api/v1/mineral-inventories/count-by-commodity")
    assert response.status_code == 200
    response_json = response.json()
    validate(instance=response_json, schema=count_by_commodity_schema)
