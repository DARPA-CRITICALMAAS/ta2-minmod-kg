from __future__ import annotations

import hashlib
from typing import Callable
from uuid import uuid4

from drepr.models.preprocessing import Context
from minmodkg.api.models.mineral_site import PageInfo
from minmodkg.config import MNO_NS, MNR_NS
from slugify import slugify


def make_site_ids(value: dict, namespace: str = MNR_NS):
    """Make all ids for a mineral site"""
    site_uri = make_site_uri(value["source_id"], value["record_id"], namespace)
    site_id = site_uri[len(namespace) :]

    # we create id for each reference as it is likely to be the same or re-ordered
    value["id"] = site_uri
    for ref in value["reference"]:
        make_reference_ids(ref, site_id, namespace)
    for inv in value.get("mineral_inventory", []):
        make_reference_ids(inv["reference"], site_id, namespace)

    # for rest, we create id based on its index
    if "location_info" in value:
        make_location_info_ids(value["location_info"], site_id, namespace)

    for deptype_i, deptype in enumerate(value.get("deposit_type_candidate", [])):
        deptype["id"] = f"{namespace}{site_id}__deptype__{deptype_i}"

    for inv_i, inv in enumerate(value.get("mineral_inventory", [])):
        make_inventory_ids(inv, inv_i, site_id, namespace)


def make_location_info_ids(loc_info: dict, site_id: str, namespace: str = MNR_NS):
    loc_info["id"] = namespace + site_id + "__location_info"
    for i, country in enumerate(loc_info.get("country", [])):
        country["id"] = namespace + site_id + f"__country__{i}"
    for i, state in enumerate(loc_info.get("state_or_province", [])):
        state["id"] = namespace + site_id + f"__state__{i}"
    if loc_info.get("crs") is not None:
        loc_info["crs"]["id"] = namespace + site_id + f"__crs"


def make_inventory_ids(
    inv: dict, inv_index: int, site_id: str, namespace: str = MNR_NS
):
    inv_id = f"{site_id}__inv__{inv_index}"
    inv["id"] = f"{namespace}{inv_id}"
    for cat_idx, cat in enumerate(inv.get("category", [])):
        cat["id"] = f"{namespace}{inv_id}__cat__{cat_idx}"
    for key in ["commodity", "material_form"]:
        if inv.get(key) is not None:
            inv[key]["id"] = f"{namespace}{inv_id}__{key}"
    for key in ["cutoff_grade", "grade", "ore"]:
        if inv.get(key) is not None:
            inv[key]["id"] = f"{namespace}{inv_id}__{key}"
            if inv[key].get("unit") is not None:
                inv[key]["unit"]["id"] = f"{namespace}{inv_id}__{key}__unit"
    make_reference_ids(inv["reference"], site_id, namespace)


def make_reference_ids(ref: dict, site_id: str, namespace: str = MNR_NS):
    ref["document"]["id"] = make_document_uri(ref["document"], site_id, namespace)

    docid = ref["document"]["id"]
    if docid.startswith(namespace):
        docid = docid[len(namespace) :]
    elif docid.startswith("https://"):
        docid = docid[8:]
    elif docid.startswith("http://"):
        docid = docid[7:]
    docid = slugify(docid)

    ref["id"] = make_reference_uri(ref, slugify(docid), namespace)
    for i, pageinfo in enumerate(ref.get("page_info", [])):
        pageinfo["id"] = f"{ref['id']}__pageinfo__{i}"


def make_site_uri(source_id: str, record_id: str, namespace: str = MNR_NS) -> str:
    if source_id.find("::") != -1:
        # we need to remove the category from the source_id
        category, source_id = source_id.split("::", 1)
        assert category in {
            "mining-report",
            "article",
            "database",
            "curated-mining-report",
            "curated-article",
            "curated-database",
        }

    if source_id.startswith("http://"):
        if source_id.startswith("http://"):
            source_id = source_id[7:]
            if source_id.endswith("/"):
                source_id = source_id[:-1]
    elif source_id.startswith("https://"):
        source_id = source_id[8:]
        if source_id.endswith("/"):
            source_id = source_id[:-1]

    source_id = slugify(source_id)

    if isinstance(record_id, int):
        record_id = str(record_id)
    else:
        record_id = slugify(record_id)

    path = shorten_id(f"{source_id}__{record_id}", 120)
    return f"{namespace}site__{path}"


def make_document_uri(doc: dict, site_id: str, namespace: str = MNR_NS):
    if "uri" in doc:
        return doc["uri"]
    if "doi" in doc:
        assert not (
            doc["doi"].startswith("https://")
            or doc["doi"].startswith("http://")
            or doc["doi"].startswith("doi:")
            or doc["doi"].startswith("/")
        )
        return "https://doi.org/" + doc["doi"]

    if "title" not in doc:
        raise ValueError("Document must have a URI, DOI, or at least a title")

    path = site_id + "__doc__" + shorten_id(slugify(doc["title"]), 120)
    return f"{namespace}{path}"


def make_reference_uri(ref: dict, doc_id: str, namespace: str = MNR_NS):
    # gen pageinfo id
    if len(ref.get("page_info", [])) > 0:
        pageinfo_id = hashlib.sha256(
            b"|".join(
                (
                    PageInfo.model_validate(page_info).to_enc_str().encode()
                    for page_info in ref["page_info"]
                )
            )
        ).hexdigest()[:16]
    else:
        pageinfo_id = ""

    if "property" in ref:
        property = ref["property"]
        assert property.startswith(MNO_NS)
        property = property[len(MNO_NS) :]
    else:
        property = ""

    constraintinfo = property + "_" + pageinfo_id
    if len(constraintinfo) == 1:
        return namespace + doc_id + "__ref"
    else:
        return namespace + doc_id + "__ref__" + shorten_id(slugify(constraintinfo), 120)


def get_uuid4(prefix: str = "B", namespace: str = MNR_NS):
    return f"{namespace}{prefix}_{str(uuid4()).replace('-', '')}"


def shorten_id(long_id: str, max_length: int = 120):
    if len(long_id) > max_length:
        return (
            long_id[:max_length]
            + "__"
            + hashlib.sha256(long_id.encode()).hexdigest()[:8]
        )
    return long_id
