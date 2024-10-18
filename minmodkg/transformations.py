from __future__ import annotations

import hashlib
from uuid import uuid4

from slugify import slugify

MNR_NS = "https://minmod.isi.edu/resource/"


def make_site_uri(source_id: str, record_id: str, namespace: str = MNR_NS) -> str:
    if source_id.find("::") != -1:
        # we need to remove the category from the source_id
        category, source_id = source_id.split("::")
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

    path = f"{source_id}__{record_id}"
    if len(path) > 120:
        path = path[:120] + "__" + hashlib.sha256(path.encode()).hexdigest()[:8]
    return f"{namespace}site__{path}"


def get_uuid4(prefix: str = "B", namespace: str = MNR_NS):
    return f"{namespace}{prefix}_{str(uuid4()).replace('-', '')}"
