from __future__ import annotations

import hashlib

from slugify import slugify

MNR_NS = "https://minmod.isi.edu/resource/"


def make_site_uri(source_id: str, record_id: str) -> str:
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
    return f"{MNR_NS}site__{path}"
