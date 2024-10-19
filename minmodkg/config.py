from __future__ import annotations

import os
from pathlib import Path

import serde.yaml

cfg_file = Path(__file__).parent.parent / "config.yml"
cfg = serde.yaml.deser(cfg_file)

# for sparql/ontology
DEFAULT_ENDPOINT = cfg["sparql_endpoint"]
MNR_NS = cfg["mnr_ns"]
MNO_NS = cfg["mno_ns"]

# for databases
DBFILE = Path(cfg["dbfile"])
DBFILE.parent.mkdir(parents=True, exist_ok=True)

SECRET_KEY = cfg["secret_key"]
JWT_ALGORITHM = "HS256"
JWT_ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24 * 3  # 3 days
