from __future__ import annotations

import os
from pathlib import Path

import serde.yaml

if "CFG_FILE" not in os.environ:
    CFG_FILE = Path(__file__).parent.parent / "config.yml"
else:
    CFG_FILE = Path(os.environ["CFG_FILE"])
assert CFG_FILE.exists(), f"Config file {CFG_FILE} does not exist"
cfg = serde.yaml.deser(CFG_FILE)

# for sparql/ontology
SPARQL_ENDPOINT = cfg["triplestore"]["query"]
SPARQL_UPDATE_ENDPOINT = cfg["triplestore"]["update"]
MNR_NS = cfg["mnr_ns"]
MNO_NS = cfg["mno_ns"]

# for databases
DBFILE = Path(cfg["dbfile"])
DBFILE.parent.mkdir(parents=True, exist_ok=True)

# for login/security
SECRET_KEY = cfg["secret_key"]
JWT_ALGORITHM = "HS256"
JWT_ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24 * 3  # 3 days
