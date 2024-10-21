from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from typing import Annotated, Optional

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import APIKeyCookie
from minmodkg.api.models.db import SessionDep
from minmodkg.api.models.user import User
from minmodkg.config import JWT_ALGORITHM, MNR_NS, SECRET_KEY, SPARQL_ENDPOINT
from minmodkg.misc import LongestPrefixIndex, sparql_query

# for login/security
token_from_cookie = APIKeyCookie(name="session")
TokenDep = Annotated[str, Depends(token_from_cookie)]


async def get_current_user(session: SessionDep, token: TokenDep):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[JWT_ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        expired_at = datetime.fromtimestamp(payload.get("exp"), timezone.utc)
        if expired_at < datetime.now(timezone.utc):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Credentials expired",
            )
    except jwt.InvalidTokenError:
        raise credentials_exception

    user = session.get(User, username)
    if user is None:
        raise credentials_exception
    return user


CurrentUserDep = Annotated[User, Depends(get_current_user)]


def get_snapshot_id(endpoint: str = SPARQL_ENDPOINT):
    query = "SELECT ?snapshot_id WHERE { mnr:kg dcterms:hasVersion ?snapshot_id }"
    qres = sparql_query(query, endpoint)
    return qres[0]["snapshot_id"]


def norm_commodity(commodity: str, endpoint: str = SPARQL_ENDPOINT) -> str:
    if commodity.startswith("http"):
        raise HTTPException(
            status_code=404,
            detail=f"Expect commodity to be either just an id (QXXX) or name. Get `{commodity}` instead",
        )
    if not is_minmod_id(commodity):
        uri = get_commodity_by_name(commodity, endpoint)
        if uri is None:
            raise HTTPException(
                status_code=404, detail=f"Commodity `{commodity}` not found"
            )
        commodity = uri
    return commodity


def is_minmod_id(text: str) -> bool:
    return text.startswith("Q") and text[1:].isdigit()


def get_commodity_by_name(name: str, endpoint: str = SPARQL_ENDPOINT) -> Optional[str]:
    query = (
        'SELECT ?uri WHERE { ?uri a :Commodity ; rdfs:label ?name . FILTER(LCASE(STR(?name)) = "%s") } LIMIT 1'
        % name.lower()
    )
    qres = sparql_query(query, endpoint)
    if len(qres) == 0:
        return None
    uri = qres[0]["uri"]
    assert uri.startswith(MNR_NS)
    uri = uri[len(MNR_NS) :]
    return uri


def rank_source(
    source_id: str, snapshot_id: str, endpoint: str = SPARQL_ENDPOINT
) -> int:
    """Get ranking of a source, higher is better"""
    default_score = 5
    score = get_source_scores(snapshot_id, endpoint).get_score(source_id)
    if score is None:
        print("Unknown source id:", source_id)
        return default_score
    return score


@dataclass
class SourceScore:
    source2score: dict[str, int]
    index: LongestPrefixIndex

    def get_score(self, source_id: str) -> Optional[int]:
        prefix = self.index.get(source_id)
        if prefix is not None:
            return self.source2score[prefix]
        return None


@lru_cache(maxsize=1)
def get_source_scores(snapshot_id: str, endpoint: str = SPARQL_ENDPOINT):
    query = """
    SELECT ?uri ?prefixed_id ?score
    WHERE {
        ?uri a :SourceScore ;
            :prefixed_id ?prefixed_id ;
            :score ?score
    }
    """
    qres = sparql_query(query, endpoint)
    source2score = {record["prefixed_id"]: record["score"] for record in qres}
    index = LongestPrefixIndex.create(list(source2score.keys()))
    return SourceScore(source2score, index)
