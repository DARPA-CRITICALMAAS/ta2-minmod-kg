from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Annotated

import jwt
from fastapi import APIRouter, Body, HTTPException, Response, status
from minmodkg.api.dependencies import CurrentUserDep
from minmodkg.api.models.db import SessionDep
from minmodkg.api.models.user import User, UserPublic
from minmodkg.config import JWT_ACCESS_TOKEN_EXPIRE_MINUTES, JWT_ALGORITHM, SECRET_KEY

router = APIRouter(tags=["login"])


@router.post("/login")
async def login(
    session: SessionDep,
    response: Response,
    username: Annotated[str, Body()],
    password: Annotated[str, Body()],
):
    # authenticate user
    user = session.get(User, username)
    if user is None or not user.verify_password(password):
        # validate password
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
        )

    expired_at = datetime.now(timezone.utc) + timedelta(
        minutes=JWT_ACCESS_TOKEN_EXPIRE_MINUTES
    )
    access_token = jwt.encode(
        {"username": user.username, "exp": expired_at.timestamp()},
        SECRET_KEY,
        algorithm=JWT_ALGORITHM,
    )
    response.set_cookie(
        key="session",
        value=access_token,
    )
    return "Logged in"


@router.get("/whoami", response_model=UserPublic)
def whoami(user: CurrentUserDep):
    return user
