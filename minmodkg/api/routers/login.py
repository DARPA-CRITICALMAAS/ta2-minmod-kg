from __future__ import annotations

from datetime import datetime, timedelta, timezone
from hashlib import sha256
from typing import Annotated
from uuid import uuid4

import jwt
from fastapi import APIRouter, HTTPException, Query, Response, status
from jwt.exceptions import InvalidTokenError
from minmodkg.api.dependencies import CurrentUserDep, TokenDep
from minmodkg.api.models.db import SessionDep
from minmodkg.api.models.user import User, UserCreate, UserPublic
from minmodkg.config import JWT_ACCESS_TOKEN_EXPIRE_MINUTES, JWT_ALGORITHM, SECRET_KEY
from passlib.context import CryptContext
from sqlmodel import select

router = APIRouter(tags=["login"])


@router.post("/login")
async def login(
    session: SessionDep,
    response: Response,
    username: str,
    password: str,
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
        {"sub": user.username, "exp": expired_at.timestamp()},
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
