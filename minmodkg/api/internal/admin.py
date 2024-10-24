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

router = APIRouter(tags=["admin"])


@router.get("/users", response_model=list[UserPublic])
def get_users(
    session: SessionDep,
    user: CurrentUserDep,
    offset: int = 0,
    limit: Annotated[int, Query(le=100)] = 100,
):
    """Get all users"""
    users = session.exec(select(User).offset(offset).limit(limit)).all()
    return users


@router.post("/users", response_model=UserPublic)
def create_user(user: UserCreate, session: SessionDep, current_user: CurrentUserDep):
    """Create a new user"""
    if current_user.scope != "admin":
        raise HTTPException(403, "You do not have permission to create a new user")
    return create_user_priv(user, session)


def create_user_priv(user: UserCreate, session: SessionDep):
    exist_user = session.get(User, user.username)
    if exist_user is not None:
        raise HTTPException(400, "Username already exists")

    dbuser = User.model_validate(user)
    dbuser.encrypt_password()

    session.add(dbuser)
    session.commit()
    session.refresh(dbuser)
    return dbuser
