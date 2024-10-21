from __future__ import annotations

from datetime import datetime
from enum import Enum
from hashlib import sha256
from typing import Literal, Optional
from uuid import uuid4

from passlib.context import CryptContext
from sqlmodel import Field, SQLModel, String

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


class UserBase(SQLModel):
    username: str = Field(max_length=100, primary_key=True)
    name: str
    email: str

    def is_system(self):
        return self.username in ("inferlink", "sri", "umn", "usc")


class User(UserBase, table=True):
    salt: str = Field(default_factory=lambda: str(uuid4()))
    password: str = Field(max_length=64)
    scope: Literal["admin", "user"] = Field(default="user", sa_type=String)

    def encrypt_password(self):
        self.password = pwd_context.hash(self.salt + self.password)

    def verify_password(self, password: str):
        return pwd_context.verify(self.salt + password, self.password)


class UserPublic(UserBase): ...


class UserCreate(UserBase):
    password: str


class UserUpdate(UserBase):
    name: Optional[str]
    email: Optional[str]
    password: Optional[str]
