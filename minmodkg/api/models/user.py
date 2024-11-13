from __future__ import annotations

from datetime import datetime
from enum import Enum
from hashlib import sha256
from typing import Literal, Optional
from uuid import uuid4

import bcrypt
from sqlmodel import Field, SQLModel, String


class Role(str, Enum):
    admin = "admin"
    user = "user"
    system = "system"


class UserBase(SQLModel):
    username: str = Field(max_length=100, primary_key=True)
    name: str
    email: str


class User(UserBase, table=True):
    password: bytes = Field(max_length=64)
    role: Literal["admin", "user", "system"] = Field(default="user", sa_type=String)

    def is_system(self):
        return self.role == Role.system

    def encrypt_password(self):
        self.password = bcrypt.hashpw(self.password, bcrypt.gensalt())

    def verify_password(self, password: str):
        return bcrypt.checkpw(password.encode(), self.password)


class UserPublic(UserBase): ...


class UserCreate(UserBase):
    password: str


class UserUpdate(UserBase):
    name: Optional[str]
    email: Optional[str]
    password: Optional[str]
