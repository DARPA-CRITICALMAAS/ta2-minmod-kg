from __future__ import annotations

from fastapi import HTTPException, status
from httpx import Response


class UnconvertibleUnitError(Exception):
    pass


class TransactionError(HTTPException):
    def __init__(self, message: str):
        super().__init__(status_code=status.HTTP_409_CONFLICT, detail=message)


class DBError(Exception):

    def __init__(self, message: str, resp: Response):
        self.message = message
        self.resp = resp


class UnreachableError(Exception):
    pass
