from __future__ import annotations

from httpx import Response


class UnconvertibleUnitError(Exception):
    pass


class TransactionError(Exception):
    pass


class DBError(Exception):

    def __init__(self, message: str, resp: Response):
        self.message = message
        self.resp = resp
