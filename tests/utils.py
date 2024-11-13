from __future__ import annotations

from typing import Callable

import httpx
from loguru import logger


def check_req(cb: Callable[[], httpx.Response]):
    try:
        resp = cb()
        resp.raise_for_status()
        return resp
    except httpx.HTTPStatusError as err:
        logger.exception(err)
        raise
