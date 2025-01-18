from __future__ import annotations

from typing import Optional

from minmodkg.models.kgrel.base import engine
from sqlalchemy import Engine


class EntityService:
    instance = None

    def __init__(self, _engine: Optional[Engine] = None):
        self.engine = _engine or engine

    @staticmethod
    def get_instance():
        if EntityService.instance is None:
            EntityService.instance = EntityService()
        return EntityService.instance
