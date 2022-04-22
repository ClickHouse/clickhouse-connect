import logging
from typing import Type

from sqlalchemy.sql.base import SchemaEventTarget
from sqlalchemy.sql.visitors import Visitable

engine_map: dict[str, Type['TableEngine']] = {}


class TableEngine(SchemaEventTarget, Visitable):

    def __init_subclass__(cls, **kwargs):
        engine_map[cls.__name__] = cls

    def compile(self):
        return f'Engine {self.__class__.__name__}{self._engine_params()}'

    def _set_parent(self, parent):
        parent.engine = self

    def _engine_params(self):
        raise NotImplementedError


class MergeTree(TableEngine):
    def __init__(self, order_by, **_):
        self.order_by = [order_by] if isinstance(order_by, str) else order_by

    def _engine_params(self):
        return f" ORDER BY ({','.join(self.order_by)})"


def build_engine(name: str, *args, **kwargs):
    if not name:
        return None
    try:
        engine_cls = engine_map[name]
    except KeyError:
        logging.warning('Engine %s not found', name)
        return None
    return engine_cls(*args, **kwargs)
