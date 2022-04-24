import logging
from argparse import ArgumentError
from typing import Type

from sqlalchemy.sql.base import SchemaEventTarget
from sqlalchemy.sql.visitors import Visitable

engine_map: dict[str, Type['TableEngine']] = {}


def tuple_expr(expr_name, value):
    if value is None:
        return ''
    v = f' {expr_name} '
    if isinstance(value, (tuple, list)):
        return f"{v} ({','.join(value)})"
    return f'{v} {value}'


class TableEngine(SchemaEventTarget, Visitable):
    def __init_subclass__(cls, **kwargs):
        engine_map[cls.__name__] = cls

    @property
    def name(self):
        return self.__class__.__name__

    def compile(self):
        return f'Engine {self.name}{self._engine_params()}'

    def _set_parent(self, parent):
        parent.engine = self

    def _engine_params(self):
        raise NotImplementedError


class MergeTree(TableEngine):
    def __init__(self, order_by=None, primary_key=None, **kwargs):
        if not order_by and not primary_key:
            raise ArgumentError(None, 'Either PRIMARY KEY or ORDER BY must be specified')
        if primary_key and not order_by:
            order_by = primary_key
        self.order_by = order_by
        self.primary_key = primary_key
        self.args = kwargs
        self.arg_str = None

    def _engine_params(self):
        if self.arg_str:
            return self.arg_str
        v = tuple_expr('ORDER BY', self.order_by)
        v += tuple_expr('PARTITION BY', self.args.get('partition_by'))
        v += tuple_expr('PRIMARY KEY', self.primary_key)
        v += tuple_expr('SAMPLE BY', self.args.get('sample_by'))
        self.arg_str = v
        return v


def build_engine(name: str, *args, **kwargs):
    if not name:
        return None
    try:
        engine_cls = engine_map[name]
    except KeyError:
        logging.warning('Engine %s not found', name)
        return None
    return engine_cls(*args, **kwargs)
