import logging
from typing import Type, Sequence

from sqlalchemy.exc import ArgumentError, SQLAlchemyError
from sqlalchemy.sql.base import SchemaEventTarget
from sqlalchemy.sql.visitors import Visitable

engine_map: dict[str, Type['TableEngine']] = {}


def tuple_expr(expr_name, value):
    """
    Create a table parameter with a tuple or list correctly formatted
    :param expr_name: parameter
    :param value: string or tuple of strings to format
    :return: formatted parameter string
    """
    if value is None:
        return ''
    v = f' {expr_name} '
    if isinstance(value, (tuple, list)):
        return f"{v} ({','.join(value)})"
    return f'{v} {value}'


class TableEngine(SchemaEventTarget, Visitable):
    """
    SqlAlchemy Schema element to support ClickHouse table engines
    """
    arg_str = None

    def __init_subclass__(cls, **kwargs):
        engine_map[cls.__name__] = cls

    @property
    def name(self):
        return self.__class__.__name__

    def compile(self):
        if not self.arg_str:
            self.arg_str = self._engine_params()
        return f'Engine {self.name}{self.arg_str}'

    def check_primary_keys(self, primary_keys: Sequence):
        raise SQLAlchemyError(f'Table Engine {self.name} does not support primary keys')

    def _set_parent(self, parent):
        parent.engine = self

    def _engine_params(self):
        raise NotImplementedError


class MergeTree(TableEngine):
    """
    ClickHouse MergeTree engine with required/optional parameters
    """

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
        v = tuple_expr('ORDER BY', self.order_by)
        v += tuple_expr('PARTITION BY', self.args.get('partition_by'))
        v += tuple_expr('PRIMARY KEY', self.primary_key)
        v += tuple_expr('SAMPLE BY', self.args.get('sample_by'))
        return v


class ReplicatedMergeTree(MergeTree):
    """
    ClickHouse ReplicatedMergeTree engine with required/optional parameters
    """

    def __init__(self, order_by=None, primary_key=None, **kwargs):
        super().__init__(order_by, primary_key, **kwargs)
        self.zk_path = kwargs.pop('zk_path', None)
        self.replica = kwargs.pop('replica', None)

    def _engine_params(self):
        if self.zk_path and self.replica:
            return f"('{self.zk_path}', '{self.replica}') " + super()._engine_params()
        return super()._engine_params()


def build_engine(name: str, *args, **kwargs):
    """
    Factory function to create TableEngine class from name and parameters
    :param name: Engine class name
    :param args: Engine arguments
    :param kwargs: Engine keyword arguments
    :return:
    """
    if not name:
        return None
    try:
        engine_cls = engine_map[name]
    except KeyError:
        logging.warning('Engine %s not found', name)
        return None
    return engine_cls(*args, **kwargs)
