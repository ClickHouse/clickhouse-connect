import logging

from typing import Callable, NamedTuple

from sqlalchemy.sql.type_api import TypeEngine
from superset.utils.core import GenericDataType

type_map = {}


def reflect(cls, type_def):
    return cls


def compile(cls, *args, **kwargs):
    return cls.ch_cls.label(*args, **kwargs)


def ch_type(sqla_type: TypeEngine, gen_type: GenericDataType, build: Callable = None):

    def inner(cls):
        if not hasattr(cls, 'name'):
            cls.name = cls.__name__
        sqla_cls = type(cls.name, (sqla_type,), {'ch_cls': cls})
        sqla_cls.compile = classmethod(compile)
        cls.sqla_type = sqla_cls
        cls.gen_type = gen_type
        cls.build = classmethod(build if build else reflect)
        type_map[cls.name] = cls
        type_map[cls.name.upper()] = cls
    return inner


def get(name):
    type_def = _parse_name(name)
    try:
        type_cls = type_map[type_def.base]
    except KeyError:
        logging.error('Unrecognized ClickHouse type {}, base: {}', name, type_def.base)
        raise
    return type_cls.build(type_def)


class TypeDef(NamedTuple):
    base: str
    size: int = 0
    wrappers: tuple = ()
    nested: tuple = ()


def _parse_name(name:str) -> TypeDef:
    working = '<init_wrapped>'
    base = name.upper()
    wrappers = []
    nested = []
    size = 0
    while base != working:
        working = base
        if base.startswith('NULLABLE'):
            wrappers.append('Nullable')
            base = base[9:-1]
        if base.startswith('FIXEDSTRING'):
            size = int(base[12:-1])
            base = 'FixedString'
        if base.startswith('LOWCARDINALITY'):
            wrappers.append('LowCardinality')
            base = base[15:-1]
    return TypeDef(base, size, tuple(wrappers), tuple(nested))










