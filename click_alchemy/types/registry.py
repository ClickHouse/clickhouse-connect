from types import MethodType
from typing import Callable, NamedTuple

from sqlalchemy.sql.type_api import TypeEngine
from superset.utils.core import GenericDataType

type_map = {}


def _reflect(cls, type_def):
    return cls


def ch_type(visit_name:str, sqla_type: TypeEngine, gen_type: GenericDataType, build: Callable = _reflect):
    def inner(cls):
        cls.__visit_name__ = visit_name
        cls.sqla_type = sqla_type
        cls.gen_type = gen_type
        cls.build = classmethod(build)
        if not hasattr(cls, 'name'):
            cls.name = visit_name
        type_map[visit_name] = cls
    return inner


def get(name):
    type_def = _parse_name(name)
    type_cls = type_map[type_def.base]
    return type_cls.build(type_def)


class TypeDef(NamedTuple):
    base: str
    size: int = 0
    wrappers: tuple[str] = ()


def _parse_name(name:str) -> TypeDef:
    base = name
    wrappers = []
    size = 0
    if base.startswith('Array'):
        wrappers.append('Array')
        base = base[6:-1]
    if base.startswith('Nullable'):
        wrappers.append('Nullable')
        base = base[9:-1]
    if base.startswith('FixedString'):
        size = int(base[12:-1])
        base = 'FixedString'
    return TypeDef(base, size, tuple(wrappers))










