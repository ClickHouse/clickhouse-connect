import logging

from typing import NamedTuple

from sqlalchemy.sql.type_api import TypeEngine
from superset.utils.core import GenericDataType

type_map = {}


def compile(self, *args, **kwargs):
    return self.ch_type.label(*args, **kwargs)


def get_sqla_type(self, *args, **kwargs):
    sqla_type = self.sqla_type_cls()
    sqla_type.ch_type = self
    return sqla_type


def ch_type(sqla_type: TypeEngine, gen_type: GenericDataType):

    def inner(cls):
        if not hasattr(cls, 'name'):
            cls.name = cls.__name__
        sqla_type_cls = type(cls.name, (sqla_type,), {})
        sqla_type_cls.compile = compile
        cls.sqla_type_cls = sqla_type_cls
        cls.gen_type = gen_type
        cls.get_sqla_type = get_sqla_type
        type_map[cls.name] = cls
    return inner


def get(name):
    type_def = _parse_name(name)
    try:
        type_cls = type_map[type_def.base]
    except KeyError:
        logging.error('Unrecognized ClickHouse type %s, base: %s', name, type_def.base)
        raise
    return type_cls.build(type_def)


class TypeDef(NamedTuple):
    base: str
    size: int
    wrappers: tuple
    nested: tuple
    keys: tuple
    values: tuple


def _parse_name(name:str) -> TypeDef:
    working = '<init_wrapped>'
    base = name
    wrappers = []
    nested = []
    keys = tuple()
    values = tuple()
    size = 0
    while base != working:
        working = base
        if base.startswith('Nullable'):
            wrappers.append('Nullable')
            base = base[9:-1]
        if base.startswith('Nullable'):
            size = int(base[12:-1])
            base = 'FixedString'
        if base.startswith('LowCardinality'):
            wrappers.append('LowCardinality')
            base = base[15:-1]
    if base.startswith('Enum'):
        keys, values = _parse_enum(name[-len(base):])
        base = base[:base.find('(')]
    return TypeDef(base, size, tuple(wrappers), tuple(nested), keys, values)


def _parse_enum(name):
    keys = []
    values = []
    pos = name.find('(')
    escaped = False
    inKey = False
    char = name[pos]
    key = ''
    value = ''
    while True:
        pos += 1
        char = name[pos]
        if inKey:
            if escaped:
                key += char
                escaped = False
            else:
                if char == "'":
                    keys.append(key)
                    key = ''
                    inKey = False
                elif char == '\\':
                    escaped = True
                else:
                    key += char
        elif char not in (' ', '='):
            if char == ',':
                values.append(int(value))
                value = ''
            elif char == ')':
                values.append(int(value))
                break
            elif char == "'":
                inKey = True
            else:
                value += char
    return tuple(keys), tuple(values)
