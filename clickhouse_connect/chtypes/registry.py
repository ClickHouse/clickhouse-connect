import logging

from typing import NamedTuple


type_map = {}


def ch_type(cls):
    if not cls.name:
        cls.name = cls.__name__
    type_map[cls.name] = cls


class TypeDef(NamedTuple):
    base: str
    size: int
    wrappers: tuple
    nested: tuple
    keys: tuple
    values: tuple


def get_from_name(name:str):
    return get_from_def(_parse_name(name))


def get_from_def(type_def:TypeDef):
    try:
        type_cls = type_map[type_def.base]
    except KeyError:
        logging.error('Unrecognized ClickHouse type %s', type_def.base)
        raise
    return type_cls.build(type_def)


def _parse_name(name:str) -> TypeDef:
    working = None
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
        keys, values = _parse_enum(base)
        base = base[:base.find('(')]
    elif base.startswith('Array'):
        nt = base[base.find('(') + 1:-1]
        nested.append(_parse_name(nt))
        base = 'Array'
    return TypeDef(base, size, tuple(wrappers), tuple(nested), keys, values)


def _parse_enum(name):
    keys = []
    values = []
    pos = name.find('(')
    escaped = False
    in_key = False
    key = ''
    value = ''
    while True:
        pos += 1
        char = name[pos]
        if in_key:
            if escaped:
                key += char
                escaped = False
            else:
                if char == "'":
                    keys.append(key)
                    key = ''
                    in_key = False
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
                in_key = True
            else:
                value += char
    return tuple(keys), tuple(values)
