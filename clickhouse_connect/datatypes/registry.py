import re
import logging
from decimal import Decimal

from typing import Tuple, NamedTuple, Any, Type, Dict, TYPE_CHECKING

if TYPE_CHECKING:
    from clickhouse_connect.datatypes.datatypes import ClickHouseType


class TypeDef(NamedTuple):
    size: int
    wrappers: tuple
    keys: tuple
    values: tuple


type_map: Dict[str, Type['ClickHouseType']] = {}


def register_bases(*args):
    for cls in args:
        type_map[cls.__name__.upper()] = cls


size_pattern = re.compile(r'([A-Z]+)(\d+)')


def get_from_name(name:str) -> 'ClickHouseType':
    working = None
    base = name
    size = 0
    wrappers = []
    keys = tuple()
    values = []
    arg_str = ''
    while base != working:
        working = base
        if base.upper().startswith('NULLABLE'):
            wrappers.append('Nullable')
            base = base[9:-1]
        if base.upper().startswith('LOWCARDINALITY'):
            wrappers.append('LowCardinality')
            base = base[15:-1]
    if base.upper().startswith('ENUM'):
        keys, values = _parse_enum(base)
        base = base[:base.find('(')]
    paren = base.find('(')
    if paren != -1:
        arg_str = base[paren + 1: -1]
        base = base[:paren]
    if base.upper().startswith('ARRAY'):
        values = [get_from_name(arg_str)]
    elif arg_str:
        values = _parse_args(arg_str)
    base = base.upper()
    match = size_pattern.match(base)
    if match:
        base = match.group(1)
        size = int(match.group(2))
    try:
        type_cls = type_map[base]
    except KeyError:
        err_str = f'Unrecognized ClickHouse type base: {base} name: {name}'
        logging.error(err_str)
        raise Exception(err_str)
    return type_cls(TypeDef(size, tuple(wrappers),  keys, tuple(values)))


def _parse_enum(name) -> Tuple[Tuple[str], Tuple[int]]:
    keys = []
    values = []
    pos = name.find('(') + 1
    escaped = False
    in_key = False
    key = ''
    value = ''
    while True:
        char = name[pos]
        pos += 1
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


def _parse_args(name) -> [Any]:
    values = []
    value = ''
    in_str = False
    escaped = False
    pos = 0
    while pos < len(name):
        char = name[pos]
        pos += 1
        if in_str:
            if escaped:
                value += char
                escaped = False
            else:
                if char == "'":
                    values.append(value)
                    value = ''
                    in_str = False
                elif char == '\\':
                    escaped = True
                else:
                    value += char
        elif char != ' ':
            if char == ',':
                if '.' in value:
                    values.append(Decimal(value))
                else:
                    values.append(int(value))
    return values



