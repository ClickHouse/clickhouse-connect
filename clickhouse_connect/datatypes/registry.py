import array
import re
import logging

from typing import Tuple, Any, List

from clickhouse_connect.datatypes.base import TypeDef, ClickHouseType, type_map
from clickhouse_connect.driver import DriverError

size_pattern = re.compile(r'^([A-Z]+)(\d+)')
int_pattern = re.compile(r'^-?\d+$')


def get_from_name(name: str) -> ClickHouseType:
    base = name
    size = 0
    wrappers = []
    keys = tuple()
    values = tuple()
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
        arg_str = base[paren + 1:-1]
        base = base[:paren]
        values = _parse_args(arg_str)
    base = base.upper()
    if base not in type_map:
        match = size_pattern.match(base)
        if match:
            base = match.group(1)
            size = int(match.group(2))
    try:
        type_cls = type_map[base]
    except KeyError:
        err_str = f'Unrecognized ClickHouse type base: {base} name: {name}'
        logging.error(err_str)
        raise DriverError(err_str)
    return type_cls.build(TypeDef(size, tuple(wrappers), keys, values))


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
            elif char == "'" and not value:
                in_key = True
            else:
                value += char
    return tuple(keys), tuple(values)


def _parse_args(name) -> [Any]:
    values: List[Any] = []
    value = ''
    l = len(name)
    in_str = False
    escaped = False
    pos = 0

    def add_value():
        if int_pattern.match(value):
            values.append(int(value))
        else:
            values.append(value)

    while pos < l:
        char = name[pos]
        pos += 1
        if in_str:
            value += char
            if escaped:
                escaped = False
            else:
                if char == "'":
                    in_str = False
                elif char == '\\':
                    escaped = True
        else:
            while char == ' ':
                char = name[pos]
                pos += 1
                if pos == l:
                    break
            if char == ',':
                add_value()
                value = ''
            else:
                if char == "'" and not value:
                    in_str = True
                value += char
    if value != '':
        add_value()
    return tuple(values)
