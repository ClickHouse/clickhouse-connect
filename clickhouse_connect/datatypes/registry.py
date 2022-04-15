import re
import logging

from typing import Tuple, Any, List
from clickhouse_connect.datatypes.base import TypeDef, ClickHouseType, type_map
from clickhouse_connect.driver.exceptions import InternalError

size_pattern = re.compile(r'^([A-Z]+)(\d+)')
int_pattern = re.compile(r'^-?\d+$')


def parse_name(name: str, no_nulls: bool = False) -> Tuple[str, str, TypeDef]:
    base = name
    size = 0
    wrappers = []
    keys = tuple()
    values = tuple()
    if base.upper().startswith('LOWCARDINALITY'):
        wrappers.append('LowCardinality')
        base = base[15:-1]
    if base.upper().startswith('NULLABLE'):
        if not no_nulls:
            wrappers.append('Nullable')
        base = base[9:-1]
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
    return base, name, TypeDef(size, tuple(wrappers), keys, values)


def get_from_name(name: str, no_nulls: bool = False) -> ClickHouseType:
    base, name, type_def = parse_name(name, no_nulls)
    try:
        return type_map[base].build(type_def)
    except KeyError:
        err_str = f'Unrecognized ClickHouse type base: {base} name: {name if name else base}'
        logging.error(err_str)
        raise InternalError(err_str) from None


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
                elif char == '\\' and name[pos] == "'" and name[pos:pos + 4] != "' = " and name[pos:] != "')":
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
    values, keys = zip(*sorted(zip(values, keys)))
    return tuple(keys), tuple(values)


# pylint: disable=too-many-branches
def _parse_args(name) -> [Any]:
    values: List[Any] = []
    value = ''
    sz = len(name)
    in_str = False
    escaped = False
    pos = 0
    parens = 0

    def add_value():
        if int_pattern.match(value):
            values.append(int(value))
        else:
            values.append(value)

    while pos < sz:
        char = name[pos]
        pos += 1
        if in_str:
            value += char
            if escaped:
                escaped = False
            else:
                if char == "'":
                    in_str = False
                elif char == '\\' and name[pos] == "'" and name[pos:pos + 4] != "' = " and name[pos:pos + 2] != "')":
                    escaped = True
        else:
            while char == ' ' and 'Enum' not in value:
                char = name[pos]
                pos += 1
                if pos == sz:
                    break
            if char == ',' and not parens:
                add_value()
                value = ''
            else:
                if char == "'" and (not value or 'Enum' in value):
                    in_str = True
                elif char == '(':
                    parens += 1
                elif char == ')' and parens:
                    parens -= 1
                value += char
    if value != '':
        add_value()
    return tuple(values)
