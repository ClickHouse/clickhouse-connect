import random
import re
from collections.abc import Sequence

import pkg_resources

from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver.extras import random_col_data

LOW_CARD_PERC = 0.4
NULLABLE_PERC = 0.2
TUPLE_MAX = 5
FIXED_STR_RANGE = 256
NESTED_DEPTH = 3

weighted_types = (('Int8', 1), ('UInt8', 1), ('Int16', 1), ('UInt16', 1), ('Int32', 1), ('UInt32', 1), ('Int64', 1),
                  ('UInt64', 2), ('Int128', 1), ('UInt128', 1), ('Int256', 1), ('UInt256', 1), ('String', 8),
                  ('FixedString', 4), ('Float32', 2), ('Float64', 2), ('UUID', 2), ('Array', 8), ('Tuple', 4),
                  ('Map', 0))
all_types, all_weights = tuple(zip(*weighted_types))
nested_types = ['Array', 'Tuple', 'Map']
terminal_types = set(all_types) - set(nested_types)
total_weight = sum(all_weights)
all_weights = [x / total_weight for x in all_weights]

random.seed()


def random_type(depth: int = 0, low_card_perc: float = LOW_CARD_PERC, nullable_perc: float = NULLABLE_PERC):
    base_type = random.choices(all_types, all_weights)[0]
    while depth >= NESTED_DEPTH and base_type in nested_types:
        base_type = random.choices(all_types, all_weights)[0]
    if base_type in terminal_types:
        if base_type == 'FixedString':
            base_type = f'{base_type}({random.randint(1, FIXED_STR_RANGE)})'
        if random.random() < nullable_perc:
            base_type = f'Nullable({base_type})'
        if 'String' in base_type and random.random() < low_card_perc:
            base_type = f'LowCardinality({base_type})'
        return get_from_name(base_type)
    return build_nested_type(base_type, depth)


def build_nested_type(base_type: str, depth: int):
    if base_type == 'Array':
        element = random_type(depth + 1)
        return get_from_name(f'Array({element.name})')
    if base_type == 'Tuple':
        elements = [random_type(depth + 1) for _ in range(random.randint(1, TUPLE_MAX))]
        return get_from_name(f"Tuple({', '.join(x.name for x in elements)})")
    return None


def random_columns(cnt: int = 16, col_prefix: str = 'col'):
    col_names = []
    col_types = []
    for y in range(cnt):
        col_type = random_type()
        col_types.append(col_type)
        short_name = re.sub(r'(,|\s+|\(+|\)+)', '_', col_type.name.lower())
        short_name = re.sub(r'_+', '_', short_name)
        short_name = short_name.replace('nullable', 'n').replace('lowcardinality', 'lc')
        col_names.append(f'{col_prefix}{y}_{short_name[:24]}')
    return tuple(col_names), tuple(col_types)


def random_data(col_types: Sequence[ClickHouseType], num_rows: int = 1):
    data = [tuple(random_col_data(col_type, num_rows)) for col_type in col_types]
    all_cols = [list(range(num_rows))]
    all_cols.extend(data)
    return list(zip(*all_cols))


def to_bytes(hex_str: str):
    return memoryview(bytes.fromhex(hex_str))


def to_hex(b: bytes):
    lines = [(b[ix: ix + 16].hex(' ', -2)) for ix in range(0, len(b), 16)]
    return '\n'.join(lines)


def add_test_entry_points():
    dist = pkg_resources.Distribution('clickhouse-connect')
    ep1 = pkg_resources.EntryPoint.parse(
        'clickhousedb.connect = clickhouse_connect.cc_sqlalchemy.dialect:ClickHouseDialect', dist=dist)
    ep2 = pkg_resources.EntryPoint.parse(
        'clickhousedb = clickhouse_connect.cc_sqlalchemy.dialect:ClickHouseDialect', dist=dist)
    entry_map = dist.get_entry_map()
    entry_map['sqlalchemy.dialects'] = {'clickhousedb.connect': ep1, 'clickhousedb': ep2}
    pkg_resources.working_set.add(dist)
    print('test eps added to distribution')
