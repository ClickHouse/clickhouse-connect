import random
from collections.abc import Sequence

import pkg_resources

from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver.extras import random_col_data

SUPPORTED_TYPES = (('Int8', 1), ('UInt8', 1), ('Int16', 1), ('UInt16', 1), ('Int32', 1), ('UInt32', 1), ('Int64', 1),
                   ('UInt64', 2), ('Int128', 1), ('UInt128', 1), ('Int256', 1), ('UInt256', 1), ('String', 8),
                   ('FixedString', 4), ('Array', 8))
LOW_CARD_PERC = 0.4
NULLABLE_PERC = 0.3
FIXED_STR_RANGE = 256

total_weight = sum(x[1] for x in SUPPORTED_TYPES)
weights = [x[1] / total_weight for x in SUPPORTED_TYPES]
types = [x[0] for x in SUPPORTED_TYPES]
random.seed()


def random_type(low_card_perc: float = LOW_CARD_PERC, nullable_perc: float = NULLABLE_PERC):
    base_type = random.choices(types, weights)[0]
    if base_type in ('Nested', 'Array', 'Map'):
        return base_type
    if base_type == 'FixedString':
        base_type = f'{base_type}({random.randint(1, FIXED_STR_RANGE)})'
    if random.random() < nullable_perc:
        base_type = f'Nullable({base_type})'
    if 'String' in base_type and random.random() < low_card_perc:
        base_type = f'LowCardinality({base_type})'
    return base_type


def random_columns(cnt: int = 16, col_prefix: str = 'col'):
    col_names = []
    col_types = []
    for y in range(cnt):
        base_type = random_type()
        col_type = base_type
        if base_type == 'Array':
            depth = 0
            while True:
                depth += 1
                element = random_type()
                if depth > 3 and element == 'Array':  # Three levels of array nesting should be enough to test
                    continue
                col_type = f'{col_type}({element}'
                if element != 'Array':
                    col_type += ')' * depth
                    break
        col_name = f'{col_prefix}{y}_{base_type.lower()}'.replace('(', '_').replace(')', '_')
        col_names.append(col_name)
        col_types.append(get_from_name(col_type))
    return tuple(col_names), tuple(col_types)


def random_data(col_types: Sequence[ClickHouseType], num_rows: int = 1):
    data = [tuple(random_col_data(col_type, num_rows)) for col_type in col_types]
    all_cols = [list(range(num_rows))]
    all_cols.extend(data)
    return list(zip(*all_cols))


def to_bytes(hex_str:str ):
    return memoryview(bytes.fromhex(hex_str))


def to_hex(b: bytes):
    lines = [(b[ix: ix + 16].hex(' ', -2)) for ix in range(0, len(b), 16)]
    return '\n'.join(lines)


def add_test_entries():
    dist = pkg_resources.Distribution('clickhouse-connect')
    ep1 = pkg_resources.EntryPoint.parse(
        'clickhousedb.connect = clickhouse_connect.cc_sqlalchemy.dialect:ClickHouseDialect', dist=dist)
    ep2 = pkg_resources.EntryPoint.parse(
        'clickhousedb = clickhouse_connect.cc_sqlalchemy.dialect:ClickHouseDialect', dist=dist)
    entry_map = dist.get_entry_map()
    entry_map['sqlalchemy.dialects'] = {'clickhousedb.connect': ep1, 'clickhousedb': ep2}
    pkg_resources.working_set.add(dist)
    print('test eps added to distribution')
