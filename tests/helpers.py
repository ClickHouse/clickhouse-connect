import random
import re
from typing import Sequence, Union, Type

import math
import pytz

from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.tools.datagen import random_col_data, random_ascii_str, RandomValueDef
from clickhouse_connect.driver.insert import InsertContext
from clickhouse_connect.driver.transform import NativeTransform
from clickhouse_connect.driverc.buffer import ResponseBuffer  # pylint: disable=no-name-in-module

LOW_CARD_PERC = 0.4
NULLABLE_PERC = 0.2
TUPLE_MAX = 5
FIXED_STR_RANGE = 256
ENUM_VALUES = 5
NESTED_DEPTH = 2

random.seed()
weighted_types = (('Int8', 1), ('UInt8', 1), ('Int16', 1), ('UInt16', 1), ('Int32', 1), ('UInt32', 1), ('Int64', 1),
                  ('UInt64', 2), ('Int128', 1), ('UInt128', 1), ('Int256', 1), ('UInt256', 1), ('String', 8),
                  ('FixedString', 4), ('Float32', 2), ('Float64', 2), ('Enum8', 2), ('Decimal', 4), ('Enum16', 2),
                  ('Bool', 1), ('UUID', 2), ('Date', 2), ('Date32', 1), ('DateTime', 4), ('DateTime64', 2), ('IPv4', 2),
                  ('IPv6', 2), ('Array', 16), ('Tuple', 10), ('Map', 10), ('Nested', 4))
all_types, all_weights = tuple(zip(*weighted_types))
nested_types = ('Array', 'Tuple', 'Map', 'Nested')
terminal_types = set(all_types) - set(nested_types)
total_weight = sum(all_weights)
all_weights = [x / total_weight for x in all_weights]
unsupported_types = set()
native_transform = NativeTransform()


def random_type(depth: int = 0, low_card_perc: float = LOW_CARD_PERC,
                nullable_perc: float = NULLABLE_PERC, parent_type: str = None):
    base_type = random.choices(all_types, all_weights)[0]
    low_card_ok = True
    while (base_type in unsupported_types
           or (depth >= NESTED_DEPTH and base_type in nested_types)
           or parent_type == 'Nested' and base_type in ('Int128', 'Int256', 'UInt256', 'UInt126')):
        base_type = random.choices(all_types, all_weights)[0]
    if base_type in terminal_types:
        if base_type == 'FixedString':
            base_type = f'{base_type}({random.randint(1, FIXED_STR_RANGE)})'
        if base_type == 'DateTime64':
            base_type = f'{base_type}({random.randint(0, 3) * 3})'
            low_card_ok = False
        if base_type == 'Decimal':
            prec = int(random.random() * 76) + 1
            scale = int(random.random() * prec)
            base_type = f'Decimal({prec}, {scale})'
            low_card_ok = False
        if base_type.startswith('Enum'):
            sz = 8 if base_type == 'Enum8' else 16
            keys = set()
            values = set()
            base_type += '('
            for _ in range(int(random.random() * ENUM_VALUES) + 1):
                while True:
                    key = random_ascii_str(8, 2)
                    key = re.sub(r"[\\\')(]", '_', key)
                    value = int(random.random() * 2 ** sz) - 2 ** (sz - 1)
                    if key not in keys and value not in values:
                        break
                keys.add(key)
                values.add(value)
                base_type += f"'{key}' = {value},"
            base_type = base_type[:-1] + ')'
            low_card_ok = False
        if 'Int256' in base_type or 'Int128' in base_type:
            low_card_ok = False
        if random.random() < nullable_perc:
            base_type = f'Nullable({base_type})'
        if low_card_ok and random.random() < low_card_perc:
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
    if base_type == 'Map':
        key = random_type(1000, nullable_perc=0)
        while key.python_type not in (str, int) or (key.python_type == int and key.low_card):
            key = random_type(1000, nullable_perc=0)
        value = random_type(depth + 1)
        while value.python_type not in (int, str, list):
            value = random_type(depth + 1)
        return get_from_name(f'Map({key.name}, {value.name})')
    if base_type == 'Nested':
        elements = [random_type(depth + 1, parent_type='Nested') for _ in range(random.randint(1, TUPLE_MAX))]
        cols = [f'key_{ix} {element.name}' for ix, element in enumerate(elements)]
        return get_from_name(f"Nested({', '.join(cols)})")
    raise ValueError(f'Unrecognized nested type {base_type}')


def random_columns(cnt: int = 16, col_prefix: str = 'col'):
    col_names = []
    col_types = []
    for x in range(cnt):
        col_type = random_type()
        col_types.append(col_type)
        short_name = col_type.name.lower()
        ix = short_name.find('enum')
        if ix > -1:
            short_name = short_name[:ix + 5]
        short_name = re.sub(r'(,|\s+|\(+|\)+)', '_', short_name)
        short_name = re.sub(r'_+', '_', short_name)
        short_name = short_name.replace('nullable', 'n').replace('lowcardinality', 'lc')
        col_names.append(f'{col_prefix}{x}_{short_name[:24]}')
        x += 1
    return tuple(col_names), tuple(col_types)


def random_data(col_types: Sequence[ClickHouseType],
                num_rows: int = 1,
                server_tz: pytz.tzinfo = pytz.UTC):
    col_def = RandomValueDef(server_tz)
    data = [tuple(random_col_data(col_type, num_rows, col_def)) for col_type in col_types]
    all_cols = [list(range(num_rows))]
    all_cols.extend(data)
    return list(zip(*all_cols))


def to_bytes(hex_str: str):
    return memoryview(bytes.fromhex(hex_str))


def to_hex(b: bytes):
    lines = [(b[ix: ix + 16].hex(' ', -2)) for ix in range(0, len(b), 16)]
    return '\n'.join(lines)


def native_insert_block(data, column_names, column_types):
    context = InsertContext('table', column_names, column_types, data)
    context.current_block = 1
    output = bytearray()
    for chunk in native_transform.build_insert(context):
        output.extend(chunk)
    return output


def list_equal(a: Sequence, b: Sequence) -> bool:
    for x, y in zip(a, b):
        if x is y:
            continue
        if isinstance(x, (list, tuple)):
            if list_equal(x, y):
                continue
            return False
        if x == y:
            continue
        if isinstance(x, float):
            if math.isnan(x) and math.isnan(y):
                continue
            if math.isclose(x, y):
                continue
        return False
    return True


def random_query(row_count: int = 10000, col_count: int = 10, date32: bool = True):
    columns = []
    max_cols = random.randint(1, col_count)
    while len(columns) < max_cols:
        col_type = random_type(NESTED_DEPTH, low_card_perc=0, nullable_perc=0).name
        if 'Enum' in col_type or 'IP' in col_type or (not date32 and 'Date32' in col_type):
            continue
        columns.append(f'col_{chr(len(columns) + 97)} {col_type}')
    columns = ', '.join(columns)
    return f"SELECT * FROM generateRandom('{columns}') LIMIT {row_count}"


def bytes_source(data: Union[str, bytes], chunk_size: int = 256, cls: Type = ResponseBuffer):
    if isinstance(data, str):
        data = bytes.fromhex(data)

    def gen():
        end = 0
        for _ in range(len(data) // chunk_size):
            yield data[end:end + chunk_size]
            end += chunk_size
        if end < len(data):
            yield data[end:]

    class TestSource:
        def __init__(self):
            self.gen = gen()

        def close(self, ex: Exception = None):
            pass

    return cls(TestSource())
