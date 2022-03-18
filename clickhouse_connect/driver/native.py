import sys
from typing import Union, List, Any, Iterable, Collection

from clickhouse_connect.datatypes import registry
from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.driver.common import read_leb128, read_leb128_str


def parse_response(source: Union[memoryview, bytes, bytearray]) -> (Collection[Collection[Any]], List[str], List[ClickHouseType]):
    if not isinstance(source, memoryview):
        source = memoryview(source)
    loc = 0
    num_cols, loc = read_leb128(source, loc)
    num_rows, loc = read_leb128(source, loc)
    names = []
    col_types: List[ClickHouseType] = []
    result = []
    for _ in range(num_cols):
        name, loc = read_leb128_str(source, loc)
        names.append(name)
        type_name, loc = read_leb128_str(source, loc)
        col_type = registry.get_from_name(type_name)
        col_types.append(col_type)
        column, loc = col_type.from_native(source, loc, num_rows)
        result.append(column)
    result = tuple(zip(*result))
    return result, names, col_types


def build_insert(data: Collection[Collection[Any]], *, column_type_names: Collection[str] = None,
                 column_types: Collection[ClickHouseType] = None):
    if not column_types:
        column_types = [registry.get_from_name(name) for name in column_type_names]
    output = bytearray()
    for row in data:
        for (value, conv) in zip(row, convs):
            conv(value, output)
    return output


