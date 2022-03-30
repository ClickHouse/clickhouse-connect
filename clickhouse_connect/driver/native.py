from collections.abc import Sequence
from typing import Union, List, Any, Collection

from clickhouse_connect.datatypes import registry
from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.driver.common import read_leb128, read_leb128_str, write_leb128


def parse_response(source: Union[memoryview, bytes, bytearray], use_none: bool = True) -> (
        Sequence[Sequence[Any]], List[str], List[ClickHouseType]):
    if not isinstance(source, memoryview):
        source = memoryview(source)
    loc = 0
    names = []
    col_types = []
    result = []
    total_size = len(source)
    block = 0
    while loc < total_size:
        num_cols, loc = read_leb128(source, loc)
        num_rows, loc = read_leb128(source, loc)
        for col_num in range(num_cols):
            name, loc = read_leb128_str(source, loc)
            if block == 0:
                names.append(name)
            type_name, loc = read_leb128_str(source, loc)
            if block == 0:
                col_type = registry.get_from_name(type_name)
                col_types.append(col_type)
            else:
                col_type = col_types[col_num]
            column, loc = col_type.from_native(source, loc, num_rows, use_none=use_none)
            if block == 0:
                result.append(column)
            else:
                result[col_num] += column
        block += 1
    result = list(zip(*result))
    return result, names, col_types


def build_insert(data: Collection[Collection[Any]], *, column_names: Collection[str],
                 column_type_names: Collection[str] = None, column_types: Collection[ClickHouseType] = None):
    if not column_types:
        column_types = [registry.get_from_name(name) for name in column_type_names]
    output = bytearray()
    columns = tuple(zip(*data))
    write_leb128(len(columns), output)
    write_leb128(len(data), output)
    for col_name, col_type, column in zip(column_names, column_types, columns):
        write_leb128(len(col_name), output)
        output += col_name.encode()
        write_leb128(len(col_type.name), output)
        output += col_type.name.encode()
        col_type.to_native(column, output)
    return output
