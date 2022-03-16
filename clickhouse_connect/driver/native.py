import array
import sys
from typing import Union, List, Any, Iterable

from clickhouse_connect.datatypes import registry
from clickhouse_connect.datatypes.registry import ClickHouseType
from clickhouse_connect.driver import DriverError
from clickhouse_connect.driver.rowbinary import read_leb128, read_leb128_str

must_swap = sys.byteorder == 'big'


def parse_response(source: Union[memoryview, bytes, bytearray]):
    result, names, types = parse_raw(source)
    for ix, type in enumerate(types):
        if type.to_python:
            result[ix] = type.to_python(result[ix])
    result = tuple(zip(*result))
    return result, names, types


def parse_raw(source: Union[memoryview, bytes, bytearray]) -> (Iterable[Iterable[Any]], List[str], List[ClickHouseType]):
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
        if col_type.nullable:
            null_map = memoryview(source[loc: loc + num_rows])
            loc += num_rows
        else:
            null_map = None
        column, loc = col_type.from_native(source, loc, num_rows, must_swap)
        if null_map:
            column = tuple((None if null_map[ix] else column[ix] for ix in range(num_rows)))
        result.append(column)
    return result, names, col_types


