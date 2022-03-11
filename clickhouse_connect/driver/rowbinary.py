import logging
from collections import deque

from typing import Iterable, List, Any

from clickhouse_connect.datatypes import registry
from clickhouse_connect.datatypes.registry import ClickHouseType
from clickhouse_connect.driver.exceptions import DriverError

logger = logging.getLogger(__name__)


def parse_response(source: bytes) -> (List[List[Any]], List[str], List[ClickHouseType]):
    response_size = len(source)
    loc = 0
    num_columns, loc = read_leb128(source, loc)
    logger.debug("Processing response, num columns = %d", num_columns)
    names = []
    for _ in range(num_columns):
        name, loc = read_leb128_str(source, loc)
        names.append(name)
    logger.debug("Processing response, column names = %s", ','.join(names))
    col_types = []
    for _ in range(num_columns):
        col_type, loc = read_leb128_str(source, loc)
        try:
            col_types.append(registry.get_from_name(col_type))
        except KeyError:
            raise DriverError(f"Unknown ClickHouse type returned for type {col_type}")
    logger.debug("Processing response, column ch_types = %s", ','.join([t.name for t in col_types]))
    convs = tuple([t.from_row_binary for t in col_types])
    result = deque()
    row = deque()
    while loc < response_size:
        row.clear()
        for conv in convs:
            v, loc = conv(source, loc)
            row.append(v)
        result.append(tuple(row))
    return result, names, col_types


def build_insert(data: Iterable[Iterable[Any]], *, column_type_names: Iterable[str] = None,
                 column_types: Iterable[ClickHouseType] = None):
    if not column_types:
        column_types = [registry.get_from_name(name) for name in column_type_names]
    convs = tuple([t.to_row_binary for t in column_types])
    output = bytearray()
    for row in data:
        for (value, conv) in zip(row, convs):
            conv(value, output)
    return output


def read_leb128(source: bytes, loc: int):
    length = 0
    ix = 0
    while True:
        b = source[loc + ix]
        length = length + ((b & 0x7f) << (ix * 7))
        ix += 1
        if (b & 0x80) == 0:
            break
    return length, loc + ix


def read_leb128_str(source: bytes, loc: int, encoding: str = 'utf8'):
    length, loc = read_leb128(source, loc)
    return source[loc:loc + length].decode(encoding), loc + length


def to_leb128(value: int) -> bytearray:  #Unsigned only
    result = bytearray()
    while True:
        b = value & 0x7f
        value = value >> 7
        if value == 0:
            result.append(b)
            break
        result.append(0x80 | b)
    return result

