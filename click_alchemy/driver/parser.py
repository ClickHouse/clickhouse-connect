import logging

from typing import List, Callable, Any

from click_alchemy.driver import type_map, ClickHouseType

logger = logging.getLogger(__name__)


def parse_leb128(source: bytearray, start: int):
    length = 0
    ix = 0
    while True:
        b = source[start + ix]
        length = length + ((b & 0x7f) << (ix * 7))
        if (b & 0x80) == 0:
            break
    return ix + 1, length


def leb128_string(source: bytearray, start: int, encoding: str = 'utf8'):
    (sb, l) = parse_leb128(source, start)
    return source[start + sb:start + sb + l].decode(encoding), start + sb + l


def parse_row(source: bytearray, start: int, conversions: List[Callable]):
    loc = start
    row = []
    for conv in conversions:
        v, loc = conv(source, loc)
        row.append(v)
    return row, loc


def parse_response(source: bytearray) -> (List[List[Any]], List[str], List[ClickHouseType]):
    response_size = len(source)
    loc = 0
    num_columns, loc = parse_leb128(source, loc)
    logger.debug("Processing response, num columns = %d", num_columns)
    names = []
    for _ in range(num_columns):
        name, loc = leb128_string(source, loc)
        names.append(name)
    logger.debug("Processing response, column names = %s", ','.join(names))
    col_types = []
    for _ in range(num_columns):
        col_type, loc = leb128_string(source, loc)
        try:
            col_types.append(type_map[col_type])
        except KeyError as ke:
            logger.error("Unknown type returned", ke)
            raise
    logger.debug("Processing response, column types = %s", ','.join([t.name for t in col_types]))
    convs = [t.to_python for t in col_types]
    result = []
    while loc < response_size:
        row, loc = parse_row(source, loc, convs)
        result.append(row)
    return result, names, col_types
