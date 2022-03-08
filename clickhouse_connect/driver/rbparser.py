import logging

from typing import List, Callable, Any

from clickhouse_connect.datatypes import registry
from clickhouse_connect.datatypes.registry import ClickHouseType
from clickhouse_connect.driver.exceptions import DriverError
from clickhouse_connect.driver.rowbinary import read_leb128, string_leb128

logger = logging.getLogger(__name__)


def parse_row(source: bytes, start: int, conversions: List[Callable]):
    loc = start
    row = []
    for conv in conversions:
        v, loc = conv(source, loc)
        row.append(v)
    return row, loc


def parse_response(source: bytes) -> (List[List[Any]], List[str], List[ClickHouseType]):
    response_size = len(source)
    loc = 0
    num_columns, loc = read_leb128(source, loc)
    logger.debug("Processing response, num columns = %d", num_columns)
    names = []
    for _ in range(num_columns):
        name, loc = string_leb128(source, loc)
        names.append(name)
    logger.debug("Processing response, column names = %s", ','.join(names))
    col_types = []
    for _ in range(num_columns):
        col_type, loc = string_leb128(source, loc)
        try:
            col_types.append(registry.get_from_name(col_type))
        except KeyError as ke:
            raise DriverError(f"Unknown ClickHouse type returned for type {col_type}")
    logger.debug("Processing response, column chtypes = %s", ','.join([t.name for t in col_types]))
    convs = [t.from_row_binary for t in col_types]
    result = []
    while loc < response_size:
        row, loc = parse_row(source, loc, convs)
        result.append(row)
    return result, names, col_types


def build_insert(data: List[List[Any]], *, column_type_names: List[str] = None,
                 column_ch_types: List[ClickHouseType] = None):
    if not column_ch_types:
        column_ch_types = [registry.get_from_name(name) for name in column_type_names]
    convs = [t.to_row_binary for t in column_ch_types]
    output = bytearray()
    for row in data:
        for (value, conv) in zip(row, convs):
            conv(value, output)
    return output
