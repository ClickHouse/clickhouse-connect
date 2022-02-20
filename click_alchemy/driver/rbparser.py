import logging

from typing import List, Callable, Any

from click_alchemy.types import registry
from click_alchemy.driver.rowbinary import parse_leb128, leb128_string

logger = logging.getLogger(__name__)


def parse_row(source: bytes, start: int, conversions: List[Callable]):
    loc = start
    row = []
    for conv in conversions:
        v, loc = conv(source, loc)
        row.append(v)
    return row, loc


def parse_response(source: bytes) -> (List[List[Any]], List[str], List[Any]):
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
            col_types.append(registry.get(col_type))
        except KeyError as ke:
            logger.error("Unknown type returned", ke)
            raise
    logger.debug("Processing response, column types = %s", ','.join([t.name for t in col_types]))
    convs = [t.from_row_binary for t in col_types]
    result = []
    while loc < response_size:
        row, loc = parse_row(source, loc, convs)
        result.append(row)
    return result, names, col_types
