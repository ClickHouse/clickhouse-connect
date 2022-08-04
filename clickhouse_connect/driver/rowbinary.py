import logging

from typing import Any, Sequence

from clickhouse_connect.datatypes import registry
from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.driver.common import read_leb128, read_leb128_str
from clickhouse_connect.driver.exceptions import InterfaceError
from clickhouse_connect.driver.query import DataResult
from clickhouse_connect.driver.transform import DataTransform, QueryContext

logger = logging.getLogger(__name__)


class RowBinaryTransform(DataTransform):

    def _transform_response(self, source: Sequence, context: QueryContext) -> DataResult:
        if not isinstance(source, memoryview):
            source = memoryview(source)
        response_size = len(source)
        loc = 0
        num_columns, loc = read_leb128(source, loc)
        names = []
        for _ in range(num_columns):
            name, loc = read_leb128_str(source, loc)
            names.append(name)
        col_types = []
        for _ in range(num_columns):
            col_type, loc = read_leb128_str(source, loc)
            try:
                col_types.append(registry.get_from_name(col_type))
            except KeyError:
                raise InterfaceError(f'Unknown ClickHouse type returned for type {col_type}') from None
        convs = tuple(t.from_row_binary for t in col_types)
        result = []
        while loc < response_size:
            row = []
            for conv in convs:
                v, loc = conv(source, loc)
                row.append(v)
            result.append(row)
        return DataResult(result, tuple(names), tuple(col_types))

    def build_insert(self, data: Sequence[Sequence[Any]], *, column_type_names: Sequence[str] = None,
                     column_types: Sequence[ClickHouseType] = None, column_oriented: bool = False, **_):
        if not column_types:
            column_types = [registry.get_from_name(name) for name in column_type_names]
        convs = tuple(t.to_row_binary for t in column_types)
        if column_oriented:
            data = tuple(zip(*data))
        output = bytearray()
        for row in data:
            for (value, conv) in zip(row, convs):
                conv(value, output)
        return output
