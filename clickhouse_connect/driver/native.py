from typing import Any, Sequence

from clickhouse_connect.datatypes import registry
from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.driver.common import read_leb128, read_leb128_str, write_leb128
from clickhouse_connect.driver.query import DataResult
from clickhouse_connect.driver.transform import DataTransform, QueryContext


class NativeTransform(DataTransform):
    # pylint: disable=too-many-locals
    def _transform_response(self, source: Sequence, context: QueryContext) -> DataResult:
        if not isinstance(source, memoryview):
            source = memoryview(source)
        loc = 0
        names = []
        col_types = []
        result = []
        total_size = len(source)
        block = 0
        use_none = context.use_none
        while loc < total_size:
            result_block = []
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
                context.start_column(name, col_type)
                column, loc = col_type.read_native_column(source, loc, num_rows, use_none=use_none)
                result_block.append(column)
            block += 1
            result.extend(list(zip(*result_block)))
        return DataResult(result, tuple(names), tuple(col_types))

    def build_insert(self, data: Sequence[Sequence[Any]], *, column_names: Sequence[str],
                     column_type_names: Sequence[str] = None,
                     column_types: Sequence[ClickHouseType] = None,
                     column_oriented: bool = False):
        if not column_types:
            column_types = [registry.get_from_name(name) for name in column_type_names]
        output = bytearray()
        columns = data if column_oriented else tuple(zip(*data))
        write_leb128(len(columns), output)
        write_leb128(len(columns[0]), output)
        for col_name, col_type, column in zip(column_names, column_types, columns):
            write_leb128(len(col_name), output)
            output += col_name.encode()
            write_leb128(len(col_type.name), output)
            output += col_type.name.encode()
            col_type.write_native_column(column, output)
        return output
