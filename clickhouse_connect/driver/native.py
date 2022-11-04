import zlib
from typing import Any, Sequence, Optional

from clickhouse_connect.datatypes import registry
from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.driver.common import read_leb128, read_leb128_str, write_leb128
from clickhouse_connect.driver.query import DataResult
from clickhouse_connect.driver.transform import DataTransform, QueryContext


block_size = 16384


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
                     column_oriented: bool = False, compression: Optional[str] = None):
        # pylint: disable=too-many-statements
        if not column_types:
            column_types = [registry.get_from_name(name) for name in column_type_names]
        zlib_obj = zlib.compressobj(6, zlib.DEFLATED, 31)
        if column_oriented:
            def gen():
                columns = data
                block_data = tuple(zip(column_names, column_types, columns))
                total_rows = len(columns[0])
                block_start = 0
                while True:
                    row_count = total_rows - block_start
                    if row_count < 0:
                        if compression == 'gzip':
                            yield zlib_obj.flush()
                        return
                    block_rows = min(row_count, block_size)
                    output = bytearray()
                    write_leb128(len(columns), output)
                    write_leb128(block_rows, output)
                    for col_name, col_type, column in block_data:
                        write_leb128(len(col_name), output)
                        output += col_name.encode()
                        write_leb128(len(col_type.name), output)
                        output += col_type.name.encode()
                        block_column = column[block_start:block_start + block_rows]
                        col_type.write_native_column(block_column, output)
                    if compression == 'gzip':
                        output = zlib_obj.compress(output)
                    yield output
                    block_start += block_size
        else:
            def gen():
                total_rows = len(data)
                block_start = 0
                col_count = len(column_names)
                while True:
                    row_count = total_rows - block_start
                    if row_count < 0:
                        if compression == 'gzip':
                            yield zlib_obj.flush()
                        return
                    block_rows = min(row_count, block_size)
                    output = bytearray()
                    write_leb128(col_count, output)
                    write_leb128(row_count, output)
                    source = data[block_start:block_start + block_rows]
                    columns = tuple(zip(*source))
                    for ix in range(col_count):
                        col_name, col_type, block_column = column_names[ix], column_types[ix], columns[ix]
                        write_leb128(len(col_name), output)
                        output += col_name.encode()
                        write_leb128(len(col_type.name), output)
                        output += col_type.name.encode()
                        col_type.write_native_column(block_column, output)
                    if compression == 'gzip':
                        output = zlib_obj.compress(output)
                    yield output
                    block_start += block_size
        return gen()
