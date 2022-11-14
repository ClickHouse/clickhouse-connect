import zlib
from typing import Sequence

from clickhouse_connect.datatypes import registry
from clickhouse_connect.driver.common import read_leb128, read_leb128_str, write_leb128
from clickhouse_connect.driver.insert import InsertContext
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
            if context.column_oriented:
                if block == 0:
                    result = [x if isinstance(x, list) else list(x) for x in result_block]
                else:
                    for base, added in zip(result, result_block):
                        base.extend(added)
            else:
                result.extend(list(zip(*result_block)))
            block += 1
        return DataResult(result, tuple(names), tuple(col_types), context.column_oriented)

    def _build_insert(self, context: InsertContext):
        if context.compression == 'gzip':
            compressor = GzipCompressor()
        else:
            compressor = NullCompressor()

        def chunk_gen():
            for x in context.next_block():
                output = bytearray()
                write_leb128(x.column_count, output)
                write_leb128(x.row_count, output)
                for col_name, col_type, data in zip(x.column_names, x.column_types, x.column_data):
                    write_leb128(len(col_name), output)
                    output += col_name.encode()
                    write_leb128(len(col_type.name), output)
                    output += col_type.name.encode()
                    context.start_column(col_name, col_type)
                    try:
                        col_type.write_native_column(data, output)
                    except Exception as ex: # pylint: disable=broad-except
                        # This is hideous, but some low level serializations can fail while streaming
                        # the insert if the user has included bad data in the column.  We need to ensure that the
                        # insert fails (using garbage data) to avoid a partial insert, and use the context to
                        # propagate the correct exception to the user
                        context.insert_exception = ex
                        yield 'INTERNAL EXCEPTION WHILE SERIALIZING'.encode()
                        return
                yield compressor.compress_block(output)
            footer = compressor.complete()
            if footer:
                yield footer

        return chunk_gen()


class NullCompressor:
    @staticmethod
    def compress_block(block):
        return block

    def complete(self):
        pass


class GzipCompressor:
    def __init__(self, level: int = 6, wbits: int = 31):
        self.zlib_obj = zlib.compressobj(level=level, wbits=wbits)

    def compress_block(self, block):
        return self.zlib_obj.compress(block)

    def complete(self):
        return self.zlib_obj.flush()
