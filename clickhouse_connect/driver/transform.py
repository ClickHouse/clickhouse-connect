import zlib

from clickhouse_connect.datatypes import registry
from clickhouse_connect.driver.common import write_leb128
from clickhouse_connect.driver.insert import InsertContext
from clickhouse_connect.driver.query import QueryResult, QueryContext
from clickhouse_connect.driver.types import ByteSource

_EMPTY_QUERY_CONTEXT = QueryContext()


class NativeTransform:
    # pylint: disable=too-many-locals
    @staticmethod
    def parse_response(source: ByteSource, context: QueryContext = _EMPTY_QUERY_CONTEXT) -> QueryResult:
        with context:
            names = []
            col_types = []
            use_none = context.use_none
            block_num = 0

            def get_block():
                nonlocal block_num
                result_block = []
                try:
                    num_cols = source.read_leb128()
                    num_rows = source.read_leb128()
                except (StopIteration, IndexError):
                    source.close()
                    return None
                for col_num in range(num_cols):
                    name = source.read_leb128_str()
                    type_name = source.read_leb128_str()
                    if block_num == 0:
                        names.append(name)
                        col_type = registry.get_from_name(type_name)
                        col_types.append(col_type)
                    else:
                        col_type = col_types[col_num]
                    context.start_column(name, col_type)
                    result_block.append(col_type.read_native_column(source, num_rows, use_none=use_none))
                block_num += 1
                return result_block

            first_block = get_block()
            if first_block is None:
                return QueryResult([])

            def gen():
                yield first_block
                while True:
                    next_block = get_block()
                    if next_block is None:
                        return
                    yield next_block

        return QueryResult(None, gen(), tuple(names), tuple(col_types), context.column_oriented, source)

    @staticmethod
    def build_insert(context: InsertContext):
        if context.compression == 'gzip':
            compressor = _GZIP_COMPRESSOR
        else:
            compressor = _NULL_COMPRESSOR

        with context:

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
                        except Exception as ex:  # pylint: disable=broad-except
                            # This is hideous, but some low level serializations can fail while streaming
                            # the insert if the user has included bad data in the column.  We need to ensure that the
                            # insert fails (using garbage data) to avoid a partial insert, and use the context to
                            # propagate the correct exception to the user
                            context.insert_exception = ex
                            yield 'INTERNAL EXCEPTION WHILE SERIALIZING'.encode()
                            return
                    yield compressor.compress_block(output)
                footer = compressor.flush()
                if footer:
                    yield footer

        return chunk_gen()


class NullCompressor:
    @staticmethod
    def compress_block(block):
        return block

    def flush(self):
        pass


class GzipCompressor:
    def __init__(self, level: int = 6, wbits: int = 31):
        self.zlib_obj = zlib.compressobj(level=level, wbits=wbits)

    def compress_block(self, block):
        return self.zlib_obj.compress(block)

    def flush(self):
        return self.zlib_obj.flush()


_NULL_COMPRESSOR = NullCompressor()
_GZIP_COMPRESSOR = GzipCompressor()
