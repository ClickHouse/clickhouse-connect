from typing import Union, Sequence, MutableSequence, Collection, List
from uuid import UUID as PYUUID

from clickhouse_connect.datatypes import registry
from clickhouse_connect.datatypes.base import TypeDef, ClickHouseType, ArrayType, UnsupportedType
from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver.ctypes import data_conv
from clickhouse_connect.driver.errors import handle_error
from clickhouse_connect.driver.exceptions import DataError
from clickhouse_connect.driver.insert import InsertContext
from clickhouse_connect.driver.query import QueryContext
from clickhouse_connect.driver.types import ByteSource
from clickhouse_connect.json_impl import any_to_json

empty_uuid_b = bytes(b'\x00' * 16)


class UUID(ClickHouseType):
    valid_formats = 'string', 'native'
    np_type = 'U36'
    byte_size = 16

    def python_null(self, ctx):
        return '' if self.read_format(ctx) == 'string' else PYUUID(int=0)

    def _read_column_binary(self, source: ByteSource, num_rows: int, ctx: QueryContext):
        if self.read_format(ctx) == 'string':
            return self._read_binary_str(source, num_rows)
        return data_conv.read_uuid_col(source, num_rows)

    @staticmethod
    def _read_binary_str(source: ByteSource, num_rows: int):
        v = source.read_array('Q', num_rows * 2)
        column = []
        app = column.append
        for i in range(num_rows):
            ix = i << 1
            x = f'{(v[ix] << 64 | v[ix + 1]):032x}'
            app(f'{x[:8]}-{x[8:12]}-{x[12:16]}-{x[16:20]}-{x[20:]}')
        return column

    # pylint: disable=too-many-branches
    def _write_column_binary(self, column: Union[Sequence, MutableSequence], dest: bytearray, ctx: InsertContext):
        first = self._first_value(column)
        empty = empty_uuid_b
        if isinstance(first, str) or self.write_format(ctx) == 'string':
            for v in column:
                if v:
                    x = int(v.replace('-', ''), 16)
                    dest += (x >> 64).to_bytes(8, 'little') + (x & 0xffffffffffffffff).to_bytes(8, 'little')
                else:
                    dest += empty
        elif isinstance(first, int):
            for x in column:
                if x:
                    dest += (x >> 64).to_bytes(8, 'little') + (x & 0xffffffffffffffff).to_bytes(8, 'little')
                else:
                    dest += empty
        elif isinstance(first, PYUUID):
            for v in column:
                if v:
                    x = v.int
                    dest += (x >> 64).to_bytes(8, 'little') + (x & 0xffffffffffffffff).to_bytes(8, 'little')
                else:
                    dest += empty
        elif isinstance(first, (bytes, bytearray, memoryview)):
            for v in column:
                if v:
                    dest += bytes(reversed(v[:8])) + bytes(reversed(v[8:]))
                else:
                    dest += empty
        else:
            dest += empty * len(column)


class Nothing(ArrayType):
    _array_type = 'b'

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.nullable = True

    def _write_column_binary(self, column: Union[Sequence, MutableSequence], dest: bytearray, _ctx):
        dest += bytes(0x30 for _ in range(len(column)))


class SimpleAggregateFunction(ClickHouseType):
    _slots = ('element_type',)

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.element_type: ClickHouseType = get_from_name(type_def.values[1])
        self._name_suffix = type_def.arg_str
        self.byte_size = self.element_type.byte_size

    def _data_size(self, sample: Sequence) -> int:
        return self.element_type.data_size(sample)

    def read_column_prefix(self, source: ByteSource, ctx: QueryContext):
        return self.element_type.read_column_prefix(source, ctx)

    def write_column_prefix(self, dest: bytearray):
        self.element_type.write_column_prefix(dest)

    def _read_column_binary(self, source: ByteSource, num_rows: int, ctx: QueryContext):
        return self.element_type.read_column_data(source, num_rows, ctx)

    def _write_column_binary(self, column: Union[Sequence, MutableSequence], dest: bytearray, ctx: InsertContext):
        self.element_type.write_column_data(column, dest, ctx)


class AggregateFunction(UnsupportedType):
    pass


def json_sample_size(_, sample: Collection) -> int:
    if len(sample) == 0:
        return 0
    total = 0
    for x in sample:
        if isinstance(x, str):
            total += len(x)
        elif x:
            total += len(any_to_json(x))
    return total // len(sample) + 1

def write_json(self, column: Sequence, dest: bytearray, ctx: InsertContext):
    first = self._first_value(column)
    write_col = column
    encoding = ctx.encoding or self.encoding
    if not isinstance(first, str) and self.write_format(ctx) != 'string':
        to_json = any_to_json
        write_col = [to_json(v) for v in column]
        encoding = None
    handle_error(data_conv.write_str_col(write_col, self.nullable, encoding, dest))


class JSON(ClickHouseType):
    valid_formats = 'string', 'native'
    _data_size = json_sample_size
    write_column_data = write_json

    def read_column_prefix(self, source: ByteSource, ctx: QueryContext):
        if source.read_uint64() != 0: # object serialization version, currently only 0 is recognized
            raise DataError('unrecognized object serialization version')
        source.read_leb128() # the max number of dynamic paths.  Used to preallocate storage in ClickHouse, we ignore it
        dynamic_path_cnt = source.read_leb128()
        ctx.read_state['dynamic_paths'] = [source.read_leb128_str() for _ in range(dynamic_path_cnt)]
        ctx.read_state['dynamic_variants'] = [read_dynamic_prefix(source) for _ in range(dynamic_path_cnt)]

    def _read_column_binary(self, source: ByteSource, num_rows: int, ctx: QueryContext):
        dynamic_paths = ctx.read_state.pop('dynamic_paths')
        sub_columns:List[Sequence] = [[]] * len(dynamic_paths)
        for ix, _ in enumerate(dynamic_paths):
            variant_types = ctx.read_state['dynamic_variants'][ix]
            sub_columns[ix] = read_variant_column(variant_types, source, num_rows, ctx)
        shared_type = registry.get_from_name('Array(Tuple(String, String))')
        shared_type.read_column(source, num_rows, ctx)

        col = [{}] * num_rows
        for row_num in range(num_rows):
            item = {}
            for ix, field in enumerate(dynamic_paths):
                item[field] = sub_columns[ix][row_num]
            col[row_num] = item
        return col


def read_variant_column(variant_types: List[ClickHouseType], source: ByteSource, num_rows:int, ctx: QueryContext) -> Sequence:
    v_count = len(variant_types)
    discriminators = source.read_array('B', num_rows)
    # Currently we have to figure out how many of each discriminator there are in the block to read
    # the sub columns correctly
    disc_rows = [0] * v_count
    for disc in discriminators:
        if disc != 255:
            disc_rows[disc] += 1
    sub_columns: List[Sequence] = [[]] * v_count
    # Read all the sub-columns
    for ix in range(v_count):
        if disc_rows[ix] > 0:
            sub_columns[ix] = variant_types[ix].read_column_data(source, disc_rows[ix], ctx)
    # Now we have to walk through each of the discriminators again to assign the correct value from
    # the sub-column to the final result column
    sub_indexes = [0] * v_count
    col = []
    app_col = col.append
    for disc in discriminators:
        if disc == 255:
            app_col(None)
        else:
            app_col(sub_columns[disc][sub_indexes[disc]])
            sub_indexes[disc] += 1
    return col


class Variant(ClickHouseType):
    _slots = 'element_types'
    python_type = object

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.element_types:List[ClickHouseType] = [get_from_name(name) for name in type_def.values]
        self._name_suffix = f"({', '.join(ch_type.name for ch_type in self.element_types)})"

    def read_column_prefix(self, source: ByteSource, ctx:QueryContext):
        if source.read_uint64() != 0:
            raise DataError(f'Unexpected discriminator format in Variant column')

    def _read_column_binary(self, source: ByteSource, num_rows: int, ctx: QueryContext) -> Sequence:
        return read_variant_column(self.element_types, source, num_rows, ctx)


def read_dynamic_prefix(source: ByteSource) -> List[ClickHouseType]:
    dynamic_struct_ver = source.read_uint64()
    if dynamic_struct_ver  != 1:  # dynamic structure serialization version, currently only 1 is recognized
        raise DataError('unrecognized dynamic structure version')
    source.read_leb128()  # max dynamic types, we ignore this value
    num_variants = source.read_leb128()
    variant_types = [get_from_name(source.read_leb128_str()) for _ in range(num_variants)]
    variant_types.append(get_from_name('String'))
    if source.read_uint64() != 0:
        raise DataError(f'Unexpected discriminator format in Variant column prefix')
    return variant_types


class Dynamic(ClickHouseType):
    python_type = object

    def read_column_prefix(self, source: ByteSource, ctx:QueryContext):
        ctx.read_state['variant_types'] = read_dynamic_prefix(source)

    def _read_column_binary(self, source: ByteSource, num_rows: int, ctx: QueryContext) -> Sequence:
        return read_variant_column(ctx.read_state.pop('variant_types'), source, num_rows, ctx)


class Object(ClickHouseType):
    python_type = dict
    # Native is a Python type (primitive, dict, array), string is an actual JSON string
    valid_formats = 'string', 'native'
    _data_size = json_sample_size
    write_column_data = write_json

    def __init__(self, type_def):
        data_type = type_def.values[0].lower().replace(' ', '')
        if data_type not in ("'json'", "nullable('json')"):
            raise NotImplementedError('Only json or Nullable(json) Object type is currently supported')
        super().__init__(type_def)
        self._name_suffix = type_def.arg_str

    def write_column_prefix(self, dest: bytearray):
        dest.append(0x01)
