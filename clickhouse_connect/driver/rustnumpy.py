"""Numpy/pandas column converters for the rust native codec.

The rust Arrow export is raw: Date is uint16 days, DateTime is uint32 seconds with the timezone dropped,
Enum is raw ints. A naive to_pandas() therefore yields wrong dtypes. These converters are resolved once
per query from the driver's own ClickHouseType (np_type, tzinfo, nullability) so the produced columns match
the Python codec by construction. Primitive numerics and Booleans use typed buffers, including nullable
integer and float columns in extended pandas output. BFloat16 widens its buffer words to float32, and
Intervals expose signed counts through buffers. Temporal columns take the Arrow exit. Time and Time64 keep
their declared duration units through NumPy and extended pandas output. Extended pandas output builds String
columns from the Arrow buffers. Other strings,
enums, and remaining nullable columns take the rust python-object exit and are finalized through the driver's own _finalize_column.
"""

import logging
import sys
from collections.abc import Callable, Sequence
from typing import Any, cast

from clickhouse_connect.datatypes import dynamic as dynamic_module
from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.datatypes.container import Array, Map, Nested, Tuple
from clickhouse_connect.datatypes.numeric import BFloat16, Interval
from clickhouse_connect.datatypes.special import SimpleAggregateFunction
from clickhouse_connect.datatypes.string import String
from clickhouse_connect.datatypes.temporal import Date, DateTime, DateTime64, DateTimeBase, Time, Time64
from clickhouse_connect.driver import options
from clickhouse_connect.driver.common import first_value
from clickhouse_connect.driver.exceptions import NotSupportedError
from clickhouse_connect.driver.query import QueryContext

logger: logging.Logger = logging.getLogger(__name__)

BlockConverter = Callable[[Any, Any, int], Any]

_TIME64_UNITS = {0: "s", 3: "ms", 6: "us", 9: "ns"}
_INTERVAL_ARROW_UNITS = {"IntervalSecond": "s", "IntervalMillisecond": "ms", "IntervalMicrosecond": "us", "IntervalNanosecond": "ns"}
_NUMERIC_BUFFER_TYPES = frozenset({"Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64", "Float32", "Float64"})
_NUMERIC_BUFFER_KINDS = frozenset(name.lower() for name in _NUMERIC_BUFFER_TYPES)
_COMPOUND_JSON_BINARY_TYPE_INDEXES = frozenset({0x1E, 0x1F, 0x20, 0x23, 0x26, 0x27, 0x2B, 0x30})


def _time64_unit(ch_type: Time64) -> str:
    _ = ch_type.np_type
    return _TIME64_UNITS[ch_type.scale]


def _contains_json_type(ch_type: object) -> bool:
    """Return whether a ClickHouse type contains JSON through a supported container."""
    if not isinstance(ch_type, ClickHouseType):
        return False
    if ch_type.base_type == "JSON":
        return True
    for attr in ("element_type", "key_type", "value_type"):
        if _contains_json_type(getattr(ch_type, attr, None)):
            return True
    return any(_contains_json_type(child) for child in getattr(ch_type, "element_types", ()))


def _decode_json_tree(value: Any, context: QueryContext) -> Any:
    """Decode raw shared-data cells inside one materialized JSON object."""
    if isinstance(value, (bytes, bytearray, memoryview)):
        binary_value = bytes(value)
        if not binary_value or binary_value[0] not in _COMPOUND_JSON_BINARY_TYPE_INDEXES:
            return value
        decoded = dynamic_module.decode_shared_data_value(binary_value, context)
        return value if decoded == binary_value else decoded
    if isinstance(value, dict):
        return {key: _decode_json_tree(item, context) for key, item in value.items()}
    if isinstance(value, list):
        return [_decode_json_tree(item, context) for item in value]
    if isinstance(value, tuple):
        return tuple(_decode_json_tree(item, context) for item in value)
    return value


def _variant_materialized_type(ch_type: ClickHouseType) -> type | None:
    if ch_type.base_type == "JSON" or isinstance(ch_type, Map):
        return dict
    if isinstance(ch_type, Nested):
        return list
    if isinstance(ch_type, Tuple) and ch_type.element_names:
        return dict
    return ch_type.python_type


def _normalize_json_shared_value(ch_type: ClickHouseType, value: Any, context: QueryContext) -> Any:
    """Apply JSON shared-data decoding without changing non-JSON sibling values."""
    if value is None:
        return None
    if isinstance(ch_type, SimpleAggregateFunction):
        return _normalize_json_shared_value(ch_type.element_type, value, context)
    if ch_type.base_type == "JSON":
        return _decode_json_tree(value, context)
    if isinstance(ch_type, Array):
        return [_normalize_json_shared_value(ch_type.element_type, item, context) for item in value]
    if isinstance(ch_type, Tuple):
        if ch_type.element_names and isinstance(value, dict):
            result = dict(value)
            for name, element_type in zip(ch_type.element_names, ch_type.element_types):
                result[name] = _normalize_json_shared_value(element_type, value[name], context)
            return result
        return tuple(_normalize_json_shared_value(element_type, item, context) for element_type, item in zip(ch_type.element_types, value))
    if isinstance(ch_type, Map):
        return {
            _normalize_json_shared_value(ch_type.key_type, key, context): _normalize_json_shared_value(ch_type.value_type, item, context)
            for key, item in value.items()
        }
    if isinstance(ch_type, Nested):
        return [
            {
                name: _normalize_json_shared_value(element_type, item[name], context)
                for name, element_type in zip(ch_type.element_names, ch_type.element_types)
            }
            for item in value
        ]
    if isinstance(ch_type, dynamic_module.Variant):
        if isinstance(value, dynamic_module.TypedVariant):
            for element_type in ch_type.element_types:
                if element_type.name == value.type_name:
                    normalized = _normalize_json_shared_value(element_type, value.value, context)
                    return dynamic_module.TypedVariant(normalized, value.type_name)
            return value
        candidates = [element_type for element_type in ch_type.element_types if _variant_materialized_type(element_type) is type(value)]
        if len(candidates) == 1:
            return _normalize_json_shared_value(candidates[0], value, context)
    return value


def _normalize_json_shared_column(ch_type: ClickHouseType, column: Sequence[Any], context: QueryContext) -> Sequence[Any]:
    if not _contains_json_type(ch_type):
        return column
    return [_normalize_json_shared_value(ch_type, value, context) for value in column]


class _Converter:
    """One column's converter. ``needs_arrow`` decides whether the block Arrow table is built."""

    __slots__ = ("needs_arrow", "_convert")

    def __init__(self, needs_arrow: bool, convert: BlockConverter):
        self.needs_arrow = needs_arrow
        self._convert = convert

    def __call__(self, arrow_table: Any, col_batch: Any, index: int) -> Any:
        return self._convert(arrow_table, col_batch, index)


def _arrow_column(arrow_table: Any, index: int) -> Any:
    return arrow_table.column(index).combine_chunks()


def _numeric_convert(arrow_table: Any, _col_batch: Any, index: int) -> Any:
    return _arrow_column(arrow_table, index).to_numpy(zero_copy_only=False)


def _buffer_values(column: Any) -> Any:
    """View a primitive descriptor, or unpack its Boolean bits."""
    if column.kind == "bool_bitmap":
        packed = options.np.frombuffer(column.values, dtype="uint8")
        return options.np.unpackbits(packed, count=column.length, bitorder="little").view(options.np.bool_)
    if column.kind in _NUMERIC_BUFFER_KINDS:
        dtype = options.np.dtype(column.kind)
    elif column.kind == "bfloat16":
        dtype = options.np.dtype("uint16")
    else:
        raise NotImplementedError(f"Unsupported column buffer kind {column.kind!r}")
    if column.byteorder != sys.byteorder:
        dtype = dtype.newbyteorder()
    return options.np.frombuffer(column.values, dtype=dtype, count=column.length)


def _buffer_null_mask(column: Any) -> Any:
    """Expand validity bits into a writable Boolean null mask."""
    if column.validity is None:
        return options.np.zeros(column.length, dtype=options.np.bool_)
    packed = options.np.frombuffer(column.validity, dtype="uint8")
    mask = options.np.unpackbits(packed, count=column.length, bitorder="little").view(options.np.bool_)
    options.np.logical_not(mask, out=mask)
    return mask


def _join_chunks(chunks: list[Any], dtype: Any) -> Any:
    if not chunks:
        return options.np.empty(0, dtype=dtype)
    return chunks[0] if len(chunks) == 1 else options.np.concatenate(chunks)


def _make_numeric_buffer_convert(ch_type: ClickHouseType, nullable_alias: bool = False) -> BlockConverter:
    dtype = options.np.dtype(ch_type.np_type)
    bool_objects = options.np.array([False, True, None], dtype=object) if nullable_alias and dtype.kind == "b" else None

    def convert(_arrow_table: Any, col_batch: Any, index: int) -> Any:
        columns = col_batch.column_buffers(index)
        if columns is None:
            raise NotImplementedError(f"Unsupported column buffers for {ch_type.name}")
        chunks = []
        for column in columns:
            values = _buffer_values(column)
            if nullable_alias and column.null_count:
                null_mask = _buffer_null_mask(column)
                if bool_objects is not None:
                    values = bool_objects.take(options.np.where(null_mask, options.np.uint8(2), values.view(options.np.uint8)))
                else:
                    values = options.np.where(null_mask, options.np.nan, values)
            chunks.append(values)
        return _join_chunks(chunks, dtype)

    return convert


def _make_bfloat16_convert(as_extended_pandas: bool) -> BlockConverter:
    def convert(_arrow_table: Any, col_batch: Any, index: int) -> Any:
        columns = col_batch.column_buffers(index)
        if columns is None:
            raise NotImplementedError("Unsupported column buffers for BFloat16")
        chunks = []
        for column in columns:
            values = _buffer_values(column).astype(options.np.uint32)
            values <<= options.np.uint32(16)
            values = values.view(options.np.float32)
            if column.null_count:
                values = options.np.where(_buffer_null_mask(column), options.np.nan, values)
            chunks.append(values)
        values = _join_chunks(chunks, "float32")
        if as_extended_pandas:
            distinguish_nan = getattr(getattr(options.pd.options, "future", None), "distinguish_nan_and_na", False)
            mask = options.np.zeros(len(values), dtype="bool") if distinguish_nan else options.np.isnan(values)
            if not distinguish_nan and mask.any():
                values = options.np.where(mask, options.np.nan, values)
            return options.pd.arrays.FloatingArray(values, mask, copy=False)
        return values

    return convert


def _make_bfloat16_alias_convert(as_float: bool = False) -> BlockConverter:
    """Preserve the raw fixed-binary output of nested BFloat16 aliases."""

    def convert(_arrow_table: Any, col_batch: Any, index: int) -> Any:
        columns = col_batch.column_buffers(index)
        if columns is None:
            raise NotImplementedError("Unsupported column buffers for BFloat16")
        chunks = []
        for column in columns:
            values = options.np.frombuffer(column.values, dtype="V2", count=column.length).astype(object)
            if column.null_count:
                values[_buffer_null_mask(column)] = None
            chunks.append(values)
        values = _join_chunks(chunks, object)
        return values.astype("float64") if as_float else values

    return convert


def _make_interval_alias_convert(unit: str) -> BlockConverter:
    """Preserve duration output for nested aliases of sub-minute Intervals."""
    dtype = options.np.dtype(f"timedelta64[{unit}]")
    nat = options.np.timedelta64("NaT", unit)

    def convert(_arrow_table: Any, col_batch: Any, index: int) -> Any:
        columns = col_batch.column_buffers(index)
        if columns is None:
            raise NotImplementedError("Unsupported column buffers for Interval")
        chunks = []
        for column in columns:
            values = _buffer_values(column).astype("int64", copy=False).view(dtype)
            if column.null_count:
                values = options.np.where(_buffer_null_mask(column), nat, values)
            chunks.append(values)
        return _join_chunks(chunks, dtype)

    return convert


def _make_date_convert(as_pandas: bool) -> BlockConverter:
    def convert(arrow_table: Any, _col_batch: Any, index: int) -> Any:
        days = _arrow_column(arrow_table, index).to_numpy(zero_copy_only=False).astype("datetime64[D]")
        return days.astype("datetime64[s]") if as_pandas else days

    return convert


def _make_datetime_convert(as_pandas: bool, active_tz: Any) -> BlockConverter:
    def convert(arrow_table: Any, _col_batch: Any, index: int) -> Any:
        naive = _arrow_column(arrow_table, index).to_numpy(zero_copy_only=False).astype("datetime64[s]")
        if as_pandas and active_tz is not None:
            return options.pd.DatetimeIndex(naive, tz="UTC").tz_convert(active_tz)
        return naive

    return convert


def _make_datetime64_convert(as_pandas: bool, active_tz: Any) -> BlockConverter:
    def convert(arrow_table: Any, _col_batch: Any, index: int) -> Any:
        # Arrow timestamp[unit] -> datetime64[unit], tz metadata dropped to UTC instants.
        column = _arrow_column(arrow_table, index).to_numpy(zero_copy_only=False)
        if as_pandas and active_tz is not None:
            return options.pd.DatetimeIndex(column, tz="UTC").tz_convert(active_tz)
        return column

    return convert


def _pandas_infers_ns_timedeltas() -> bool:
    """pandas < 3 infers timedelta64[ns] for the object arrays the Python codec's nullable
    temporal path hands to the DataFrame constructor. pandas >= 3 keeps the scalar unit."""
    return int(options.pd.__version__.split(".", 1)[0]) < 3


def _make_time_convert(
    ch_type: Time | Time64,
    as_pandas: bool = False,
    use_extended_dtypes: bool = False,
) -> BlockConverter:
    unit = "s" if isinstance(ch_type, Time) else _time64_unit(ch_type)
    nullable_pandas_ns = as_pandas and not use_extended_dtypes and _pandas_infers_ns_timedeltas()

    def convert(arrow_table: Any, _col_batch: Any, index: int) -> Any:
        column = _arrow_column(arrow_table, index)
        if ch_type.nullable:
            null_count = column.null_count
            if isinstance(ch_type, Time):
                column = column.cast(options.arrow.int64())
            values = column.cast(options.arrow.duration(unit)).to_numpy(zero_copy_only=False)
            if as_pandas:
                return values.astype("timedelta64[ns]") if nullable_pandas_ns else values
            # The Python codec's query_np contract for nullable temporal columns
            # is an object array of numpy.timedelta64 scalars and None. Assign a
            # list here because direct ndarray assignment coerces the scalars to
            # datetime.timedelta at microsecond precision.
            result = list(values)
            if null_count:
                for null_index in options.np.flatnonzero(options.np.isnat(values)):
                    result[null_index] = None
            return result
        values = column.to_numpy(zero_copy_only=False)
        if isinstance(ch_type, Time64):
            # The core exports Time64 as its raw signed Int64 tick buffer because
            # Arrow time types cannot represent negative or >=24-hour values.
            # NumPy timedelta64 has the same 64-bit layout, so only reinterpret
            # the dtype here. No values or validity data are copied.
            return values.view(ch_type.np_type)
        # Time is Int32 on the wire while NumPy timedelta64 uses Int64. Widening
        # requires one allocation, but still avoids one Python timedelta object
        # per cell and lets NumPy perform the conversion in bulk.
        return values.astype(ch_type.np_type, copy=False)

    return convert


def _any_leaf(ch_type: ClickHouseType, predicate: Callable[[ClickHouseType], bool]) -> bool:
    if isinstance(ch_type, Array):
        return _any_leaf(ch_type.element_type, predicate)
    if isinstance(ch_type, Map):
        return _any_leaf(ch_type.key_type, predicate) or _any_leaf(ch_type.value_type, predicate)
    if isinstance(ch_type, dynamic_module.JSON):
        return any(_any_leaf(typed_type, predicate) for typed_type in ch_type.typed_types)
    element_types = getattr(ch_type, "element_types", None)
    if element_types is not None:
        return any(_any_leaf(element, predicate) for element in element_types)
    return predicate(ch_type)


def _validate_time64_units(ch_type: ClickHouseType) -> None:
    def validate(leaf: ClickHouseType) -> bool:
        if isinstance(leaf, Time64):
            _time64_unit(leaf)
        return False

    _any_leaf(ch_type, validate)


JsonTimePath = tuple[tuple[str, ...], ClickHouseType]
JsonTimePaths = dict[ClickHouseType, tuple[JsonTimePath, ...]]


def _prepare_json_time_paths(ch_type: ClickHouseType) -> JsonTimePaths:
    prepared: JsonTimePaths = {}

    def is_time(leaf: ClickHouseType) -> bool:
        return isinstance(leaf, (Time, Time64))

    def prepare(current: ClickHouseType) -> None:
        if isinstance(current, Array):
            prepare(current.element_type)
            return
        if isinstance(current, Map):
            prepare(current.key_type)
            prepare(current.value_type)
            return
        if isinstance(current, dynamic_module.JSON):
            prepared[current] = tuple(
                (tuple(dynamic_module._json_path_segments(path)), typed_type)
                for path, typed_type in zip(current.typed_paths, current.typed_types)
                if _any_leaf(typed_type, is_time)
            )
            for typed_type in current.typed_types:
                prepare(typed_type)
            return
        for element_type in getattr(current, "element_types", ()):
            prepare(element_type)

    prepare(ch_type)
    return prepared


def _contains_nested_time(ch_type: ClickHouseType) -> bool:
    # Container-only: bare Time/Time64 variants keep their dedicated or object-exit converters.
    return isinstance(ch_type, (Array, Tuple, Map, Nested, dynamic_module.JSON)) and _any_leaf(
        ch_type, lambda leaf: isinstance(leaf, (Time, Time64))
    )


def _materialize_raw_times(
    ch_type: ClickHouseType,
    raw: Any,
    extended_time_null: bool,
    json_time_paths: JsonTimePaths,
) -> Any:
    """Replace raw Time ticks in an otherwise materialized Python object tree."""
    if raw is None:
        if extended_time_null and isinstance(ch_type, Time):
            return options.np.timedelta64("NaT", "s")
        if extended_time_null and isinstance(ch_type, Time64):
            return options.np.timedelta64("NaT", _TIME64_UNITS[ch_type.scale])
        return None
    if isinstance(ch_type, Time):
        return options.np.timedelta64(raw, "s")
    if isinstance(ch_type, Time64):
        return options.np.timedelta64(raw, _TIME64_UNITS[ch_type.scale])
    if isinstance(ch_type, dynamic_module.JSON):
        for segments, typed_type in json_time_paths.get(ch_type, ()):
            item = raw
            for segment in segments[:-1]:
                item = item[segment]
            key = segments[-1]
            item[key] = _materialize_raw_times(typed_type, item[key], extended_time_null, json_time_paths)
        return raw
    if isinstance(ch_type, Nested):
        return [
            {
                name: _materialize_raw_times(element_type, item[name], extended_time_null, json_time_paths)
                for name, element_type in zip(ch_type.element_names, ch_type.element_types)
            }
            for item in raw
        ]
    if isinstance(ch_type, Array):
        for index, value in enumerate(raw):
            raw[index] = _materialize_raw_times(ch_type.element_type, value, extended_time_null, json_time_paths)
        return raw
    if isinstance(ch_type, Tuple):
        if isinstance(raw, dict):
            return {
                name: _materialize_raw_times(elem_type, raw[name], extended_time_null, json_time_paths)
                for name, elem_type in zip(ch_type.element_names, ch_type.element_types)
            }
        return tuple(
            _materialize_raw_times(elem_type, value, extended_time_null, json_time_paths)
            for elem_type, value in zip(ch_type.element_types, raw)
        )
    if isinstance(ch_type, Map):
        return {
            _materialize_raw_times(ch_type.key_type, key, extended_time_null, json_time_paths): _materialize_raw_times(
                ch_type.value_type, value, extended_time_null, json_time_paths
            )
            for key, value in raw.items()
        }
    return raw


def _make_nested_time_convert(ch_type: ClickHouseType, context: QueryContext) -> BlockConverter:
    _validate_time64_units(ch_type)
    extended_time_null = context.as_pandas and context.use_extended_dtypes
    json_time_paths = _prepare_json_time_paths(ch_type)
    leaf_predicate = _refinalize_predicate(ch_type, context)

    def convert(_arrow_table: Any, col_batch: Any, index: int) -> Any:
        try:
            column = col_batch.column_data(index, raw_time_ticks=True)
        except NotImplementedError as ex:
            raise NotSupportedError(
                f"The rust native codec cannot decode this column for numpy/pandas output: {ex}. "
                'Use native_codec="python" to fall back to the Python codec'
            ) from ex
        if json_time_paths:
            column = cast(list[Any], _normalize_json_shared_column(ch_type, column, context))
        for row, value in enumerate(column):
            column[row] = _materialize_raw_times(ch_type, value, extended_time_null, json_time_paths)
        if leaf_predicate is not None:
            column = _refinalize_leaves(ch_type, column, context, leaf_predicate)
        return column

    return convert


def _array_time_leaf(ch_type: ClickHouseType) -> tuple[int, Time | Time64] | None:
    """Return (nesting depth, leaf type) for pure Array(...(Time/Time64)) columns, else None."""
    depth = 0
    while isinstance(ch_type, Array):
        depth += 1
        ch_type = ch_type.element_type
    if depth and isinstance(ch_type, (Time, Time64)) and not ch_type.low_card:
        return depth, ch_type
    return None


def _make_array_time_convert(leaf: Time | Time64, depth: int, context: QueryContext) -> BlockConverter:
    unit = "s" if isinstance(leaf, Time) else _time64_unit(leaf)
    extended_time_null = context.as_pandas and context.use_extended_dtypes

    def convert(arrow_table: Any, _col_batch: Any, index: int) -> Any:
        column = _arrow_column(arrow_table, index)
        offset_levels = []
        for _ in range(depth):
            offset_levels.append(column.offsets.to_numpy().tolist())
            column = column.values
        if leaf.nullable:
            null_count = column.null_count
            if isinstance(leaf, Time):
                column = column.cast(options.arrow.int64())
            values = column.cast(options.arrow.duration(unit)).to_numpy(zero_copy_only=False)
            # list() of a timedelta64 array yields numpy scalars, NaT included,
            # which is the extended-dtypes null representation already.
            cells = list(values)
            if null_count and not extended_time_null:
                for null_index in options.np.flatnonzero(options.np.isnat(values)):
                    cells[null_index] = None
        else:
            values = column.to_numpy(zero_copy_only=False)
            values = values.view(leaf.np_type) if isinstance(leaf, Time64) else values.astype(leaf.np_type, copy=False)
            cells = list(values)
        for offsets in reversed(offset_levels):
            cells = [cells[start:stop] for start, stop in zip(offsets, offsets[1:])]
        return cells

    return convert


def _make_low_card_time_convert(ch_type: Time, as_pandas: bool) -> BlockConverter:
    def convert(arrow_table: Any, _col_batch: Any, index: int) -> Any:
        # The core exports LowCardinality(Time) as dictionary<int32, int32> with
        # nulls in the indices. Decoding then casting stays fully vectorized.
        column = _arrow_column(arrow_table, index).dictionary_decode()
        values = column.cast(options.arrow.int64()).cast(options.arrow.duration("s")).to_numpy(zero_copy_only=False)
        if as_pandas or not ch_type.nullable:
            return values
        result = list(values)
        if column.null_count:
            for null_index in options.np.flatnonzero(options.np.isnat(values)):
                result[null_index] = None
        return result

    return convert


def _extended_refinalize_leaf(leaf: ClickHouseType) -> bool:
    # Nullable leaves gain pd.NA/NaN/NaT, and Date leaves are converted from rust's datetime.date objects
    # to the numpy datetime64 values produced by the Python codec. Time leaves are excluded:
    # _materialize_raw_times fully renders them before refinalize runs.
    return (leaf.nullable and not isinstance(leaf, (Time, Time64))) or isinstance(leaf, Date)


def _bfloat16_refinalize_leaf(leaf: ClickHouseType) -> bool:
    # Non-extended numpy output only densifies nullable BFloat16 leaves. Other nullable leaves keep
    # python None, matching the Python codec.
    return isinstance(leaf, BFloat16) and leaf.nullable and not leaf.low_card


LeafPredicate = Callable[[ClickHouseType], bool]


def _needs_refinalize(ch_type: ClickHouseType, leaf_predicate: LeafPredicate = _extended_refinalize_leaf) -> bool:
    # Only leaves need rewriting.
    return _any_leaf(ch_type, leaf_predicate)


def _refinalize_predicate(ch_type: ClickHouseType, context: QueryContext) -> LeafPredicate | None:
    """Select the leaf predicate for nested refinalize, or None when no leaf needs it."""
    if not isinstance(ch_type, (Array, Tuple, Map, Nested)):
        return None
    if context.as_pandas and context.use_extended_dtypes:
        predicate: LeafPredicate = _extended_refinalize_leaf
    elif context.use_numpy:
        predicate = _bfloat16_refinalize_leaf
    else:
        return None
    return predicate if _needs_refinalize(ch_type, predicate) else None


def _refinalize_leaves(
    ch_type: ClickHouseType,
    column: list,
    context: QueryContext,
    leaf_predicate: LeafPredicate = _extended_refinalize_leaf,
) -> list:
    """Rebuild a rust-decoded object column so nested leaves match the Python codec.

    The rust object exit materializes nulls as python None. The Python codec finalizes each flat leaf
    column, so pandas/numpy leaves render nulls as pd.NA/NaT and values as numpy scalars. Each affected
    leaf is flattened, run through its own _finalize_column, and resliced. Unaffected sibling leaves keep
    their value-equal rust-native scalars.
    """
    if isinstance(ch_type, Array):
        lengths = [None if cell is None else len(cell) for cell in column]
        flat = _refinalize_leaves(
            ch_type.element_type,
            [item for cell in column if cell is not None for item in cell],
            context,
            leaf_predicate,
        )
        out: list = []
        pos = 0
        for length in lengths:
            if length is None:
                out.append(None)
                continue
            out.append(flat[pos : pos + length])
            pos += length
        return out
    if isinstance(ch_type, Nested):
        lengths = [None if row is None else len(row) for row in column]
        flat = [dict(item) for row in column if row is not None for item in row]
        refinalize_indexes = [index for index, elem in enumerate(ch_type.element_types) if _needs_refinalize(elem, leaf_predicate)]
        if refinalize_indexes:
            refinalized_columns = [
                _refinalize_leaves(
                    ch_type.element_types[index],
                    [item[ch_type.element_names[index]] for item in flat],
                    context,
                    leaf_predicate,
                )
                for index in refinalize_indexes
            ]
            for item_index, item in enumerate(flat):
                for element_index, values in zip(refinalize_indexes, refinalized_columns):
                    item[ch_type.element_names[element_index]] = values[item_index]
        out = []
        position = 0
        for length in lengths:
            if length is None:
                out.append(None)
                continue
            out.append(flat[position : position + length])
            position += length
        return out
    if isinstance(ch_type, Tuple):
        refinalize_indexes = [index for index, elem in enumerate(ch_type.element_types) if _needs_refinalize(elem, leaf_predicate)]
        if not refinalize_indexes:
            return column
        keyed = bool(ch_type.element_names) and isinstance(first_value(column), dict)
        keys: list = [ch_type.element_names[index] if keyed else index for index in refinalize_indexes]
        columns: list[list] = [[] for _ in refinalize_indexes]
        for row in column:
            if row is None:
                continue
            for slot, key in zip(columns, keys):
                slot.append(row[key])
        columns = [
            _refinalize_leaves(ch_type.element_types[index], slot, context, leaf_predicate)
            for index, slot in zip(refinalize_indexes, columns)
        ]
        rows_iter = iter(zip(*columns))
        out = []
        for row in column:
            if row is None:
                out.append(None)
                continue
            replaced = next(rows_iter)
            if keyed:
                updated = dict(row)
                updated.update(zip(keys, replaced))
                out.append(updated)
            else:
                values = list(row)
                for index, value in zip(refinalize_indexes, replaced):
                    values[index] = value
                out.append(tuple(values))
        return out
    if isinstance(ch_type, Map):
        refinalize_keys = _needs_refinalize(ch_type.key_type, leaf_predicate)
        refinalize_values = _needs_refinalize(ch_type.value_type, leaf_predicate)
        if not refinalize_keys and not refinalize_values:
            return column
        key_iter = None
        if refinalize_keys:
            key_iter = iter(_refinalize_leaves(ch_type.key_type, [key for row in column if row for key in row], context, leaf_predicate))
        value_iter = None
        if refinalize_values:
            value_iter = iter(
                _refinalize_leaves(ch_type.value_type, [value for row in column if row for value in row.values()], context, leaf_predicate)
            )
        out = []
        for row in column:
            if row is None:
                out.append(None)
                continue
            mapped = {}
            for key, value in row.items():
                mapped[next(key_iter) if key_iter is not None else key] = next(value_iter) if value_iter is not None else value
            out.append(mapped)
        return out
    # Float leaf nulls become numpy NaN via the Python codec's numpy read rather than _finalize_column.
    # BFloat16 leaves fall through to _finalize_column, which produces pd.array Float32 in extended mode.
    if _np_kind(ch_type) == "f" and not (isinstance(ch_type, BFloat16) and not ch_type.low_card):
        return list(options.np.array(column, dtype=ch_type.np_type))
    # Aware stdlib datetimes from the rust exit become Timestamps so _finalize_column takes its tz-aware path.
    if isinstance(ch_type, DateTimeBase) and getattr(first_value(column), "tzinfo", None) is not None:
        column = [None if value is None else options.pd.Timestamp(value) for value in column]
    # _finalize_column returns a pandas/numpy container for the types that diverge.
    finalized = ch_type._finalize_column(column, context)
    return finalized if isinstance(finalized, list) else list(finalized)


def _make_object_convert(ch_type: ClickHouseType, context: QueryContext) -> BlockConverter:
    leaf_predicate = _refinalize_predicate(ch_type, context)
    extended_pandas = context.as_pandas and context.use_extended_dtypes

    def convert(_arrow_table: Any, col_batch: Any, index: int) -> Any:
        try:
            column = col_batch.column_data(index)
        except NotImplementedError as ex:
            raise NotSupportedError(
                f"The rust native codec cannot decode this column for numpy/pandas output: {ex}. "
                'Use native_codec="python" to fall back to the Python codec'
            ) from ex
        column = cast(list[Any], _normalize_json_shared_column(ch_type, column, context))
        if leaf_predicate is not None:
            column = _refinalize_leaves(ch_type, column, context, leaf_predicate)
        if extended_pandas and isinstance(ch_type, DateTimeBase) and getattr(first_value(column), "tzinfo", None) is not None:
            # DateTimeBase._finalize_column's extended-dtype path recognizes timezone-aware pandas values
            # by their `.tz` attribute. The rust object exit produces stdlib datetime values, whose
            # equivalent attribute is `.tzinfo`, so wrap them before nullable pandas finalization selects
            # a naive dtype. Non-pandas output keeps the stdlib datetimes the Python codec returns.
            column = [None if value is None else options.pd.Timestamp(value) for value in column]
        return ch_type._finalize_column(column, context)

    return convert


def _make_string_convert(ch_type: ClickHouseType, context: QueryContext) -> BlockConverter:
    # Extended pandas String output built from the Arrow buffers instead of a Python str list. The rust export
    # does not validate UTF-8, so a block with invalid bytes takes the object exit and renders them as hex.
    pd_dtype = options.pd.StringDtype()
    object_convert = _make_object_convert(ch_type, context)

    def convert(arrow_table: Any, col_batch: Any, index: int) -> Any:
        column = arrow_table.column(index)
        try:
            column.validate(full=True)
        except options.arrow.ArrowInvalid:
            return object_convert(arrow_table, col_batch, index)
        return pd_dtype.__from_arrow__(column)

    return convert


def _make_nullable_int_convert(pd_dtype: Any) -> BlockConverter:
    def convert(_arrow_table: Any, col_batch: Any, index: int) -> Any:
        columns = col_batch.column_buffers(index)
        if columns is None:
            raise NotImplementedError(f"Unsupported column buffers for {pd_dtype}")
        values = [_buffer_values(column) for column in columns]
        masks = [_buffer_null_mask(column) for column in columns]
        if not values:
            return options.pd.arrays.IntegerArray(options.np.empty(0, dtype=pd_dtype.numpy_dtype), options.np.empty(0, dtype="bool"))
        data = values[0] if len(values) == 1 else options.np.concatenate(values)
        mask = masks[0] if len(masks) == 1 else options.np.concatenate(masks)
        return options.pd.arrays.IntegerArray(data.astype(pd_dtype.numpy_dtype, copy=False), mask, copy=False)

    return convert


def _nullable_float_buffer_convert(_arrow_table: Any, col_batch: Any, index: int) -> Any:
    # The Python codec renders nullable Float32/Float64 as a plain float64 array with NaN in null positions.
    columns = col_batch.column_buffers(index)
    if columns is None:
        raise NotImplementedError("Unsupported column buffers for nullable float")
    chunks = []
    for column in columns:
        values = _buffer_values(column)
        if column.null_count:
            values = options.np.where(_buffer_null_mask(column), options.np.nan, values)
        chunks.append(values.astype("float64", copy=False))
    return _join_chunks(chunks, "float64")


def _nullable_float_convert(arrow_table: Any, _col_batch: Any, index: int) -> Any:
    return _arrow_column(arrow_table, index).to_numpy(zero_copy_only=False).astype("float64")


def _np_kind(ch_type: ClickHouseType) -> str | None:
    try:
        return str(options.np.dtype(ch_type.np_type).kind)
    except Exception:  # noqa: BLE001 - any non-numpy np_type is not an Arrow-numeric column
        return None


def _numeric_buffer_type(ch_type: ClickHouseType) -> ClickHouseType | None:
    while isinstance(ch_type, SimpleAggregateFunction) and not ch_type.low_card:
        ch_type = ch_type.element_type
    if not ch_type.low_card and (
        ch_type.base_type in _NUMERIC_BUFFER_TYPES or ch_type.base_type in ("Bool", "Boolean") or isinstance(ch_type, (BFloat16, Interval))
    ):
        return ch_type
    return None


def _build_converter(ch_type: ClickHouseType, context: QueryContext) -> _Converter:
    declared_nullable = ch_type.nullable
    # SimpleAggregateFunction is a name-decoration alias: convert as the element type, matching both the
    # rust core's physical_delegate expansion and the Python codec's delegated read.
    if isinstance(ch_type, SimpleAggregateFunction) and not ch_type.low_card:
        ch_type = ch_type.element_type
    _validate_time64_units(ch_type)
    if isinstance(ch_type, BFloat16) and not ch_type.low_card:
        extended = ch_type.nullable and context.as_pandas and context.use_extended_dtypes
        return _Converter(False, _make_bfloat16_convert(extended))
    # LowCardinality(T) routes through the object exit regardless of inner type. Its values are correct there,
    # and the Python codec's own LowCardinality numpy handling is inconsistent per inner type (and truncates
    # LowCardinality(numeric)), so there is no clean parity target for an Arrow dictionary fast path.
    if isinstance(ch_type, (Time, Time64)) and not ch_type.low_card:
        return _Converter(True, _make_time_convert(ch_type, context.as_pandas, context.use_extended_dtypes))
    if isinstance(ch_type, Time) and ch_type.low_card:
        return _Converter(True, _make_low_card_time_convert(ch_type, context.as_pandas))
    if isinstance(ch_type, Interval) and not ch_type.low_card:
        if not ch_type.nullable:
            # Nullable(SimpleAggregateFunction(anyLast, Interval)) unwraps to a plain Interval whose column carries nulls.
            return _Converter(False, _make_numeric_buffer_convert(ch_type, nullable_alias=True))
        if context.as_pandas and context.use_extended_dtypes:
            return _Converter(False, _make_nullable_int_convert(options.pd.Int64Dtype()))
    array_time = _array_time_leaf(ch_type)
    if array_time is not None:
        depth, leaf = array_time
        return _Converter(True, _make_array_time_convert(leaf, depth, context))
    if _contains_nested_time(ch_type):
        return _Converter(False, _make_nested_time_convert(ch_type, context))
    if (
        isinstance(ch_type, String)
        and not ch_type.low_card
        and context.as_pandas
        and context.use_extended_dtypes
        and ch_type.read_format(context) == "native"
    ):
        return _Converter(True, _make_string_convert(ch_type, context))
    if not ch_type.nullable and not ch_type.low_card:
        if isinstance(ch_type, DateTime64):
            _ = ch_type.np_type  # ProgrammingError for precisions outside {0,3,6,9}, matching the Python codec
            return _Converter(True, _make_datetime64_convert(context.as_pandas, context.active_tz(ch_type.tzinfo)))
        if isinstance(ch_type, DateTime):
            return _Converter(True, _make_datetime_convert(context.as_pandas, context.active_tz(ch_type.tzinfo)))
        if isinstance(ch_type, Date):  # Date32 subclasses Date
            return _Converter(True, _make_date_convert(context.as_pandas))
        if not declared_nullable and (ch_type.base_type in _NUMERIC_BUFFER_TYPES or ch_type.base_type in ("Bool", "Boolean")):
            return _Converter(False, _make_numeric_buffer_convert(ch_type))
        if _np_kind(ch_type) in ("i", "u", "f", "b"):
            physical_type = _numeric_buffer_type(ch_type)
            if isinstance(physical_type, BFloat16):
                return _Converter(False, _make_bfloat16_alias_convert())
            if isinstance(physical_type, Interval) and physical_type.base_type in _INTERVAL_ARROW_UNITS:
                return _Converter(False, _make_interval_alias_convert(_INTERVAL_ARROW_UNITS[physical_type.base_type]))
            if physical_type is not None:
                return _Converter(False, _make_numeric_buffer_convert(physical_type, nullable_alias=True))
            return _Converter(True, _numeric_convert)
    elif ch_type.nullable and not ch_type.low_card and context.as_pandas and context.use_extended_dtypes:
        # Extended pandas output keeps integer masks and widens nullable floats to float64 with NaN.
        kind = _np_kind(ch_type)
        if kind in ("i", "u"):
            return _Converter(False, _make_nullable_int_convert(options.pd.api.types.pandas_dtype(ch_type.base_type)))
        if kind == "f":
            physical_type = _numeric_buffer_type(ch_type)
            if isinstance(physical_type, BFloat16):
                return _Converter(False, _make_bfloat16_alias_convert(as_float=True))
            if physical_type is not None:
                return _Converter(False, _nullable_float_buffer_convert)
            return _Converter(True, _nullable_float_convert)
    return _Converter(False, _make_object_convert(ch_type, context))


def _build_converters(column_types: Sequence[ClickHouseType], context: QueryContext) -> list[_Converter]:
    """Resolve one converter per column from the driver's own ClickHouseType metadata."""
    return [_build_converter(ch_type, context) for ch_type in column_types]


def _convert_block(col_batch: Any, converters: Sequence[_Converter]) -> list:
    """Convert one decoded ColBatch into a list of numpy arrays, pandas arrays, or object lists."""
    arrow_table = None
    if any(conv.needs_arrow for conv in converters):
        arrow_table = options.arrow.RecordBatchReader.from_stream(col_batch).read_all()
    return [conv(arrow_table, col_batch, index) for index, conv in enumerate(converters)]
