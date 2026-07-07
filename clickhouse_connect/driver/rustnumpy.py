"""Numpy/pandas column converters for the rust native codec.

The rust Arrow export is raw: Date is uint16 days, DateTime is uint32 seconds with the timezone dropped,
Enum is raw ints. A naive to_pandas() therefore yields wrong dtypes. These converters are resolved once
per query from the driver's own ClickHouseType (np_type, tzinfo, nullability) so the produced columns match
the Python codec by construction. Non-nullable numeric and temporal columns take the zero-copy Arrow exit;
strings, enums, low-cardinality, and nullable columns take the rust python-object exit and are finalized
through the driver's own _finalize_column.
"""

import logging
from collections.abc import Callable, Sequence
from typing import Any

from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.datatypes.temporal import Date, DateTime, DateTime64
from clickhouse_connect.driver import options
from clickhouse_connect.driver.exceptions import NotSupportedError
from clickhouse_connect.driver.query import QueryContext

logger = logging.getLogger(__name__)

BlockConverter = Callable[[Any, Any, int], Any]


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


def _make_object_convert(ch_type: ClickHouseType, context: QueryContext) -> BlockConverter:
    def convert(_arrow_table: Any, col_batch: Any, index: int) -> Any:
        try:
            column = col_batch.column_data(index)
        except NotImplementedError as ex:
            raise NotSupportedError(
                f"The rust native codec cannot decode this column for numpy/pandas output: {ex}. "
                'Use native_codec="python" to fall back to the Python codec'
            ) from ex
        return ch_type._finalize_column(column, context)

    return convert


def _make_nullable_int_convert(pd_dtype: Any) -> BlockConverter:
    def convert(arrow_table: Any, _col_batch: Any, index: int) -> Any:
        return pd_dtype.__from_arrow__(_arrow_column(arrow_table, index))

    return convert


def _nullable_float_convert(arrow_table: Any, _col_batch: Any, index: int) -> Any:
    # The Python codec renders nullable Float32/Float64 as a plain float64 array with NaN in null positions.
    return _arrow_column(arrow_table, index).to_numpy(zero_copy_only=False).astype("float64")


def _np_kind(ch_type: ClickHouseType) -> str | None:
    try:
        return str(options.np.dtype(ch_type.np_type).kind)
    except Exception:  # noqa: BLE001 - any non-numpy np_type is not an Arrow-numeric column
        return None


def _build_converter(ch_type: ClickHouseType, context: QueryContext) -> _Converter:
    # LowCardinality(T) routes through the object exit regardless of inner type. Its values are correct there,
    # and the Python codec's own LowCardinality numpy handling is inconsistent per inner type (and truncates
    # LowCardinality(numeric)), so there is no clean parity target for an Arrow dictionary fast path.
    if not ch_type.nullable and not ch_type.low_card:
        if isinstance(ch_type, DateTime64):
            _ = ch_type.np_type  # ProgrammingError for precisions outside {0,3,6,9}, matching the Python codec
            return _Converter(True, _make_datetime64_convert(context.as_pandas, context.active_tz(ch_type.tzinfo)))
        if isinstance(ch_type, DateTime):
            return _Converter(True, _make_datetime_convert(context.as_pandas, context.active_tz(ch_type.tzinfo)))
        if isinstance(ch_type, Date):  # Date32 subclasses Date
            return _Converter(True, _make_date_convert(context.as_pandas))
        if _np_kind(ch_type) in ("i", "u", "f", "b"):
            return _Converter(True, _numeric_convert)
    elif ch_type.nullable and not ch_type.low_card and context.as_pandas and context.use_extended_dtypes:
        # query_df renders nullable numeric via zero-copy pandas extension arrays. Building them from the Arrow
        # validity+values buffers skips the per-value Python object list the object exit would otherwise create.
        kind = _np_kind(ch_type)
        if kind in ("i", "u"):
            return _Converter(True, _make_nullable_int_convert(options.pd.api.types.pandas_dtype(ch_type.base_type)))
        if kind == "f":
            return _Converter(True, _nullable_float_convert)
    return _Converter(False, _make_object_convert(ch_type, context))


def build_converters(column_types: Sequence[ClickHouseType], context: QueryContext) -> list[_Converter]:
    """Resolve one converter per column from the driver's own ClickHouseType metadata."""
    return [_build_converter(ch_type, context) for ch_type in column_types]


def convert_block(col_batch: Any, converters: Sequence[_Converter]) -> list:
    """Convert one decoded ColBatch into a list of numpy arrays, pandas arrays, or object lists."""
    arrow_table = None
    if any(conv.needs_arrow for conv in converters):
        arrow_table = options.arrow.RecordBatchReader.from_stream(col_batch).read_all()
    return [conv(arrow_table, col_batch, index) for index, conv in enumerate(converters)]
