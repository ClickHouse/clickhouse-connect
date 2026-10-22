from collections.abc import Collection, Sequence
from typing import Any

from clickhouse_connect.datatypes.base import ClickHouseType, TypeDef, _TypeArgs
from clickhouse_connect.datatypes.dynamic import Variant
from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver.insert import InsertContext
from clickhouse_connect.driver.query import QueryContext
from clickhouse_connect.driver.types import ByteSource

POINT_DATA_TYPE: ClickHouseType
RING_DATA_TYPE: ClickHouseType
POLYGON_DATA_TYPE: ClickHouseType
MULTI_POLYGON_DATA_TYPE: ClickHouseType

# ruff: noqa: F821 (Undefine name)


class Point(ClickHouseType):
    def _data_size(self, sample: Collection) -> int:
        return POINT_DATA_TYPE._data_size(sample)

    def _write_column_binary(self, column: Sequence, dest: bytearray, ctx: InsertContext):
        # Nullable(Point) rows that are None are hidden by the NULL map the base class writes
        if self.nullable:
            POINT_DATA_TYPE._write_masked_column_data(column, dest, ctx, [value is None for value in column])
        else:
            POINT_DATA_TYPE.write_column_data(column, dest, ctx)

    def _write_masked_column_data(
        self,
        column: Sequence[Any],
        dest: bytearray,
        ctx: InsertContext,
        null_map: Sequence[bool],
    ) -> None:
        if self.nullable:
            null_map = [hidden or value is None for value, hidden in zip(column, null_map)]
            dest += bytes(null_map)
        POINT_DATA_TYPE._write_masked_column_data(column, dest, ctx, null_map)

    def read_column_prefix(self, source: ByteSource, ctx: QueryContext):
        return POINT_DATA_TYPE.read_column_prefix(source, ctx)

    def _read_column_binary(self, source: ByteSource, num_rows: int, ctx: QueryContext, read_state: Any) -> Sequence:
        return POINT_DATA_TYPE.read_column_data(source, num_rows, ctx, read_state)


class Ring(ClickHouseType):
    def _data_size(self, sample: Collection) -> int:
        return RING_DATA_TYPE._data_size(sample)

    def write_column(self, column: Sequence, dest: bytearray, ctx: InsertContext):
        return RING_DATA_TYPE.write_column(column, dest, ctx)

    def write_column_data(self, column: Sequence, dest: bytearray, ctx: InsertContext):
        return RING_DATA_TYPE.write_column_data(column, dest, ctx)

    def _write_masked_column_data(
        self,
        column: Sequence[Any],
        dest: bytearray,
        ctx: InsertContext,
        null_map: Sequence[bool],
    ) -> None:
        RING_DATA_TYPE._write_masked_column_data(column, dest, ctx, null_map)

    def read_column_prefix(self, source: ByteSource, ctx: QueryContext):
        return RING_DATA_TYPE.read_column_prefix(source, ctx)

    def read_column_data(self, source: ByteSource, num_rows: int, ctx: QueryContext, read_state) -> Sequence:
        return RING_DATA_TYPE.read_column_data(source, num_rows, ctx, read_state)


class Polygon(ClickHouseType):
    def _data_size(self, sample: Collection) -> int:
        return POLYGON_DATA_TYPE._data_size(sample)

    def write_column(self, column: Sequence, dest: bytearray, ctx: InsertContext):
        return POLYGON_DATA_TYPE.write_column(column, dest, ctx)

    def write_column_data(self, column: Sequence, dest: bytearray, ctx: InsertContext):
        return POLYGON_DATA_TYPE.write_column_data(column, dest, ctx)

    def _write_masked_column_data(
        self,
        column: Sequence[Any],
        dest: bytearray,
        ctx: InsertContext,
        null_map: Sequence[bool],
    ) -> None:
        POLYGON_DATA_TYPE._write_masked_column_data(column, dest, ctx, null_map)

    def read_column_prefix(self, source: ByteSource, ctx: QueryContext):
        return POLYGON_DATA_TYPE.read_column_prefix(source, ctx)

    def read_column_data(self, source: ByteSource, num_rows: int, ctx: QueryContext, read_state: Any) -> Sequence:
        return POLYGON_DATA_TYPE.read_column_data(source, num_rows, ctx, read_state)


class MultiPolygon(ClickHouseType):
    def _data_size(self, sample: Collection) -> int:
        return MULTI_POLYGON_DATA_TYPE._data_size(sample)

    def write_column(self, column: Sequence, dest: bytearray, ctx: InsertContext):
        return MULTI_POLYGON_DATA_TYPE.write_column(column, dest, ctx)

    def write_column_data(self, column: Sequence, dest: bytearray, ctx: InsertContext):
        return MULTI_POLYGON_DATA_TYPE.write_column_data(column, dest, ctx)

    def _write_masked_column_data(
        self,
        column: Sequence[Any],
        dest: bytearray,
        ctx: InsertContext,
        null_map: Sequence[bool],
    ) -> None:
        MULTI_POLYGON_DATA_TYPE._write_masked_column_data(column, dest, ctx, null_map)

    def read_column_prefix(self, source: ByteSource, ctx: QueryContext):
        return MULTI_POLYGON_DATA_TYPE.read_column_prefix(source, ctx)

    def read_column_data(self, source: ByteSource, num_rows: int, ctx: QueryContext, read_state: Any) -> Sequence:
        return MULTI_POLYGON_DATA_TYPE.read_column_data(source, num_rows, ctx, read_state)


class LineString(Ring):
    pass


class MultiLineString(Polygon):
    pass


class MultiPoint(Ring):
    pass


class Geometry(Variant):
    """ClickHouse Geometry with its fixed Native discriminator order."""

    python_type = None
    valid_formats = "typed", "native"
    _type_args = _TypeArgs()
    _alternative_names = (
        "LineString",
        "MultiLineString",
        "MultiPolygon",
        "Point",
        "Polygon",
        "Ring",
        "MultiPoint",
    )

    def __init__(self, type_def: TypeDef):
        ClickHouseType.__init__(self, type_def)
        self.element_types = [get_from_name(name) for name in self._alternative_names]
        self._build_dispatch()
