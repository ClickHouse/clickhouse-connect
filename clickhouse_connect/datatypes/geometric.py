from collections.abc import Collection, Sequence
from typing import Any

from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.driver.insert import InsertContext
from clickhouse_connect.driver.query import QueryContext
from clickhouse_connect.driver.types import ByteSource

POINT_DATA_TYPE: ClickHouseType
RING_DATA_TYPE: ClickHouseType
POLYGON_DATA_TYPE: ClickHouseType
MULTI_POLYGON_DATA_TYPE: ClickHouseType
GEOMETRY_DATA_TYPE: ClickHouseType

# ruff: noqa: F821 (Undefine name)


class Point(ClickHouseType):
    def _data_size(self, sample: Collection) -> int:
        return POINT_DATA_TYPE._data_size(sample)

    def write_column(self, column: Sequence, dest: bytearray, ctx: InsertContext):
        return POINT_DATA_TYPE.write_column(column, dest, ctx)

    def write_column_data(self, column: Sequence, dest: bytearray, ctx: InsertContext):
        return POINT_DATA_TYPE.write_column_data(column, dest, ctx)

    def read_column_prefix(self, source: ByteSource, ctx: QueryContext):
        return POINT_DATA_TYPE.read_column_prefix(source, ctx)

    def read_column_data(self, source: ByteSource, num_rows: int, ctx: QueryContext, read_state: Any) -> Sequence:
        return POINT_DATA_TYPE.read_column_data(source, num_rows, ctx, read_state)


class Ring(ClickHouseType):
    def _data_size(self, sample: Collection) -> int:
        return RING_DATA_TYPE._data_size(sample)

    def write_column(self, column: Sequence, dest: bytearray, ctx: InsertContext):
        return RING_DATA_TYPE.write_column(column, dest, ctx)

    def write_column_data(self, column: Sequence, dest: bytearray, ctx: InsertContext):
        return RING_DATA_TYPE.write_column_data(column, dest, ctx)

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

    def read_column_prefix(self, source: ByteSource, ctx: QueryContext):
        return MULTI_POLYGON_DATA_TYPE.read_column_prefix(source, ctx)

    def read_column_data(self, source: ByteSource, num_rows: int, ctx: QueryContext, read_state: Any) -> Sequence:
        return MULTI_POLYGON_DATA_TYPE.read_column_data(source, num_rows, ctx, read_state)


class LineString(Ring):
    pass


class MultiLineString(Polygon):
    pass


class Geometry(ClickHouseType):
    """ClickHouse Geometry, a fixed Variant over the six geo types."""

    def data_size(self, sample: Collection[Any]) -> int:
        return GEOMETRY_DATA_TYPE.data_size(sample)

    def write_column_prefix(self, dest: bytearray):
        return GEOMETRY_DATA_TYPE.write_column_prefix(dest)

    def write_column_data(self, column: Sequence, dest: bytearray, ctx: InsertContext):
        return GEOMETRY_DATA_TYPE.write_column_data(column, dest, ctx)

    def read_column_prefix(self, source: ByteSource, ctx: QueryContext):
        return GEOMETRY_DATA_TYPE.read_column_prefix(source, ctx)

    def read_column_data(self, source: ByteSource, num_rows: int, ctx: QueryContext, read_state: Any) -> Sequence:
        return GEOMETRY_DATA_TYPE.read_column_data(source, num_rows, ctx, read_state)
