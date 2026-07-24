from collections.abc import Callable

import pytest

from clickhouse_connect.datatypes.dynamic import typed_variant
from clickhouse_connect.driver import Client
from clickhouse_connect.driver.exceptions import DatabaseError, DataError


def _require_geometry(client: Client, call) -> None:
    try:
        resolved_type = call(client.command, "SELECT toTypeName(defaultValueOfTypeName('Geometry'))")
    except DatabaseError as ex:
        if ex.name != "UNKNOWN_TYPE":
            raise
        pytest.skip(f"Geometry is not supported by server {client.server_version}")
    if resolved_type != "Geometry":
        pytest.skip(f"Geometry is not supported by server {client.server_version}")


def test_point_column(param_client: Client, call, table_context: Callable):
    with table_context("point_column_test", ["key Int32", "point Point"]):
        data = [[1, (3.55, 3.55)], [2, (4.55, 4.55)]]
        call(param_client.insert, "point_column_test", data)

        query_result = call(param_client.query, "SELECT * FROM point_column_test ORDER BY key").result_rows
        assert len(query_result) == 2
        assert query_result[0] == (1, (3.55, 3.55))
        assert query_result[1] == (2, (4.55, 4.55))


def test_ring_column(param_client: Client, call, table_context: Callable):
    with table_context("ring_column_test", ["key Int32", "ring Ring"]):
        data = [[1, [(5.522, 58.472), (3.55, 3.55)]], [2, [(4.55, 4.55)]]]
        call(param_client.insert, "ring_column_test", data)

        query_result = call(param_client.query, "SELECT * FROM ring_column_test ORDER BY key").result_rows
        assert len(query_result) == 2
        assert query_result[0] == (1, [(5.522, 58.472), (3.55, 3.55)])
        assert query_result[1] == (2, [(4.55, 4.55)])


def test_polygon_column(param_client: Client, call, table_context: Callable):
    with table_context("polygon_column_test", ["key Int32", "polygon Polygon"]):
        res = call(param_client.query, "SELECT readWKTPolygon('POLYGON ((-64.8 32.3, -65.5 18.3, -80.3 25.2, -64.8 32.3))') as polygon")
        pg = res.first_row[0]
        call(param_client.insert, "polygon_column_test", [(1, pg), (4, pg)])
        query_result = call(param_client.query, "SELECT key, polygon FROM polygon_column_test WHERE key = 4")
        assert query_result.first_row[1] == pg


def test_geometry_python_codec_round_trip(client_factory, call, client_mode):
    client = client_factory(native_codec="python")
    _require_geometry(client, call)
    table = f"geometry_python_codec_{client_mode}"
    values = [
        ("LineString", [(13.0, 23.0), (14.0, 24.0)]),
        ("MultiLineString", [[(31.0, 41.0), (32.0, 42.0)]]),
        ("MultiPolygon", [[[(51.0, 61.0)]]]),
        ("Point", (71.0, 81.0)),
        ("Polygon", [[(91.0, 101.0), (92.0, 102.0)]]),
        ("Ring", [(111.0, 121.0)]),
    ]
    rows = []
    expected = []
    for index, (type_name, value) in enumerate(values):
        tagged = typed_variant(value, type_name)
        rows.append(
            [
                index,
                tagged,
                [tagged, None],
                (tagged, index),
                [(tagged, index), (None, 79)],
                {"value": tagged, "null": None},
            ]
        )
        expected.append((index, value, [value, None], (value, index), [(value, index), (None, 79)], {"value": value, "null": None}))
    rows.append([len(values), None, [None], (None, len(values)), [(None, len(values))], {"null": None}])
    expected.append((len(values), None, [None], (None, len(values)), [(None, len(values))], {"null": None}))
    schema = "id UInt8, g Geometry, a Array(Geometry), t Tuple(Geometry, UInt8), at Array(Tuple(Geometry, UInt8)), m Map(String, Geometry)"

    try:
        call(client.command, f"DROP TABLE IF EXISTS {table}")
        call(client.command, f"CREATE TABLE {table} ({schema}) ENGINE MergeTree ORDER BY id")
        call(client.insert, table, rows, column_names=["id", "g", "a", "t", "at", "m"])
        result = call(client.query, f"SELECT * FROM {table} ORDER BY id").result_rows
        assert result == expected
    finally:
        call(client.command, f"DROP TABLE IF EXISTS {table}")


def test_geometry_python_codec_rejects_ambiguous_values(client_factory, call, client_mode):
    client = client_factory(native_codec="python")
    _require_geometry(client, call)
    table = f"geometry_python_codec_errors_{client_mode}"

    try:
        call(client.command, f"DROP TABLE IF EXISTS {table}")
        call(client.command, f"CREATE TABLE {table} (id UInt8, g Geometry) ENGINE MergeTree ORDER BY id")
        with pytest.raises(DataError, match="Cannot map Python type list"):
            call(client.insert, table, [[0, [(13.0, 23.0)]]], column_names=["id", "g"])
        with pytest.raises(DataError, match="Type 'String' is not a member"):
            call(client.insert, table, [[1, typed_variant("bad", "String")]], column_names=["id", "g"])
    finally:
        call(client.command, f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize("point", [(13.0,), (13.0, 23.0, 79.0)])
def test_geometry_python_codec_rejects_malformed_point(client_factory, call, client_mode, point):
    client = client_factory(native_codec="python")
    _require_geometry(client, call)
    table = f"geometry_python_codec_point_error_{client_mode}"

    try:
        call(client.command, f"DROP TABLE IF EXISTS {table}")
        call(client.command, f"CREATE TABLE {table} (id UInt8, g Geometry) ENGINE MergeTree ORDER BY id")
        with pytest.raises(DataError, match=r"Tuple\(Float64, Float64\).*row 0"):
            call(client.insert, table, [[0, typed_variant(point, "Point")]], column_names=["id", "g"])
    finally:
        call(client.command, f"DROP TABLE IF EXISTS {table}")
