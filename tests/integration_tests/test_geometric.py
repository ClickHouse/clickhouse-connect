from collections.abc import Callable

import pytest

from clickhouse_connect.datatypes.dynamic import typed_variant
from clickhouse_connect.datatypes.geometric import Geometry
from clickhouse_connect.driver import AsyncClient, Client
from clickhouse_connect.driver.exceptions import DatabaseError, DataError
from clickhouse_connect.driver.parser import parse_callable
from tests.integration_tests.conftest import supports_multi_point

_POINT_COLUMN_TYPE = "Tuple(Float64, Float64)"
_GEOMETRY_PHYSICAL_TYPES = {
    "LineString": f"Array({_POINT_COLUMN_TYPE})",
    "MultiLineString": f"Array(Array({_POINT_COLUMN_TYPE}))",
    "MultiPolygon": f"Array(Array(Array({_POINT_COLUMN_TYPE})))",
    "Point": _POINT_COLUMN_TYPE,
    "Polygon": f"Array(Array({_POINT_COLUMN_TYPE}))",
    "Ring": f"Array({_POINT_COLUMN_TYPE})",
    "MultiPoint": f"Array({_POINT_COLUMN_TYPE})",
}


def _require_geometry(client: Client, call) -> None:
    try:
        resolved_type = call(client.command, "SELECT toTypeName(defaultValueOfTypeName('Geometry'))")
    except DatabaseError as ex:
        if ex.name != "UNKNOWN_TYPE":
            raise
        pytest.skip(f"Geometry is not supported by server {client.server_version}")
    if resolved_type != "Geometry":
        pytest.skip(f"Geometry is not supported by server {client.server_version}")


def _parse_geometry_column_members(column_type_name: str) -> tuple[str, ...]:
    wrapper, wrapper_args, remaining = parse_callable(column_type_name)
    assert wrapper == "Const" and len(wrapper_args) == 1 and not remaining
    variant, members, remaining = parse_callable(str(wrapper_args[0]))
    assert variant == "Variant" and not remaining
    return tuple(str(member) for member in members)


def test_supports_multi_point_requires_async_call():
    client = AsyncClient.__new__(AsyncClient)
    with pytest.raises(TypeError, match="call is required when checking MultiPoint support with AsyncClient"):
        supports_multi_point(client)


def test_geometry_server_members_are_known_prefix(param_client: Client, call):
    _require_geometry(param_client, call)
    column_type_name = call(param_client.command, "SELECT toColumnTypeName(defaultValueOfTypeName('Geometry'))")
    server_members = _parse_geometry_column_members(column_type_name)
    known_names = Geometry._alternative_names
    message = (
        f"Server {param_client.server_version} Geometry layout is not a known prefix of {known_names}. "
        "When ClickHouse adds a Geometry member, append it at the END of Geometry._alternative_names."
    )
    assert len(server_members) <= len(known_names), message
    known_prefix = tuple(_GEOMETRY_PHYSICAL_TYPES[name] for name in known_names[: len(server_members)])
    assert server_members == known_prefix, message


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


def test_multi_point_python_codec_round_trip(client_factory, call, client_mode):
    client = client_factory(native_codec="python")
    if not supports_multi_point(client, call):
        pytest.skip(f"MultiPoint is not supported by server {client.server_version}")
    table = f"multi_point_python_codec_{client_mode}"
    rows = [
        [
            0,
            [(13.0, 23.0), (14.0, 24.0)],
            [[(31.0, 41.0)], []],
            ([(51.0, 61.0)], 7),
            [([(71.0, 81.0)], 13)],
            {"value": [(91.0, 101.0)]},
        ],
        [1, [], [], ([], 79), [], {}],
    ]
    schema = (
        "id UInt8, mp MultiPoint, a Array(MultiPoint), t Tuple(MultiPoint, UInt8), "
        "at Array(Tuple(MultiPoint, UInt8)), m Map(String, MultiPoint)"
    )

    try:
        call(client.command, f"DROP TABLE IF EXISTS {table}")
        call(client.command, f"CREATE TABLE {table} ({schema}) ENGINE MergeTree ORDER BY id")
        call(client.insert, table, rows, column_names=["id", "mp", "a", "t", "at", "m"])
        assert call(client.query, f"SELECT * FROM {table} ORDER BY id").result_rows == [tuple(row) for row in rows]
    finally:
        call(client.command, f"DROP TABLE IF EXISTS {table}")


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
    if supports_multi_point(client, call):
        values.append(("MultiPoint", [(131.0, 141.0), (132.0, 142.0)]))
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
