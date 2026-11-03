import decimal
import struct
from datetime import date, datetime, timezone
from ipaddress import IPv4Address
from uuid import UUID

import numpy as np
import pytest
from clickhouse_connect.driverc.buffer import ResponseBuffer as CResponseBuffer

from clickhouse_connect.datatypes import registry
from clickhouse_connect.datatypes.dynamic import typed_variant
from clickhouse_connect.driver.buffer import ResponseBuffer as PyResponseBuffer
from clickhouse_connect.driver.exceptions import DataError, InternalError, NotSupportedError
from clickhouse_connect.driver.insert import InsertContext
from clickhouse_connect.driver.query import QueryContext, QueryResult
from clickhouse_connect.driver.transform import NativeTransform
from tests.helpers import bytes_source
from tests.unit_tests.test_driver.binary import NESTED_BINARY

UINT16_NULLS = """
    0104 0969 6e74 5f76 616c 7565 104e 756c
    6c61 626c 6528 5549 6e74 3136 2901 0001
    0000 0014 0000 0028 00
"""

LOW_CARDINALITY = """
    0102 026c 6316 4c6f 7743 6172 6469 6e61
    6c69 7479 2853 7472 696e 6729 0100 0000
    0000 0000 0006 0000 0000 0000 0300 0000
    0000 0000 0004 4344 4d41 0347 534d 0200
    0000 0000 0000 0102 0101 026c 6316 4c6f
    7743 6172 6469 6e61 6c69 7479 2853 7472
    696e 6729 0100 0000 0000 0000 0006 0000
    0000 0000 0200 0000 0000 0000 0004 554d
    5453 0100 0000 0000 0000 01
 """

LOW_CARD_ARRAY = """
    0102 066c 6162 656c 731d 4172 7261 7928
    4c6f 7743 6172 6469 6e61 6c69 7479 2853
    7472 696e 6729 2901 0000 0000 0000 0000
    0000 0000 0000 0000 0000 0000 0000 00
"""

SIMPLE_MAP = """
    0101 066e 6e5f 6d61 7013 4d61 7028 5374
    7269 6e67 2c20 5374 7269 6e67 2902 0000
    0000 0000 0004 6b65 7931 046b 6579 3206
    7661 6c75 6531 0676 616c 7565 32
"""

LOW_CARD_MAP = """
    0102 086d 6170 5f6e 756c 6c2b 4d61 7028
    4c6f 7743 6172 6469 6e61 6c69 7479 2853
    7472 696e 6729 2c20 4e75 6c6c 6162 6c65
    2855 5549 4429 2901 0000 0000 0000 0002
    0000 0000 0000 0004 0000 0000 0000 0000
    0600 0000 0000 0003 0000 0000 0000 0000
    0469 676f 7206 6765 6f72 6765 0400 0000
    0000 0000 0102 0102 0100 0000 0000 0000
    0000 0000 0000 0000 0000 0000 235f 7dc5
    799f 431d a9e1 93ca ccff c652 235f 7dc5
    799f 437f a9e1 93ca ccff 0052 235f 7dc5
    799f 431d a9e1 93ca ccff c652
"""


parse_response = NativeTransform().parse_response


def check_result(result, expected, row_num=0, col_num=0):
    result_set = result.result_set
    row = result_set[row_num]
    value = row[col_num]
    assert value == expected


def test_uint16_nulls():
    result = parse_response(bytes_source(UINT16_NULLS))
    assert result.result_set == [(None,), (20,), (None,), (40,)]


def test_low_cardinality():
    result = parse_response(bytes_source(LOW_CARDINALITY))
    assert result.result_set == [("CDMA",), ("GSM",), ("UMTS",)]


def test_low_card_array():
    result = parse_response(bytes_source(LOW_CARD_ARRAY))
    assert result.first_row == ([],), ([],)


def test_map():
    result = parse_response(bytes_source(SIMPLE_MAP))
    check_result(result, {"key1": "value1", "key2": "value2"})
    result = parse_response(bytes_source(LOW_CARD_MAP))
    check_result(result, {"george": UUID("1d439f79-c57d-5f23-52c6-ffccca93e1a9"), "igor": None})


def test_ip():
    ips = ["192.168.5.3", "202.44.8.25", "0.0.2.2"]
    ipv4_type = registry.get_from_name("IPv4")
    dest = bytearray()
    ipv4_type.write_column(ips, dest, InsertContext("", [], []))
    python = ipv4_type.read_column_data(bytes_source(bytes(dest)), 3, QueryContext(), None)
    assert tuple(python) == tuple(IPv4Address(ip) for ip in ips)


def test_point():
    points = ((3.22, 3.22), (5.22, 5.22), (4.22, 4.22))
    point_type = registry.get_from_name("Point")
    dest = bytearray()
    point_type.write_column(points, dest, InsertContext("", [], []))
    python = point_type.read_column_data(bytes_source(bytes(dest)), 3, QueryContext(), [None, None])
    assert tuple(python) == tuple(point for point in points)


@pytest.mark.parametrize(
    ("type_name", "values"),
    [
        (
            "MultiPoint",
            [[(13.0, 23.0), (14.0, 24.0)], [], [(15.0, 25.0)]],
        ),
        (
            "Array(MultiPoint)",
            [[[(13.0, 23.0)], []], [], [[(14.0, 24.0), (15.0, 25.0)]]],
        ),
        (
            "Tuple(MultiPoint, UInt8)",
            [([(13.0, 23.0)], 7), ([], 13), ([(14.0, 24.0)], 79)],
        ),
        (
            "Array(Tuple(MultiPoint, UInt8))",
            [[([(13.0, 23.0)], 7)], [], [([], 13), ([(14.0, 24.0)], 79)]],
        ),
        (
            "Map(String, MultiPoint)",
            [
                {"first": [(13.0, 23.0)]},
                {},
                {"second": [(14.0, 24.0), (15.0, 25.0)]},
            ],
        ),
    ],
)
def test_multi_point_native_container_matrix(type_name, values):
    ch_type = registry.get_from_name(type_name)
    dest = bytearray()
    ch_type.write_column(values, dest, InsertContext("", [], []))

    source = bytes_source(bytes(dest))
    assert list(ch_type.read_column(source, len(values), QueryContext())) == values


def test_multi_point_registry_case_behavior():
    assert registry.get_from_name("MultiPoint").name == "MultiPoint"
    for spelling in ("multipoint", "MULTIPOINT"):
        with pytest.raises(InternalError, match="Unrecognized ClickHouse type base"):
            registry.get_from_name(spelling)


def test_geometry():
    tagged = [
        typed_variant([(13.0, 23.0), (14.0, 24.0)], "LineString"),
        typed_variant([[(31.0, 41.0), (32.0, 42.0)]], "MultiLineString"),
        typed_variant([[[(51.0, 61.0)]]], "MultiPolygon"),
        typed_variant((71.0, 81.0), "Point"),
        typed_variant([[(91.0, 101.0), (92.0, 102.0)]], "Polygon"),
        typed_variant([(111.0, 121.0)], "Ring"),
        typed_variant([(131.0, 141.0), (132.0, 142.0)], "MultiPoint"),
        None,
    ]
    expected = [value.value if value is not None else None for value in tagged]
    expected_body = b"".join(
        [
            struct.pack("<Q", 0),
            bytes([0, 1, 2, 3, 4, 5, 6, 255]),
            struct.pack("<Q2d2d", 2, 13.0, 14.0, 23.0, 24.0),
            struct.pack("<QQ2d2d", 1, 2, 31.0, 32.0, 41.0, 42.0),
            struct.pack("<QQQdd", 1, 1, 1, 51.0, 61.0),
            struct.pack("<dd", 71.0, 81.0),
            struct.pack("<QQ2d2d", 1, 2, 91.0, 92.0, 101.0, 102.0),
            struct.pack("<Qdd", 1, 111.0, 121.0),
            struct.pack("<Q2d2d", 2, 131.0, 132.0, 141.0, 142.0),
        ]
    )
    geometry_type = registry.get_from_name("Geometry")
    dest = bytearray()
    geometry_type.write_column(tagged, dest, InsertContext("", [], []))
    assert bytes(dest) == expected_body

    assert geometry_type.name == "Geometry"
    assert geometry_type.python_type is None
    assert geometry_type.valid_formats == ("typed", "native")
    assert registry.get_from_name("GEOMETRY").name == "Geometry"
    assert geometry_type.read_column(bytes_source(expected_body), len(tagged), QueryContext()) == expected
    assert geometry_type.read_column(bytes_source(expected_body), len(tagged), QueryContext(query_formats={"Variant": "typed"})) == expected
    assert geometry_type.read_column(bytes_source(expected_body), len(tagged), QueryContext(query_formats={"Geometry": "typed"})) == tagged


@pytest.mark.parametrize("buffer_cls", [CResponseBuffer, PyResponseBuffer], ids=["cython", "python"])
def test_geometry_unknown_discriminator_is_data_error(buffer_cls):
    geometry_type = registry.get_from_name("Geometry")
    source = bytes_source(struct.pack("<Q", 0) + b"\x07", cls=buffer_cls)
    ctx = QueryContext()
    ctx.start_column("geometry_value")

    with pytest.raises(
        DataError,
        match=(
            r"Column 'geometry_value' has Variant discriminator 7, but the type definition has 7 alternatives\. "
            r"The server sent an unknown type member or the Native stream is corrupt\."
        ),
    ):
        geometry_type.read_column(source, 1, ctx)


@pytest.mark.parametrize("buffer_cls", [CResponseBuffer, PyResponseBuffer], ids=["cython", "python"])
def test_empty_tuple_native_markers_and_stream_alignment(buffer_cls):
    tuple_type = registry.get_from_name("Tuple()")
    uint8_type = registry.get_from_name("UInt8")
    source = bytes_source(b"000" + bytes((13, 79, 80)), cls=buffer_cls)

    assert tuple_type.read_column_data(source, 3, QueryContext(), []) == ((), (), ())
    assert list(uint8_type.read_column_data(source, 3, QueryContext(), None)) == [13, 79, 80]


def test_empty_tuple_native_write_and_size():
    tuple_type = registry.get_from_name("Tuple()")
    dest = bytearray()

    tuple_type.write_column_data([(), (), ()], dest, InsertContext("", [], []))

    assert dest == b"000"
    assert tuple_type.data_size([(), (), ()]) == 1


@pytest.mark.parametrize("buffer_cls", [CResponseBuffer, PyResponseBuffer], ids=["cython", "python"])
@pytest.mark.parametrize("null_map", [b"\x01\x00\x00\x00", b"\x01" * 4, b"\x00" * 4, b""])
def test_nullable_tuple_native_null_map_and_stream_alignment(buffer_cls, null_map):
    tuple_type = registry.get_from_name("Nullable(Tuple(name Nullable(String), value UInt8))")
    values = [
        {"name": None, "value": 13},
        {"name": "S", "value": 79},
        {"name": None, "value": 80},
        {"name": "", "value": 81},
    ]
    body = b"\x01\x00\x01\x00" + b"\x00\x01S\x00\x00" + bytes((13, 79, 80, 81))
    source = bytes_source(b"\x4f" + null_map + (body if null_map else b"") + b"\x53", cls=buffer_cls)

    assert source.read_byte() == 79
    assert tuple_type.read_column(source, len(null_map), QueryContext()) == [
        None if is_null else value for is_null, value in zip(null_map, values)
    ]
    assert list(registry.get_from_name("UInt8").read_column(source, 1, QueryContext())) == [83]


@pytest.mark.parametrize("buffer_cls", [CResponseBuffer, PyResponseBuffer], ids=["cython", "python"])
def test_nullable_empty_tuple_native_markers(buffer_cls):
    tuple_type = registry.get_from_name("Nullable(Tuple())")
    source = bytes_source(b"\x01\x00\x01" + b"000" + bytes((13, 79, 80)), cls=buffer_cls)
    ctx = QueryContext()

    assert tuple_type.read_column_data(source, 3, ctx, []) == [None, (), None]
    assert list(registry.get_from_name("UInt8").read_column_data(source, 3, ctx, None)) == [13, 79, 80]

    dest = bytearray()
    tuple_type.write_column_data([None, (), None], dest, InsertContext("", [], []))
    assert dest == b"\x01\x00\x01" + b"000"
    assert tuple_type.data_size([None, (), None]) == 2
    assert tuple_type.insert_name == "Nullable(Tuple())"


@pytest.mark.parametrize(
    "type_name, values, expected",
    [
        ("Nullable(Tuple(UInt8, String))", [None, (13, "S")], b"\x01\x00\x00\x0d\x00\x01S"),
        ("Nullable(Point)", [None, (13, 79)], b"\x01\x00" + struct.pack("<4d", 0, 13, 0, 79)),
        (
            "Nullable(Tuple(value Nullable(SimpleAggregateFunction(anyLast, UInt64)), number UInt8))",
            [None, (13, 79), (None, 80)],
            b"\x01\x00\x00\x01\x00\x01" + struct.pack("<3Q", 0, 13, 0) + b"\x00\x4f\x50",
        ),
    ],
)
def test_nullable_tuple_native_write_payload(type_name, values, expected):
    dest = bytearray()
    registry.get_from_name(type_name).write_column(values, dest, InsertContext("", [], []))
    assert dest == expected


@pytest.mark.parametrize(
    "values, expected",
    [
        ([None, 13, None], b"\x01\x00\x01" + struct.pack("<3Q", 0, 13, 0)),
        ([None, None], b"\x01\x01" + bytes(16)),
    ],
)
def test_nullable_simple_aggregate_function_native_write(values, expected):
    dest = bytearray()
    registry.get_from_name("Nullable(SimpleAggregateFunction(anyLast, UInt64))").write_column(values, dest, InsertContext("", [], []))
    assert dest == expected


@pytest.mark.parametrize(
    "element_type, value",
    [
        ("UInt64", "13"),
        ("UInt64", 13.75),
        ("Int128", "79"),
        ("Decimal32(2)", decimal.Decimal("13.79")),
        ("Decimal64(2)", "13.79"),
        ("Decimal128(2)", decimal.Decimal("13.79")),
        ("Decimal256(2)", decimal.Decimal("13.79")),
        ("String", b"user_1"),
        ("FixedString(6)", b"user_1"),
        ("Date", date(2026, 10, 22)),
        ("DateTime('UTC')", datetime(2026, 10, 22, tzinfo=timezone.utc)),
        ("IPv4", IPv4Address("10.0.0.13")),
        ("UUID", UUID(int=13)),
        ("Enum8('user_1'=13)", "user_1"),
    ],
)
@pytest.mark.parametrize("all_null", [False, True], ids=["mixed", "all-null"])
def test_nullable_tuple_scalar_defaults_and_inference(element_type, value, all_null):
    ch_type = registry.get_from_name(f"Nullable(Tuple({element_type}))")
    dest = bytearray()
    values = [None, None] if all_null else [None, (value,)]
    ch_type.write_column(values, dest, InsertContext("", [], []))
    plain = bytearray()
    registry.get_from_name(element_type).write_column([value], plain, InsertContext("", [], []))
    default = b"\x00" if element_type == "String" else bytes(len(plain))
    assert dest == (b"\x01\x01" + default * 2 if all_null else b"\x01\x00" + default + plain)


@pytest.mark.parametrize("size, precision", [(32, 9), (64, 18), (128, 38), (256, 76)])
def test_decimal_alias_canonical_precision(size, precision):
    ch_type = registry.get_from_name(f"Decimal{size}(2)")
    assert ch_type.name == f"Decimal({precision}, 2)"
    assert ch_type.prec == precision


@pytest.mark.parametrize(
    "element_type, value",
    [
        ("UInt64", None),
        ("UInt64", ""),
        ("UInt64", []),
        ("Int128", None),
        ("Int128", ""),
        ("Int128", []),
        ("Decimal(9, 2)", None),
        ("Decimal(9, 2)", ""),
        ("Decimal(38, 2)", []),
        ("String", None),
        ("Array(UInt8)", None),
        ("Tuple(UInt8)", None),
        ("Tuple(UInt8)", ()),
        ("LowCardinality(String)", None),
        ("LowCardinality(UInt64)", ""),
    ],
)
def test_nullable_tuple_mask_preserves_visible_validation(element_type, value):
    ch_type = registry.get_from_name(f"Nullable(Tuple(element {element_type}))")
    ctx = InsertContext("", [], [])
    with pytest.raises((TypeError, ValueError, AttributeError, DataError, decimal.InvalidOperation)) as original:
        ch_type.write_column([{"element": value}], bytearray(), ctx)
    with pytest.raises(type(original.value)):
        ch_type.write_column([None, {"element": value}], bytearray(), ctx)


@pytest.mark.parametrize(
    "type_name, values",
    [
        ("Nullable(Tuple(element UInt64))", [None, {}]),
        ("Array(Nullable(Tuple(element UInt64)))", [[None, {}]]),
        ("Tuple(Nullable(Tuple(element UInt64)))", [(None,), ({},)]),
        ("Array(Tuple(Nullable(Tuple(element UInt64))))", [[(None,), ({},)]]),
    ],
)
def test_nullable_tuple_mask_preserves_missing_key_validation(type_name, values):
    with pytest.raises(TypeError):
        registry.get_from_name(type_name).write_column(values, bytearray(), InsertContext("", [], []))


def test_nullable_tuple_json_mask_preserves_visible_inference():
    ch_type = registry.get_from_name("Nullable(Tuple(JSON))")
    visible = [(None,), ('{"k":13}',)]
    ordinary = bytearray()
    masked = bytearray()
    ctx = InsertContext("", [], [])
    ch_type.write_column_data(visible, ordinary, ctx)
    ch_type.write_column_data([None, *visible], masked, ctx)
    assert masked == b"\x01\x00\x00\x04null" + ordinary[2:]


@pytest.mark.parametrize("value", [(13, 79, 80), 13])
def test_nullable_tuple_wrong_length_names_visible_row(value):
    with pytest.raises(DataError, match="at row 1"):
        registry.get_from_name("Nullable(Tuple(UInt64, UInt8))").write_column([None, value], bytearray(), InsertContext("", [], []))


@pytest.mark.parametrize("value", [None, 13])
def test_nonnullable_tuple_nonsequence_row_is_data_error(value):
    with pytest.raises(DataError, match=r"Tuple\(UInt64, UInt8\) expects a sequence with 2 elements at row 1"):
        registry.get_from_name("Tuple(UInt64, UInt8)").write_column([(13, 79), value], bytearray(), InsertContext("", [], []))


@pytest.mark.parametrize(
    "type_name, values, row",
    [
        ("Tuple(value Nullable(UInt8))", [{"value": 13}, None], 1),
        ("Tuple(value Nullable(UInt8))", [None, {"value": 13}], 0),
        ("Array(Tuple(value Nullable(UInt8)))", [[{"value": 13}, None]], 1),
        ("Nullable(Tuple(inner Tuple(value Nullable(UInt8))))", [None, {"inner": {"value": 13}}, {"inner": None}], 2),
    ],
)
def test_nonnullable_named_tuple_rejects_visible_none_dict_row(type_name, values, row):
    with pytest.raises(DataError, match=rf"expects a sequence with 1 elements at row {row}"):
        registry.get_from_name(type_name).write_column(values, bytearray(), InsertContext("", [], []))


def test_nullable_named_tuple_accepts_none_dict_row():
    dest = bytearray()
    registry.get_from_name("Nullable(Tuple(value Nullable(UInt8)))").write_column([{"value": 13}, None], dest, InsertContext("", [], []))
    assert dest == b"\x00\x01\x00\x01\x0d\x00"


@pytest.mark.parametrize("values", [[None], [None, (13,)]])
def test_nullable_tuple_unsupported_element_still_rejected(values):
    with pytest.raises(NotSupportedError, match="serialization.*not supported"):
        registry.get_from_name("Nullable(Tuple(AggregateFunction(sum, UInt64)))").write_column(
            values, bytearray(), InsertContext("", [], [])
        )


@pytest.mark.parametrize(
    "type_name, values, expected",
    [
        ("Array(Nullable(Tuple()))", [[None, ()], [], [()]], [[None, ()], [], [()]]),
        (
            "Tuple(Nullable(Tuple()), UInt8)",
            [(None, 13), ((), 79), (None, 80)],
            ((None, 13), ((), 79), (None, 80)),
        ),
    ],
)
@pytest.mark.parametrize("buffer_cls", [CResponseBuffer, PyResponseBuffer], ids=["cython", "python"])
def test_nullable_empty_tuple_composes_in_containers(type_name, values, expected, buffer_cls):
    ch_type = registry.get_from_name(type_name)
    dest = bytearray()
    ch_type.write_column(values, dest, InsertContext("", [], []))
    source = bytes_source(bytes(dest) + bytes((13, 79, 80)), cls=buffer_cls)

    assert ch_type.read_column(source, 3, QueryContext()) == expected
    assert list(registry.get_from_name("UInt8").read_column(source, 3, QueryContext())) == [13, 79, 80]
    assert ch_type.insert_name == type_name


@pytest.mark.parametrize(
    "type_name, values",
    [
        ("Array(UInt8)", [[13, 79], [], [80]]),
        ("Array(UInt8)", [[], [], []]),
        ("Array(Nullable(String))", [["user_1", None], [], ["user_2"]]),
        ("Array(LowCardinality(String))", [["user_1", "user_2"], [], ["user_1"]]),
        ("Array(Array(UInt8))", [[[13], []], [], [[79, 80]]]),
        ("Array(Array(Array(UInt8)))", [[[[13]], [[], [79]]], [[]], []]),
    ],
)
@pytest.mark.parametrize("buffer_cls", [CResponseBuffer, PyResponseBuffer], ids=["cython", "python"])
def test_array_native_round_trip_and_stream_alignment(type_name, values, buffer_cls):
    ch_type = registry.get_from_name(type_name)
    dest = bytearray()
    ch_type.write_column(values, dest, InsertContext("", [], []))
    source = bytes_source(bytes(dest) + bytes((13, 79, 80)), cls=buffer_cls)

    assert ch_type.read_column(source, len(values), QueryContext()) == values
    assert list(registry.get_from_name("UInt8").read_column(source, 3, QueryContext())) == [13, 79, 80]


@pytest.mark.parametrize("buffer_cls", [CResponseBuffer, PyResponseBuffer], ids=["cython", "python"])
def test_array_numpy_read_keeps_numpy_scalars(buffer_cls):
    ch_type = registry.get_from_name("Array(Array(UInt64))")
    values = [[[13, 79], []], [], [[80]]]
    dest = bytearray()
    ch_type.write_column(values, dest, InsertContext("", [], []))

    result = ch_type.read_column(bytes_source(bytes(dest), cls=buffer_cls), 3, QueryContext(use_numpy=True))
    assert result == values
    assert {type(v) for row in result for inner in row for v in inner} == {np.uint64}


@pytest.mark.parametrize(
    "type_name, value",
    [
        ("Tuple()", 13),
        ("Tuple()", (13,)),
        ("Tuple()", {}),
        ("Tuple()", []),
        ("Tuple()", None),
        ("Nullable(Tuple())", (13,)),
    ],
)
def test_empty_tuple_native_write_rejects_invalid_values(type_name, value):
    tuple_type = registry.get_from_name(type_name)

    with pytest.raises(DataError, match=r"Tuple\(\) values must be empty tuples"):
        tuple_type.write_column_data([value], bytearray(), InsertContext("", [], []))


def test_nested():
    result = parse_response(bytes_source(NESTED_BINARY))
    check_result(result, [{"str1": "one", "int32": 5}, {"str1": "two", "int32": 55}], 2, 0)


def test_first_item_first_row_empty_result():
    # An empty result set should return None rather than raising IndexError (#824).
    columns = ("id", "name")
    row_oriented = QueryResult([], column_names=columns, column_oriented=False)
    assert row_oriented.first_item is None
    assert row_oriented.first_row is None

    col_oriented = QueryResult([[], []], column_names=columns, column_oriented=True)
    assert col_oriented.first_item is None
    assert col_oriented.first_row is None


def test_first_item_first_row_non_empty_result():
    columns = ("id", "name")
    row_oriented = QueryResult([[1, "a"], [2, "b"]], column_names=columns, column_oriented=False)
    assert row_oriented.first_item == {"id": 1, "name": "a"}
    assert row_oriented.first_row == [1, "a"]
