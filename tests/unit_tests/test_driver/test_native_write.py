import struct
from datetime import date, datetime, timedelta, timezone, tzinfo
from decimal import Decimal, localcontext
from uuid import UUID

import pytest

from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver.exceptions import DataError, ProgrammingError
from clickhouse_connect.driver.insert import InsertContext
from clickhouse_connect.driver.query import QueryContext
from tests.helpers import bytes_source, native_insert_block, to_bytes
from tests.unit_tests.test_driver.binary import NESTED_BINARY


class NoOffsetTZ(tzinfo):
    def utcoffset(self, dt):
        return None

    def dst(self, dt):
        return None


LOW_CARD_OUTPUT = """
0101 0576 616c 7565 204c 6f77 4361 7264
696e 616c 6974 7928 4e75 6c6c 6162 6c65
2853 7472 696e 6729 2901 0000 0000 0000
0000 0600 0000 0000 0002 0000 0000 0000
0000 0574 6872 6565 0100 0000 0000 0000
01
"""

TUPLE_ONE_OUTPUT = """
0101 0576 616c 7565 3854 7570 6c65 2853 
7472 696e 672c 2046 6c6f 6174 3332 2c20  
4c6f 7743 6172 6469 6e61 6c69 7479 284e  
756c 6c61 626c 6528 5374 7269 6e67 2929 
2901 0000 0000 0000 0007 7374 7269 6e67 
317b 144e 4000 0600 0000 0000 0001 0000  
0000 0000 0000 0100 0000 0000 0000 00  
"""

TUPLE_THREE_OUTPUT = """
0103 0576 616c 7565 0d54 7570 6c65 2853
7472 696e 6729 0773 7472 696e 6731 0773
7472 696e 6732 0773 7472 696e 6733
"""

POINT_OUTPUT = """
0101 0576 616c 7565 0550 6f69 6e74 c3f5
285c 8fc2 0940 c3f5 285c 8fc2 0940
"""

STRING_ACCEPTS_BYTES_OUTPUT = """
0101 0576 616c 7565 0653 7472 696e 6701
ff
"""

MAP_LOW_CARDINALITY_OUTPUT = """
0102 034D 4150 234D 6170 284C 6F77 4361
7264 696E 616C 6974 7928 5374 7269 6E67
292C 2053 7472 696E 6729 0100 0000 0000
0000 0200 0000 0000 0000 0200 0000 0000
0000 0006 0000 0000 0000 0200 0000 0000
0000 046B 6579 3104 6B65 7932 0200 0000
0000 0000 0001 0131 0374 776F
"""

LOW_CARDINALITY_NULLABLE_OUTPUT = """
0102 0373 7472 204C 6F77 4361 7264 696E
616C 6974 7928 4E75 6C6C 6162 6C65 2853
7472 696E 6729 2901 0000 0000 0000 0000
0600 0000 0000 0002 0000 0000 0000 0000
0566 6972 7374 0200 0000 0000 0000 0100
"""


def test_low_card_null():
    data = [["three"]]
    names = ["value"]
    types = [get_from_name("LowCardinality(Nullable(String))")]
    output = native_insert_block(data, names, types)
    assert bytes(output) == to_bytes(LOW_CARD_OUTPUT)


def test_tuple_one():
    data = [[("string1", 3.22, None)]]
    names = ["value"]
    types = [get_from_name("Tuple(String, Float32, LowCardinality(Nullable(String)))")]
    output = native_insert_block(data, names, types)
    assert bytes(output) == bytes.fromhex(TUPLE_ONE_OUTPUT)


def test_tuple_three():
    data = [[("string1",)], [("string2",)], [("string3",)]]
    names = ["value"]
    types = [get_from_name("Tuple(String)")]
    output = native_insert_block(data, names, types)
    assert bytes(output) == bytes.fromhex(TUPLE_THREE_OUTPUT)


def test_point():
    data = [[(3.22, 3.22)]]
    names = ["value"]
    types = [get_from_name("Point")]
    output = native_insert_block(data, names, types)
    assert bytes(output) == bytes.fromhex(POINT_OUTPUT)


@pytest.mark.parametrize(
    ("points", "message"),
    [
        ([(13.0,)], "got 1 at row 0"),
        ([(13.0, 23.0, 79.0)], "got 3 at row 0"),
        ([(13.0, 23.0), (79.0,)], "got 1 at row 1"),
        ([(13.0, 23.0), (13.0, 23.0, 79.0)], "got 3 at row 1"),
    ],
)
def test_point_rejects_wrong_coordinate_count(points, message):
    point_type = get_from_name("Point")
    context = InsertContext("table", ["value"], [point_type], [[(13.0, 23.0)]])
    with pytest.raises(DataError, match=message):
        point_type.write_column(points, bytearray(), context)


def test_nested():
    data = [
        ([],),
        ([{"str1": "three", "int32": 5}, {"str1": "five", "int32": 77}],),
        ([{"str1": "one", "int32": 5}, {"str1": "two", "int32": 55}],),
        ([{"str1": "one", "int32": 5}, {"str1": "two", "int32": 55}],),
    ]
    types = [get_from_name("Nested(str1 String, int32 UInt32)")]
    output = native_insert_block(data, ["nested"], types)
    assert bytes(output) == bytes.fromhex(NESTED_BINARY)


def test_string_accepts_bytes():
    data = [[bytes.fromhex("ff")]]
    names = ["value"]
    types = [get_from_name("String")]
    output = native_insert_block(data, names, types)
    assert bytes(output) == bytes.fromhex(STRING_ACCEPTS_BYTES_OUTPUT)


def test_long_str():
    x = (
        "蹝ㅝǅ잍鞏≈ﬞ㉢嫩杻⤧㛕錍к❭䦳텶샖爤㍅䱃䰅ἐ䤖엋㰾멛蹒뀃쩷섡፳聣᮵峧쒝咋觀હ鷁䯕͢퐠㏈猡칆빃밥뜼৫葘鹯勲掾ᬗ罧炼䏦險ヤⴕ懺릨봟죩ᬨ칰ԁ凢"
        + "䰚娞祃獿휢듕鞜甲뉛⠆ᗫ䐼詠圂ᱞ出裒ਗ਼ᩜ㉤扷ꑐ晏镄焬㞧ノⷶ枆侪㇉摨⒞펦埏穊僛䦃吹ꗣ麥䔲鸈麡┨࣓ꢫႮﬆᝢ妢曢ꗠᆪ擽烣졀씥⣏便꽉슕盈㪃拪풻ᯖ럐峨"
        + "箻躰䆲⏂錬횬渪㜟첯鋘ꊩ㾝톶╁茒牾붮뚂О灪噚놾蠂쌇龥䁼"
    )
    data = [[x]]
    names = ["value"]
    types = [get_from_name("String")]
    output = native_insert_block(data, names, types)
    assert bytes(output) == b"\x01\x01\x05value\x06String\xe7\x03" + x.encode()


@pytest.mark.parametrize(
    "type_name, values, expected",
    [
        ("Decimal(9, 2)", ["1.999", "1234567.899", "-1234567.899", "9999999.999"], [199, 123456789, -123456789, 999999999]),
        ("Decimal32(2)", ["9999999.999", "-9999999.999"], [999999999, -999999999]),
        ("Decimal64(2)", ["1234567890123456.899"], [123456789012345689]),
        ("Nullable(Decimal(9, 2))", [None, "1234567.899"], [0, 123456789]),
        ("Decimal(38, 2)", ["1" * 36 + ".899"], [int("1" * 36 + "89")]),
        ("Decimal128(2)", ["1" * 36 + ".899"], [int("1" * 36 + "89")]),
        ("Nullable(Decimal128(2))", [None, "1" * 36 + ".899"], [0, int("1" * 36 + "89")]),
        ("Decimal256(2)", ["-" + "1" * 74 + ".899"], [-int("1" * 74 + "89")]),
        ("Nullable(Decimal256(2))", [None, "1" * 74 + ".899"], [0, int("1" * 74 + "89")]),
    ],
)
@pytest.mark.parametrize("input_type", [Decimal, str])
def test_decimal_truncates_extra_digits(type_name, values, expected, input_type):
    ch_type = get_from_name(type_name)
    dest = bytearray()
    ch_type.write_column([input_type(v) if v is not None else None for v in values], dest, InsertContext("table", ["value"], [ch_type]))
    sz = ch_type.byte_size
    written = [int.from_bytes(dest[i : i + sz], "little", signed=True) for i in range(len(dest) - sz * len(values), len(dest), sz)]
    assert written == expected


@pytest.mark.parametrize("type_name", ["Decimal(9, 2)", "Decimal32(2)", "Nullable(Decimal32(2))"])
def test_decimal_truncates_float_extra_digits(type_name):
    ch_type = get_from_name(type_name)
    dest = bytearray()
    values = [1.999, 1234567.899, -1234567.899, 9999999.999]
    ch_type.write_column(values, dest, InsertContext("table", ["value"], [ch_type]))
    nulls = bytes(len(values)) if ch_type.nullable else b""
    assert dest == nulls + struct.pack("<4i", 199, 123456789, -123456789, 999999999)


@pytest.mark.parametrize("type_name", ["Decimal(9, 2)", "Decimal(38, 2)", "Nullable(Decimal256(2))"])
def test_decimal_write_clamped_context(type_name):
    ch_type = get_from_name(type_name)
    dest = bytearray()
    with localcontext() as outer:
        outer.clamp = 1
        ch_type.write_column([Decimal("13.799")], dest, InsertContext("table", ["value"], [ch_type]))
        assert outer.clamp == 1
    assert int.from_bytes(dest[-ch_type.byte_size :], "little", signed=True) == 1379


@pytest.mark.parametrize("type_name", ["FixedString(4)", "Nullable(FixedString(4))"])
@pytest.mark.parametrize("encoding, value", [("utf-8", "ab\ud800"), ("latin-1", "ab\u20ac")])
def test_fixed_string_rejects_unencodable_str(type_name, encoding, value):
    ch_type = get_from_name(type_name)
    ctx = InsertContext("table", ["value"], [ch_type])
    ctx.encoding = encoding
    with pytest.raises(UnicodeEncodeError):
        ch_type.write_column(["ok", value], bytearray(), ctx)


def test_low_card_map():
    data = [[{"key1": "1", "key2": "two"}], [{}]]
    names = ["MAP"]
    types = [get_from_name("Map(LowCardinality(String), String)")]
    output = native_insert_block(data, names, types)
    assert bytes(output) == bytes.fromhex(MAP_LOW_CARDINALITY_OUTPUT)


def test_low_card_nullable():
    data = [["first"], [None]]
    names = ["str"]
    types = [get_from_name("LowCardinality(Nullable(String))")]
    output = native_insert_block(data, names, types)
    assert bytes(output) == bytes.fromhex(LOW_CARDINALITY_NULLABLE_OUTPUT)


def test_bad_columns():
    data = [["str"], [3.5]]
    names = ["value"]
    types = [get_from_name("String")]
    try:
        native_insert_block(data, names, types)
    except TypeError:
        pass

    data = [[3.5], [str]]
    names = ["value"]
    types = [get_from_name("Float64")]

    try:
        native_insert_block(data, names, types)
    except ProgrammingError:
        pass


@pytest.mark.parametrize("type_name, code", [("Date", "H"), ("Date32", "i")])
@pytest.mark.parametrize("nullable", [False, True])
@pytest.mark.parametrize(
    "values, days",
    [
        pytest.param([date(1970, 1, 14), date(1970, 3, 21)], [13, 79], id="dates"),
        pytest.param([datetime(1970, 1, 14), datetime(1970, 3, 21, 23, 59, 59, 999999)], [13, 79], id="naive"),
        pytest.param(
            [
                datetime(1970, 1, 14, 0, 30, tzinfo=timezone(timedelta(hours=14))),
                datetime(1970, 3, 21, 23, 30, tzinfo=timezone(timedelta(hours=-12))),
                datetime(1970, 1, 14, 12, 30, tzinfo=timezone.utc),
            ],
            [13, 79, 13],
            id="aware",
        ),
        pytest.param(
            [date(1970, 1, 14), datetime(1970, 3, 21), datetime(1970, 1, 14, tzinfo=timezone.utc)],
            [13, 79, 13],
            id="date-first",
        ),
        pytest.param(
            [datetime(1970, 1, 14, tzinfo=timezone.utc), date(1970, 3, 21), datetime(1970, 1, 14)],
            [13, 79, 13],
            id="datetime-first",
        ),
    ],
)
def test_date_native_calendar_days(type_name, code, nullable, values, days):
    ch_type = get_from_name(f"Nullable({type_name})" if nullable else type_name)
    ctx = InsertContext("table", ["value"], [ch_type], server_tz=timezone(timedelta(hours=-12)))
    if nullable:
        values = [None, *values, None]
        days = [0, *days, 0]
    dest = bytearray()
    ch_type.write_column(values, dest, ctx)

    nulls = bytes([1, *([0] * (len(values) - 2)), 1]) if nullable else b""
    assert dest == nulls + struct.pack(f"<{len(days)}{code}", *days)


@pytest.mark.parametrize("type_name, code", [("Date", "H"), ("Date32", "i")])
@pytest.mark.parametrize("nullable", [False, True])
@pytest.mark.parametrize("write_format", ["native", "int"])
def test_date_native_integer_days(type_name, code, nullable, write_format):
    ch_type = get_from_name(f"Nullable({type_name})" if nullable else type_name)
    ctx = InsertContext("table", ["value"], [ch_type], query_formats={type_name: write_format})
    values = [0, 13, 79]
    if nullable:
        values = [None, *values, None]
    dest = bytearray()
    ch_type.write_column(values, dest, ctx)

    nulls = b"\x01\x00\x00\x00\x01" if nullable else b""
    days = [0, 0, 13, 79, 0] if nullable else values
    assert dest == nulls + struct.pack(f"<{len(days)}{code}", *days)
    if nullable:
        dest.clear()
        ch_type.write_column([None, None], dest, ctx)
        assert dest == b"\x01\x01" + struct.pack(f"<2{code}", 0, 0)


def test_date32_native_before_epoch():
    ch_type = get_from_name("Date32")
    values = [date(1900, 1, 1), datetime(1969, 12, 31, 23, 59, 59, tzinfo=timezone(timedelta(hours=-12)))]
    dest = bytearray()
    ch_type.write_column(values, dest, InsertContext("table", ["value"], [ch_type]))

    assert dest == struct.pack("<2i", -25567, -1)


@pytest.mark.parametrize("type_name", ["Date", "Date32"])
@pytest.mark.parametrize("nullable", [False, True])
def test_date_low_cardinality_preserves_calendar_days(type_name, nullable):
    inner_type = f"Nullable({type_name})" if nullable else type_name
    ch_type = get_from_name(f"LowCardinality({inner_type})")
    values = [
        datetime(2024, 1, 1, 0, 30, tzinfo=timezone(timedelta(hours=14))),
        datetime(2023, 12, 31, 10, 30, tzinfo=timezone.utc),
    ]
    expected = [date(2024, 1, 1), date(2023, 12, 31)]
    assert values[0] == values[1]
    if nullable:
        values = [None, *values, None]
        expected = [None, *expected, None]
    dest = bytearray()
    ch_type.write_column(values, dest, InsertContext("table", ["value"], [ch_type]))

    assert ch_type.read_column(bytes_source(bytes(dest)), len(values), QueryContext()) == expected


@pytest.mark.parametrize("type_name", ["Date", "Date32"])
@pytest.mark.parametrize("nullable", [False, True])
def test_date_low_cardinality_none_offset_matches_naive(type_name, nullable):
    inner_type = f"Nullable({type_name})" if nullable else type_name
    ch_type = get_from_name(f"LowCardinality({inner_type})")
    naive = [datetime(2024, 1, 1), datetime(2024, 1, 1, 12), datetime(2024, 1, 1)]
    values = [x.replace(tzinfo=NoOffsetTZ()) for x in naive]
    if nullable:
        naive = [None, *naive, None]
        values = [None, *values, None]
    ctx = InsertContext("table", ["value"], [ch_type])
    expected = bytearray()
    ch_type.write_column(naive, expected, ctx)
    dest = bytearray()
    ch_type.write_column(values, dest, ctx)

    assert dest == expected


@pytest.mark.parametrize(
    "type_name, values, error",
    [
        ("Date", [date(1969, 12, 31)], DataError),
        ("Date", [date(2149, 6, 7)], DataError),
        ("Date32", [2**31], DataError),
        ("Date32", [-(2**31) - 1], DataError),
        ("Date", [None], TypeError),
        ("Date32", [None], TypeError),
        ("Date", ["2024-01-01"], TypeError),
        ("Date32", ["2024-01-01"], TypeError),
        ("Date", [13.5], TypeError),
        ("Nullable(Date32)", [None, "2024-01-01"], TypeError),
    ],
)
def test_date_native_invalid_values(type_name, values, error):
    ch_type = get_from_name(type_name)
    with pytest.raises(error) as caught:
        ch_type.write_column(values, bytearray(), InsertContext("table", ["value"], [ch_type]))
    if error is TypeError:
        assert caught.value.__context__ is None


@pytest.mark.parametrize("type_name", ["Date", "Nullable(Date32)"])
@pytest.mark.parametrize(
    "values",
    [
        [datetime(2024, 1, 1, tzinfo=timezone.utc), "invalid"],
        [date(2024, 1, 1), datetime(2024, 1, 1), "invalid"],
        [datetime(2024, 1, 1), date(2024, 1, 1), "invalid"],
    ],
)
def test_date_native_invalid_value_after_valid_dates(type_name, values):
    ch_type = get_from_name(type_name)
    with pytest.raises(TypeError, match="'str'") as caught:
        ch_type.write_column(values, bytearray(), InsertContext("table", ["value"], [ch_type]))
    assert caught.value.__context__ is None


@pytest.mark.parametrize("type_name", ["Date", "Date32", "Nullable(Date32)", "LowCardinality(Date32)"])
def test_date_native_rejects_pandas_nat(type_name):
    pd = pytest.importorskip("pandas")
    ch_type = get_from_name(type_name)
    with pytest.raises(DataError):
        ch_type.write_column([pd.NaT], bytearray(), InsertContext("table", ["value"], [ch_type]))


@pytest.mark.parametrize("type_name", ["Date", "Nullable(Date32)", "LowCardinality(Date)", "LowCardinality(Nullable(Date32))"])
def test_date_native_none_offset_with_pandas_nat(type_name):
    pd = pytest.importorskip("pandas")
    ch_type = get_from_name(type_name)
    values = [datetime(2024, 1, 1, tzinfo=NoOffsetTZ()), pd.NaT]
    with pytest.raises(DataError):
        ch_type.write_column(values, bytearray(), InsertContext("table", ["value"], [ch_type]))


@pytest.mark.parametrize("type_name", ["Date", "Nullable(Date32)"])
def test_date_native_aware_datetime_with_pandas_nat(type_name):
    pd = pytest.importorskip("pandas")
    ch_type = get_from_name(type_name)
    values = [datetime(2024, 1, 1, tzinfo=timezone.utc), pd.NaT]
    with pytest.raises(DataError):
        ch_type.write_column(values, bytearray(), InsertContext("table", ["value"], [ch_type]))


@pytest.mark.parametrize("type_name, code", [("Date", "H"), ("Date32", "i")])
@pytest.mark.parametrize("error", [AttributeError, ValueError])
def test_date_native_subclass_without_ordinal(type_name, code, error):
    class LegacyDate(date):
        def toordinal(self):
            raise error("ordinal unavailable")

    ch_type = get_from_name(type_name)
    dest = bytearray()
    ch_type.write_column([LegacyDate(1970, 1, 14)], dest, InsertContext("table", ["value"], [ch_type]))

    assert dest == struct.pack(f"<{code}", 13)


@pytest.mark.parametrize(
    "type_name, code",
    [("Int8", "b"), ("UInt8", "B"), ("Int16", "h"), ("UInt16", "H"), ("Int32", "i"), ("UInt32", "I"), ("Int64", "q"), ("UInt64", "Q")],
)
@pytest.mark.parametrize("nullable", [False, True])
@pytest.mark.parametrize(
    "representation",
    ["python", "float16-list", "float32-list", "float64-list", "float16-array", "float32-array", "float64-array", "object-array"],
)
def test_integer_float_native_bytes(type_name, code, nullable, representation):
    values = [13.75, 79.25]
    if representation != "python":
        np = pytest.importorskip("numpy")
        dtype, container = representation.split("-")
        values = np.array(values, dtype=dtype)
        if container == "list":
            values = list(values)
    if nullable:
        type_name = f"Nullable({type_name})"
        if representation.endswith("-array"):
            values = pytest.importorskip("numpy").array([None, *values, None], dtype=object)
        else:
            values = [None, *values, None]
    ch_type = get_from_name(type_name)
    ctx = InsertContext("table", ["value"], [ch_type])
    dest = bytearray()
    ch_type.write_column(values, dest, ctx)

    expected = [0, 13, 79, 0] if nullable else [13, 79]
    nulls = b"\x01\x00\x00\x01" if nullable else b""
    assert dest == nulls + struct.pack(f"<{len(expected)}{code}", *expected)


@pytest.mark.parametrize("type_name", ["Int8", "UInt8", "Int16", "UInt16", "Int32", "UInt32", "Int64", "UInt64"])
@pytest.mark.parametrize("nullable", [False, True])
@pytest.mark.parametrize("value", ["nan", "inf", "-inf"])
@pytest.mark.parametrize(
    "representation",
    ["python", "float16-list", "float32-list", "float64-list", "float16-array", "float32-array", "float64-array", "object-array"],
)
def test_integer_native_rejects_nonfinite(type_name, nullable, value, representation):
    values = [13.0, float(value)]
    if representation != "python":
        np = pytest.importorskip("numpy")
        dtype, container = representation.split("-")
        values = np.array(values, dtype=dtype)
        if container == "list":
            values = list(values)
    if nullable:
        type_name = f"Nullable({type_name})"
    ch_type = get_from_name(type_name)
    ctx = InsertContext("table", ["value"], [ch_type])
    ctx.column_name = "value"
    with pytest.raises(DataError, match=f"value.*{value} is not finite"):
        ch_type.write_column(values, bytearray(), ctx)


@pytest.mark.parametrize(
    "type_name",
    [
        "Int32",
        "Nullable(Int32)",
        "Array(Int32)",
        "Array(Nullable(Int32))",
        "Tuple(Int32, String)",
        "Tuple(Nullable(Int32), String)",
        "Array(Tuple(Int32, String))",
        "Array(Tuple(Nullable(Int32), String))",
        "Map(String, Int32)",
        "Map(String, Nullable(Int32))",
        "LowCardinality(Int32)",
        "LowCardinality(Nullable(Int32))",
        "SimpleAggregateFunction(sum, Nullable(Int32))",
        "IntervalSecond",
    ],
)
@pytest.mark.parametrize("value", ["nan", "inf", "-inf"])
def test_integer_native_rejects_nested_nonfinite(type_name, value):
    value = float(value)
    if "Tuple(" in type_name:
        value = (value, "user_1")
    if type_name.startswith("Array("):
        value = [value]
    elif type_name.startswith("Map("):
        value = {"user_1": value}
    ch_type = get_from_name(type_name)
    ctx = InsertContext("table", ["value"], [ch_type])
    with pytest.raises(DataError):
        ch_type.write_column([value], bytearray(), ctx)


@pytest.mark.parametrize("values", [[13.0, None], [None, 13.0], [13, None], [None], ["13", None]])
def test_nonnullable_integer_native_rejects_none(values):
    ch_type = get_from_name("Int32")
    with pytest.raises(DataError):
        ch_type.write_column(values, bytearray(), InsertContext("table", ["value"], [ch_type]))


@pytest.mark.parametrize("nullable", [False, True])
def test_integer_float_conversion_preserves_other_errors(nullable):
    ch_type = get_from_name("Nullable(Int32)" if nullable else "Int32")
    ctx = InsertContext("table", ["value"], [ch_type])
    with pytest.raises(DataError, match="out of range"):
        ch_type.write_column([13.0, 10**400], bytearray(), ctx)
    with pytest.raises(TypeError):
        ch_type.write_column([13.0, object()], bytearray(), ctx)


@pytest.mark.parametrize("value", ["", [], {}])
def test_nullable_integer_float_column_rejects_falsy_values(value):
    ch_type = get_from_name("Nullable(Int32)")
    with pytest.raises((TypeError, ValueError)):
        ch_type.write_column([13.0, value], bytearray(), InsertContext("table", ["value"], [ch_type]))


UUID_VALUES = [
    UUID("1d439f79-c57d-5f23-52c6-ffccca93e1a9"),
    UUID("00000000-0000-0000-0000-000000000000"),
    UUID("ffffffff-ffff-ffff-ffff-ffffffffffff"),
    UUID("00000000-0000-0001-8000-000000000000"),
]


def _uuid_wire(value: UUID | None) -> bytes:
    # ClickHouse stores a UUID as two little-endian UInt64 values, high half first
    x = value.int if value is not None else 0
    return struct.pack("<QQ", x >> 64, x & 0xFFFFFFFFFFFFFFFF)


@pytest.mark.parametrize(
    "values",
    [
        UUID_VALUES,
        [str(u) for u in UUID_VALUES],
        [str(u).upper() for u in UUID_VALUES],
        [u.hex for u in UUID_VALUES],
        [u.int for u in UUID_VALUES],
    ],
    ids=["uuid", "str", "str_upper", "str_no_dashes", "int"],
)
def test_uuid_native_write(values):
    ch_type = get_from_name("UUID")
    dest = bytearray()
    ch_type.write_column(values, dest, InsertContext("table", ["value"], [ch_type]))

    assert bytes(dest) == b"".join(_uuid_wire(u) for u in UUID_VALUES)
    assert list(ch_type.read_column(bytes_source(bytes(dest)), len(values), QueryContext())) == UUID_VALUES


@pytest.mark.parametrize("type_name", ["Nullable(UUID)", "LowCardinality(Nullable(UUID))"])
@pytest.mark.parametrize(
    "values, expected",
    [
        ([UUID_VALUES[0], None, UUID_VALUES[2]], [UUID_VALUES[0], None, UUID_VALUES[2]]),
        ([None, str(UUID_VALUES[0]), None], [None, UUID_VALUES[0], None]),
        ([None, UUID_VALUES[2].int], [None, UUID_VALUES[2]]),
        ([None, None], [None, None]),
    ],
    ids=["uuid", "str_leading_none", "int_leading_none", "all_none"],
)
def test_uuid_native_write_nullable(type_name, values, expected):
    ch_type = get_from_name(type_name)
    dest = bytearray()
    ch_type.write_column(values, dest, InsertContext("table", ["value"], [ch_type]))

    read = ch_type.read_column(bytes_source(bytes(dest)), len(values), QueryContext())
    assert list(read) == expected


def test_uuid_native_write_nullable_wire():
    ch_type = get_from_name("Nullable(UUID)")
    dest = bytearray()
    ch_type.write_column([UUID_VALUES[0], None, UUID_VALUES[2]], dest, InsertContext("table", ["value"], [ch_type]))

    expected = b"\x00\x01\x00" + _uuid_wire(UUID_VALUES[0]) + _uuid_wire(None) + _uuid_wire(UUID_VALUES[2])
    assert bytes(dest) == expected


@pytest.mark.parametrize("convert", [lambda u: u, str, lambda u: u.int], ids=["uuid", "str", "int"])
@pytest.mark.parametrize(
    "type_name, build, expected",
    [
        ("Array(UUID)", lambda vals: [vals[:2], [], vals[2:]], [UUID_VALUES[:2], [], UUID_VALUES[2:]]),
        ("Tuple(UUID, UUID)", lambda vals: [(vals[0], vals[1]), (vals[2], vals[3])], [UUID_VALUES[:2], UUID_VALUES[2:]]),
        (
            "Array(Tuple(UUID, String))",
            lambda vals: [[(vals[0], "user_1"), (vals[1], "user_2")], []],
            [[[UUID_VALUES[0], "user_1"], [UUID_VALUES[1], "user_2"]], []],
        ),
    ],
    ids=["array", "tuple", "array_tuple"],
)
def test_uuid_native_write_composed(convert, type_name, build, expected):
    ch_type = get_from_name(type_name)
    column = build([convert(u) for u in UUID_VALUES])
    dest = bytearray()
    ch_type.write_column(column, dest, InsertContext("table", ["value"], [ch_type]))

    read = ch_type.read_column(bytes_source(bytes(dest)), len(column), QueryContext())

    def as_lists(value):
        return [as_lists(v) for v in value] if isinstance(value, (list, tuple)) else value

    assert as_lists(read) == expected


@pytest.mark.parametrize(
    "value, expected",
    [
        ("1234", struct.pack("<QQ", 0, 0x1234)),
        ("0" + UUID_VALUES[0].hex, _uuid_wire(UUID_VALUES[0])),
        (0, _uuid_wire(None)),
        ("", _uuid_wire(None)),
    ],
    ids=["short_hex", "zero_prefixed_hex", "int_zero", "empty_str"],
)
def test_uuid_native_write_lenient_values(value, expected):
    ch_type = get_from_name("UUID")
    dest = bytearray()
    ch_type.write_column([value], dest, InsertContext("table", ["value"], [ch_type]))

    assert bytes(dest) == expected


@pytest.mark.parametrize(
    "column, error",
    [
        ([-1], OverflowError),
        ([2**128], OverflowError),
        (["not-a-uuid"], ValueError),
        (["1" + "0" * 32], OverflowError),
        ([13, "user_1"], TypeError),
    ],
    ids=["negative_int", "int_too_large", "non_hex_str", "hex_too_large", "int_then_str"],
)
def test_uuid_native_write_invalid_values(column, error):
    ch_type = get_from_name("UUID")
    with pytest.raises(error):
        ch_type.write_column(column, bytearray(), InsertContext("table", ["value"], [ch_type]))


UUID_COLUMN_SHAPES = pytest.mark.parametrize(
    "type_name, build",
    [
        ("UUID", lambda vals: vals),
        ("Nullable(UUID)", lambda vals: vals),
        ("Array(UUID)", lambda vals: [vals]),
        ("Tuple(UUID, String)", lambda vals: [(v, "user_1") for v in vals]),
    ],
    ids=["scalar", "nullable", "array", "tuple"],
)


@pytest.mark.parametrize("convert", [bytes, bytearray, memoryview], ids=["bytes", "bytearray", "memoryview"])
def test_uuid_native_write_binary(convert):
    ch_type = get_from_name("UUID")
    dest = bytearray()
    ch_type.write_column([convert(u.bytes) for u in UUID_VALUES] + [b""], dest, InsertContext("table", ["value"], [ch_type]))

    assert bytes(dest) == b"".join(_uuid_wire(u) for u in UUID_VALUES) + _uuid_wire(None)


@UUID_COLUMN_SHAPES
@pytest.mark.parametrize(
    "value",
    [b"\x01" * 15, b"\x01" * 17, b"\x01" * 32, bytearray(b"\x01" * 31), memoryview(b"\x01")],
    ids=["short", "long", "two_uuids", "bytearray", "memoryview"],
)
def test_uuid_native_write_rejects_binary_of_wrong_length(type_name, build, value):
    # A UUID is 16 bytes on the wire, so any other length moves every value that follows in the block
    ch_type = get_from_name(type_name)
    column = build([UUID_VALUES[0].bytes, value, UUID_VALUES[2].bytes])
    with pytest.raises(DataError, match=f"UUID bytes length is {len(value)}, expected 16"):
        ch_type.write_column(column, bytearray(), InsertContext("table", ["value"], [ch_type]))


@pytest.mark.parametrize(
    "convert",
    [lambda u: u, str, lambda u: u.int, lambda u: u.bytes],
    ids=["uuid", "str", "int", "bytes"],
)
def test_uuid_native_write_leading_none(convert):
    # None in a non-nullable column is the zero UUID. Its position must not change the other rows
    ch_type = get_from_name("UUID")
    dest = bytearray()
    column = [None, convert(UUID_VALUES[0]), None, convert(UUID_VALUES[2])]
    ch_type.write_column(column, dest, InsertContext("table", ["value"], [ch_type]))

    assert bytes(dest) == b"".join(_uuid_wire(u) for u in (None, UUID_VALUES[0], None, UUID_VALUES[2]))


@pytest.mark.parametrize("value", [None, float("nan"), 1.5], ids=["none", "nan", "float"])
def test_uuid_native_write_no_supported_value(value):
    ch_type = get_from_name("UUID")
    dest = bytearray()
    ch_type.write_column([value, value], dest, InsertContext("table", ["value"], [ch_type]))

    assert bytes(dest) == _uuid_wire(None) * 2


@UUID_COLUMN_SHAPES
@pytest.mark.parametrize("value", [float("nan"), 1.5, [13, 79], {"id": 13}], ids=["nan", "float", "list", "dict"])
def test_uuid_native_write_unsupported_first_value(type_name, build, value):
    # The value raises as it does in any other row. It must not turn the UUIDs after it into zero UUIDs
    ch_type = get_from_name(type_name)
    column = build([value, UUID_VALUES[0], UUID_VALUES[2]])
    with pytest.raises(AttributeError):
        ch_type.write_column(column, bytearray(), InsertContext("table", ["value"], [ch_type]))
