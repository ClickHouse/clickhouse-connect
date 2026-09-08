import struct
from datetime import date, datetime, timedelta, timezone, tzinfo

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
