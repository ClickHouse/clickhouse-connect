import datetime
from ipaddress import IPv4Address, IPv6Address
from uuid import UUID
from zoneinfo import ZoneInfo

import pytest

from clickhouse_connect.datatypes import dynamic
from clickhouse_connect.datatypes.dynamic import read_dynamic_prefix, read_variant_column, typed_variant
from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver.bytesource import ByteArraySource
from clickhouse_connect.driver.exceptions import DataError
from clickhouse_connect.driver.insert import InsertContext
from clickhouse_connect.driver.query import QueryContext


def test_variant_data_size():
    v_type = get_from_name("Variant(UInt8, String)")

    assert v_type.data_size([]) == 1
    assert v_type.data_size([1, 2, 3]) == 2
    assert v_type.data_size([1, "hello"]) == 4
    assert v_type.data_size([typed_variant(1, "UInt8"), typed_variant("a", "String")]) == 2


def test_variant_invalid_data_size():
    v_type = get_from_name("Variant(UInt8, Int32)")

    with pytest.raises(DataError):
        v_type.data_size(["not an int"])


def test_variant_canonical_order_drives_discriminators():
    v_type = get_from_name("Variant(UInt8, String)")

    assert v_type.name == "Variant(String, UInt8)"
    assert [element.name for element in v_type.element_types] == ["String", "UInt8"]
    assert v_type._resolve_disc("user_1") == (0, "user_1")
    assert v_type._resolve_disc(13) == (1, 13)

    dest = bytearray()
    v_type.write_column_data(["user_1", 13, None], dest, InsertContext("", [], []))
    assert dest == b"\x00\x01\xff\x06user_1\x0d"

    source = ByteArraySource(dest)
    state = dynamic.VariantState(0, [None, None])
    assert v_type.read_column_data(source, 3, QueryContext(), state) == ["user_1", 13, None]


def _json_insert_prefix(ch_type) -> bytes:
    prefix = bytearray()
    ch_type.write_column_prefix(prefix)
    return bytes(prefix)


def test_json_insert_uses_native_serialization():
    json_type = get_from_name("JSON")

    assert json_type.insert_name == "JSON"
    assert _json_insert_prefix(json_type) == b"\x01\x00\x00\x00\x00\x00\x00\x00"  # serialization version 1 (UInt64 LE)

    # The deprecated module attribute is inert, including for wrapped shapes.
    original = dynamic.json_serialization_format
    dynamic.json_serialization_format = 0
    try:
        assert json_type.insert_name == "JSON"
        assert _json_insert_prefix(json_type) == b"\x01\x00\x00\x00\x00\x00\x00\x00"
        assert get_from_name("Array(JSON)").insert_name == "Array(JSON)"
        assert get_from_name("Map(String, JSON)").insert_name == "Map(String, JSON)"
        assert get_from_name("Tuple(v JSON)").insert_name == "Tuple(`v` JSON)"
    finally:
        dynamic.json_serialization_format = original


def test_dynamic_prefix_sorts_shared_variant():
    # Prefix: struct_version=1, max_dynamic_types=1, num_variants=1, "UInt64",
    # discriminator_format=0. UInt64 and SharedVariant have no per-column prefix.
    prefix = (
        b"\x01\x00\x00\x00\x00\x00\x00\x00"  # struct_version = 1 (UInt64 LE)
        b"\x01"  # max_dynamic_types leb128 = 1
        b"\x01"  # num_variants leb128 = 1
        b"\x06UInt64"  # leb128 length 6 + type name
        b"\x00\x00\x00\x00\x00\x00\x00\x00"  # discriminator_format = 0 (UInt64 LE)
    )
    # One row, discriminator=1 pointing at UInt64 in the sorted list [SharedVariant, UInt64],
    # followed by the 8-byte UInt64 value 100.
    data = b"\x01" + b"\x64\x00\x00\x00\x00\x00\x00\x00"

    source = ByteArraySource(prefix + data)
    ctx = QueryContext()

    state = read_dynamic_prefix(None, source, ctx)
    assert [t.name for t in state.variant_types] == ["SharedVariant", "UInt64"]

    column = read_variant_column(source, 1, ctx, state.variant_types, state.variant_states)
    assert column == [100]


@pytest.mark.parametrize(
    "encoded, expected",
    [
        pytest.param(b"\x1e\x01\x02\x0d\x4f", [13, 79], id="array"),
        pytest.param(b"\x1e\x23\x01\x03\x00\x0d\x01\x00\x4f", [13, None, 79], id="array-nullable"),
        pytest.param(b"\x1f\x02\x01\x15\x0d\x06user_1", (13, "user_1"), id="tuple"),
        pytest.param(b"\x1e\x1f\x01\x01\x02\x0d\x4f", [(13,), (79,)], id="array-tuple"),
        pytest.param(b"\x1f\x00", (), id="empty-tuple"),
        pytest.param(b"\x1e\x00\x00", [], id="empty-array"),
        pytest.param(b"\x20\x01\x01n\x01\x0d", {"n": 13}, id="named-tuple"),
        pytest.param(b"\x27\x15\x01\x01\x01n\x0d", {"n": 13}, id="map"),
        pytest.param(b"\x23\x01\x00\x0d", 13, id="nullable"),
        pytest.param(b"\x23\x01\x01", None, id="nullable-null"),
        pytest.param(b"\x26\x15\x06user_1", "user_1", id="low-cardinality"),
        pytest.param(b"\x2b\x20\x01\x0d", 13, id="dynamic"),
        pytest.param(b"\x2b\x20\x00", None, id="dynamic-null"),
        pytest.param(b"\x30\x00\x00\x10\x00\x00\x00\x01\x01n\x01\x0d", {"n": 13}, id="json"),
    ],
)
def test_dynamic_shared_compound_values(encoded, expected):
    assert dynamic.decode_shared_variant_value(encoded, QueryContext()) == expected


@pytest.mark.parametrize("shape", ["scalar", "array", "tuple", "array_tuple", "nullable"])
@pytest.mark.parametrize("formatted", [False, True])
@pytest.mark.parametrize(
    "type_bytes, payload, type_name, read_format, native_value, formatted_value",
    [
        (b"\x0f", b"\x0cM", "Date", "int", datetime.date(2024, 1, 2), 19724),
        (b"\x10", (-1).to_bytes(4, "little", signed=True), "Date32", "int", datetime.date(1969, 12, 31), -1),
        (b"\x11", bytes(4), "DateTime", "int", datetime.datetime(1970, 1, 1), 0),
        (
            b"\x12\x10America/New_York",
            (1705321845).to_bytes(4, "little"),
            "DateTime",
            "int",
            datetime.datetime(2024, 1, 15, 7, 30, 45, tzinfo=ZoneInfo("America/New_York")),
            1705321845,
        ),
        (
            b"\x13\x03",
            (1705321845123).to_bytes(8, "little"),
            "DateTime64",
            "int",
            datetime.datetime(2024, 1, 15, 12, 30, 45, 123000),
            1705321845123,
        ),
        (
            b"\x14\x03\x10America/New_York",
            (1705321845123).to_bytes(8, "little"),
            "DateTime64",
            "int",
            datetime.datetime(2024, 1, 15, 7, 30, 45, 123000, tzinfo=ZoneInfo("America/New_York")),
            1705321845123,
        ),
        (
            b"\x1d",
            bytes(range(7, -1, -1)) + bytes(range(15, 7, -1)),
            "UUID",
            "string",
            UUID("00010203-0405-0607-0809-0a0b0c0d0e0f"),
            "00010203-0405-0607-0809-0a0b0c0d0e0f",
        ),
        (b"\x28", b"\x01\x02\x03\x04", "IPv4", "string", IPv4Address("4.3.2.1"), "4.3.2.1"),
        (b"\x29", IPv6Address("2001:db8::13").packed, "IPv6", "string", IPv6Address("2001:db8::13"), "2001:db8::13"),
        (b"\x31", bytes(2), "BFloat16", "native", 0.0, 0.0),
        (b"\x32", (-3723).to_bytes(4, "little", signed=True), "Time", "int", datetime.timedelta(seconds=-3723), -3723),
        (
            b"\x34\x03",
            (-3723125).to_bytes(8, "little", signed=True),
            "Time64",
            "int",
            datetime.timedelta(seconds=-3723, milliseconds=-125),
            -3723125,
        ),
    ],
    ids=["date", "date32", "datetime", "datetime-tz", "datetime64", "datetime64-tz", "uuid", "ipv4", "ipv6", "bfloat16", "time", "time64"],
)
def test_dynamic_shared_scalar_values(shape, formatted, type_bytes, payload, type_name, read_format, native_value, formatted_value):
    expected = formatted_value if formatted else native_value
    if shape == "nullable":
        type_bytes = b"\x23" + type_bytes
        payload = b"\x00" + payload
    if "tuple" in shape:
        type_bytes = b"\x1f\x01" + type_bytes
        expected = (expected,)
    if "array" in shape:
        type_bytes = b"\x1e" + type_bytes
        payload = b"\x01" + payload
        expected = [expected]
    ctx = QueryContext(query_formats={type_name.swapcase(): read_format} if formatted else None)
    assert dynamic.decode_shared_variant_value(type_bytes + payload, ctx) == expected


@pytest.mark.parametrize(
    "encoded, expected",
    [
        pytest.param(b"\x1e", b"\x1e", id="truncated-type"),
        pytest.param(b"\x1e\x01\x02\x0d", b"\x1e\x01\x02\x0d", id="truncated-array"),
        pytest.param(b"\x1e\x01\x01\x0d\xff", b"\x1e\x01\x01\x0d\xff", id="trailing-bytes"),
        pytest.param(b"\x1f\x01\x16\x01x", b"\x1f\x01\x16\x01x", id="unsupported-nested-type"),
        pytest.param(b"\x27\x15", "\x27\x15", id="printable-truncated-type"),
        pytest.param(b"\x27\x15\x01\x00z", "\x27\x15\x01\x00z", id="printable-trailing-bytes"),
        pytest.param(b"\x0f\x0c", b"\x0f\x0c", id="truncated-date"),
        pytest.param(b"\x28\x01\x02\x03", "\x28\x01\x02\x03", id="printable-truncated-scalar"),
        pytest.param(b"\x15\x05\x1e\x01\x02\x0d\x4f", "\x1e\x01\x02\x0d\x4f", id="encoded-string-collision"),
        pytest.param(b"\x01user_1", "\x01user_1", id="scalar-length-mismatch"),
        pytest.param(b"\xff", None, id="null-sentinel"),
        pytest.param(b"", "", id="empty-string"),
    ],
)
def test_dynamic_shared_compound_fallback(encoded, expected):
    assert dynamic.decode_shared_variant_value(encoded, QueryContext()) == expected


@pytest.mark.parametrize("prefix", [" ", "#", "&", "'", "+", "0", "-"])
def test_dynamic_shared_plain_string(prefix):
    value = prefix + "user_1"
    assert dynamic.decode_shared_variant_value(value.encode(), QueryContext()) == value
