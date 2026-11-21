import struct

import pytest
from clickhouse_connect.driverc.buffer import ResponseBuffer as CResponseBuffer

from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver._backend.httpcommon import embed_insert_query
from clickhouse_connect.driver.binding import statement_for_wire
from clickhouse_connect.driver.buffer import ResponseBuffer as PyResponseBuffer
from clickhouse_connect.driver.bytesource import ByteArraySource
from clickhouse_connect.driver.exceptions import ProgrammingError
from clickhouse_connect.driver.insert import InsertContext
from clickhouse_connect.driver.transform import NativeTransform
from tests.helpers import bytes_source

parse_response = NativeTransform().parse_response

BUFFERS = pytest.mark.parametrize("cls", [CResponseBuffer, PyResponseBuffer], ids=["cython", "python"])


def _lp(value: bytes) -> bytes:
    """A LEB128 length prefix, for values under 128 bytes, followed by the value"""
    return bytes([len(value)]) + value


def _block(name: bytes, type_name: bytes, rows: int, data: bytes) -> bytes:
    return bytes([1, rows]) + _lp(name) + _lp(type_name) + data


def _read(cls, block: bytes):
    return parse_response(bytes_source(block, cls=cls))


@BUFFERS
@pytest.mark.parametrize(
    "type_name,data,expected",
    [
        (b"Enum8('\xff' = 1, 'other' = 2)", b"\x02", [("other",)]),
        (b"Enum16('\xff' = 1, 'other' = 2)", struct.pack("<H", 2), [("other",)]),
        (b"Enum8('caf\xe9' = 1, 'other' = 2)", b"\x02", [("other",)]),
        (b"Nullable(Enum8('\xff' = 1, 'other' = 2))", b"\x00\x01" + b"\x02\x01", [("other",), (None,)]),
        (b"Array(Enum8('\xff' = 1, 'other' = 2))", struct.pack("<Q", 2) + b"\x02\x02", [(["other", "other"],)]),
    ],
)
def test_query_reads_enum_with_label_that_is_not_utf8(cls, type_name, data, expected):
    rows = len(expected)
    result = _read(cls, _block(b"e", type_name, rows, data))

    assert result.column_names == ("e",)
    assert result.result_set == expected


@BUFFERS
def test_row_that_uses_the_non_utf8_label_keeps_its_bytes(cls):
    result = _read(cls, _block(b"e", b"Enum8('\xff' = 1, 'other' = 2)", 1, b"\x01"))

    label = result.result_set[0][0]
    assert label == "\udcff"
    assert label.encode("utf-8", "surrogateescape") == b"\xff"


@BUFFERS
def test_query_reads_column_name_that_is_not_utf8(cls):
    result = _read(cls, _block(b"a\xff", b"UInt8", 1, b"\x07"))

    assert result.column_names == ("a\udcff",)
    assert result.result_set == [(7,)]


@BUFFERS
def test_valid_utf8_names_decode_as_before(cls):
    result = _read(cls, _block("caf\u00e9 \u2603".encode(), "Enum8('\u00e9\u2603' = 1, 'b' = 2)".encode(), 1, b"\x01"))

    assert result.column_names == ("caf\u00e9 \u2603",)
    assert result.result_set == [("\u00e9\u2603",)]


@BUFFERS
def test_string_data_that_is_not_utf8_keeps_the_hex_fallback(cls):
    result = _read(cls, _block(b"s", b"String", 1, _lp(b"\xffA")))

    assert result.result_set == [("ff41",)]


def test_type_name_with_non_utf8_label_round_trips_to_the_server_bytes():
    ch_type = get_from_name("Enum8('\udcff' = 1, 'other' = 2)")

    assert ch_type.insert_name == r"Enum8('\xff' = 1, 'other' = 2)"


def test_byte_array_source_reads_names_that_are_not_utf8():
    src = ByteArraySource(_lp(b"a\xffb") + _lp("\u00e9".encode()))

    assert src.read_leb128_str() == "a\udcffb"
    assert src.read_leb128_str() == "\u00e9"


def test_insert_writes_a_column_name_that_is_not_utf8_as_its_original_bytes():
    context = InsertContext("t", ["a\udcff"], [get_from_name("UInt8")], [[7]], column_oriented=True)

    payload = b"".join(NativeTransform.build_insert(context))

    assert payload.startswith(b"INSERT INTO t (`a\xff`) FORMAT Native\n")
    assert _lp(b"a\xff") + _lp(b"UInt8") in payload


def test_insert_writes_valid_utf8_column_names_as_before():
    context = InsertContext("t", ["caf\u00e9"], [get_from_name("UInt8")], [[7]], column_oriented=True)

    payload = b"".join(NativeTransform.build_insert(context))

    assert payload.startswith("INSERT INTO t (`caf\u00e9`) FORMAT Native\n".encode())
    assert _lp("caf\u00e9".encode()) + _lp(b"UInt8") in payload


def test_statement_for_wire_leaves_text_that_encodes_alone():
    assert statement_for_wire("INSERT INTO t (`caf\u00e9`)") == "INSERT INTO t (`caf\u00e9`)"


def test_statement_for_wire_gives_back_the_original_bytes_of_a_non_utf8_name():
    assert statement_for_wire("INSERT INTO t (`a\udcff`)") == b"INSERT INTO t (`a\xff`)"


def test_raw_insert_of_a_text_block_with_a_non_utf8_column_name_is_sent_as_bytes():
    body, query_param = embed_insert_query("t", ["a\udcff"], "CSV", None, "1,\u00e9\n")

    assert body == b"INSERT INTO t (`a\xff`) FORMAT CSV\n1,\xc3\xa9\n"
    assert query_param is None


def test_raw_insert_of_a_bytes_block_with_a_non_utf8_column_name_is_sent_as_bytes():
    body, query_param = embed_insert_query("t", ["a\udcff"], "CSV", None, b"1\n")

    assert body == b"INSERT INTO t (`a\xff`) FORMAT CSV\n1\n"
    assert query_param is None


def test_raw_insert_of_a_text_block_keeps_text_when_the_names_encode():
    body, query_param = embed_insert_query("t", ["caf\u00e9"], "CSV", None, "1\n")

    assert body == "INSERT INTO t (`caf\u00e9`) FORMAT CSV\n1\n"
    assert query_param is None


@pytest.mark.parametrize("compression", ["lz4", "gzip"])
def test_raw_insert_with_a_non_utf8_column_name_rejects_a_statement_that_must_be_a_url_parameter(compression):
    with pytest.raises(ProgrammingError, match="not valid UTF-8"):
        embed_insert_query("t", ["a\udcff"], "CSV", compression, b"1\n")
    with pytest.raises(ProgrammingError, match="not valid UTF-8"):
        embed_insert_query("t", ["a\udcff"], "CSV", None, iter([b"1\n"]))


def test_raw_insert_statement_for_encodable_names_stays_a_url_parameter_when_streamed():
    body, query_param = embed_insert_query("t", ["a"], "CSV", "gzip", b"1\n")

    assert body == b"1\n"
    assert query_param == "INSERT INTO t (`a`) FORMAT CSV"
