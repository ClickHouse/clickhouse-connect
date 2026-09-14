import struct
from collections import UserDict
from types import MappingProxyType, SimpleNamespace

import pytest

from clickhouse_connect.datatypes import dynamic
from clickhouse_connect.datatypes.base import type_map
from clickhouse_connect.datatypes.container import Map
from clickhouse_connect.datatypes.format import set_read_format
from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver.ctypes import RespBuffCls
from clickhouse_connect.driver.exceptions import DataError
from clickhouse_connect.driver.insert import InsertContext
from clickhouse_connect.driver.options import np
from clickhouse_connect.driver.query import QueryContext
from clickhouse_connect.driver.transform import NativeTransform


def test_map_pairs_format_registration():
    set_read_format("Map", "pairs")
    ctx = QueryContext(column_formats={"m": "pairs"})
    ctx.start_column("m")
    assert Map.read_format(ctx) == "pairs"
    assert dynamic.SHARED_DATA_TYPE.read_format(ctx) == "native"
    assert type_map["Map"] is Map
    assert "_SharedDataMap" not in type_map


@pytest.mark.skipif(np is None, reason="NumPy not installed")
@pytest.mark.parametrize("initial_empty_block", [False, True])
def test_map_pairs_numpy_empty_blocks(initial_empty_block):
    type_name = b"Array(Map(String, UInt8))"
    header = b"\x01m" + bytes([len(type_name)]) + type_name
    blocks = [b"\x01\x00" + header] if initial_empty_block else []
    # One row with an empty Array, then one row with an Array containing duplicate Map keys.
    blocks.append(b"\x01\x01" + header + struct.pack("<Q", 0))
    blocks.append(b"\x01\x01" + header + struct.pack("<QQ", 1, 2) + b"\x01k\x01k\x0d\x4f")
    source = RespBuffCls(SimpleNamespace(gen=iter(blocks), close=lambda: None))
    ctx = QueryContext(use_numpy=True, query_formats={"Map": "pairs"})
    result = NativeTransform.parse_response(source, ctx).np_result
    assert result.shape == (2, 1)
    assert result[:, 0].tolist() == [[], [[("k", 13), ("k", 79)]]]


@pytest.mark.parametrize("operation", ["sample", "write"])
@pytest.mark.parametrize(
    "type_name,column",
    [
        ("Map(String, UInt8)", [{}, [("k", 13)]]),
        ("Array(Map(String, UInt8))", [[{}, [("k", 13)]]]),
        ("Tuple(Map(String, UInt8))", [({},), ([("k", 13)],)]),
        ("Array(Tuple(Map(String, UInt8)))", [[({},), ([("k", 13)],)]]),
        ("Map(String, Map(String, UInt8))", [{"user_1": {}, "user_2": [("k", 13)]}]),
    ],
)
def test_map_pairs_insert_error(type_name, column, operation):
    ch_type = get_from_name(type_name)
    with pytest.raises(DataError) as exc:
        if operation == "sample":
            ch_type.data_size(column)
        else:
            ch_type.write_column_data(column, bytearray(), InsertContext("unused", [], []))
    assert str(exc.value) == (
        "Map(String, UInt8) insert values must be dictionaries, got list. Pair lists from the pairs read format are not accepted."
    )
    assert exc.value.__suppress_context__


def test_map_insert_non_sequence_error_message():
    with pytest.raises(DataError, match=r"got NoneType\.$"):
        get_from_name("Map(String, UInt8)").data_size([{}, None])


@pytest.mark.parametrize("mapping_type", [dict, UserDict, MappingProxyType])
def test_map_insert_mapping_inputs(mapping_type):
    ch_type = get_from_name("Map(String, UInt8)")
    column = [mapping_type({"k": 13})]
    assert ch_type.data_size(column) == ch_type.data_size([{"k": 13}])
    dest = bytearray()
    ch_type.write_column_data(column, dest, InsertContext("unused", [], []))
    assert dest == struct.pack("<Q", 1) + b"\x01k\x0d"


@pytest.mark.parametrize("operation", ["sample", "write"])
def test_map_insert_mapping_attribute_error(monkeypatch, operation):
    error = AttributeError("mapping failure")

    def fail():
        raise error

    value = UserDict({"k": 13})
    monkeypatch.setattr(value, "keys" if operation == "sample" else "items", fail)
    ch_type = get_from_name("Map(String, UInt8)")
    with pytest.raises(AttributeError) as exc:
        if operation == "sample":
            ch_type.data_size([value])
        else:
            ch_type.write_column_data([value], bytearray(), InsertContext("unused", [], []))
    assert exc.value is error
