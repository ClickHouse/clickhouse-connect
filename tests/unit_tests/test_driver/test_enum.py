import struct

import pytest

from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver.exceptions import DataError
from clickhouse_connect.driver.insert import InsertContext


@pytest.mark.parametrize("enum_name", ["Enum8", "Enum16"])
@pytest.mark.parametrize("bad_label", ["Active", "actve", ""])
def test_unknown_enum_label_suppresses_key_error(enum_name, bad_label):
    enum_type = get_from_name(f"{enum_name}('active'=1)")
    ctx = InsertContext("unused", ["status"], [enum_type])
    ctx.start_column("status")
    with pytest.raises(DataError) as exc:
        enum_type.write_column_data(["active", bad_label], bytearray(), ctx)
    assert "status" in str(exc.value)
    assert repr(bad_label) in str(exc.value)
    assert exc.value.__cause__ is None
    assert exc.value.__suppress_context__


@pytest.mark.parametrize("enum_name, array_code", [("Enum8", "b"), ("Enum16", "h")])
@pytest.mark.parametrize(
    "nullable, column, codes",
    [
        (False, [1, 2], [1, 2]),
        (False, ["active", "inactive"], [1, 2]),
        (False, ["active", 2], [1, 2]),
        (False, [1, "inactive"], [1, 2]),
        (False, [1, ""], [1, 13]),
        (False, ["", 2], [13, 2]),
        (True, [None, 1, "inactive"], [0, 1, 2]),
        (True, [None, "active", 2], [0, 1, 2]),
        (True, [None, "active", "active", "active"], [0, 1, 1, 1]),
        (True, ["active", None, "inactive"], [1, 0, 2]),
        (True, [1, None, 2], [1, 0, 2]),
        (True, [None, None], [0, 0]),
    ],
)
def test_enum_fast_paths_and_mixed_fallback(enum_name, array_code, nullable, column, codes):
    type_name = f"{enum_name}('active'=1, 'inactive'=2, ''=13)"
    enum_type = get_from_name(f"Nullable({type_name})" if nullable else type_name)
    ctx = InsertContext("unused", ["status"], [enum_type])
    ctx.start_column("status")
    dest = bytearray(b"prefix")
    enum_type._write_column_binary(column, dest, ctx)
    assert dest == b"prefix" + struct.pack(f"<{len(codes)}{array_code}", *codes)


@pytest.mark.parametrize("enum_name", ["Enum8", "Enum16"])
@pytest.mark.parametrize("first", [1, "active"])
@pytest.mark.parametrize("bad_value", ["Active", [], {}])
@pytest.mark.parametrize("nullable", [False, True])
def test_enum_fallback_error_preserves_destination(enum_name, first, bad_value, nullable):
    type_name = f"{enum_name}('active'=1)"
    enum_type = get_from_name(f"Nullable({type_name})" if nullable else type_name)
    ctx = InsertContext("unused", ["status"], [enum_type])
    ctx.start_column("status")
    column = [None, first, bad_value] if nullable else [first, bad_value]
    dest = bytearray(b"prefix")
    with pytest.raises(DataError, match="status") as exc:
        enum_type._write_column_binary(column, dest, ctx)
    if isinstance(bad_value, str):
        assert repr(bad_value) in str(exc.value)
        assert exc.value.__suppress_context__
    assert dest == b"prefix"


@pytest.mark.parametrize("enum_name", ["Enum8", "Enum16"])
@pytest.mark.parametrize("nullable", [False, True])
def test_enum_write_lookup_preserves_name_map(enum_name, nullable):
    type_name = f"{enum_name}('active'=1, ''=13)"
    enum_type = get_from_name(f"Nullable({type_name})" if nullable else type_name)
    assert enum_type._name_map == {"active": 1, "": 13}
    assert enum_type._write_name_map == ({"active": 1, "": 13, None: 0} if nullable else enum_type._name_map)
    if not nullable:
        assert enum_type._write_name_map is enum_type._name_map
