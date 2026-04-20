import pytest

from clickhouse_connect.datatypes.dynamic import read_dynamic_prefix, read_variant_column, typed_variant
from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver.bytesource import ByteArraySource
from clickhouse_connect.driver.exceptions import DataError
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
