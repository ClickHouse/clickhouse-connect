import pytest
from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.datatypes.dynamic import typed_variant
from clickhouse_connect.driver.exceptions import DataError

def test_variant_data_size():
    v_type = get_from_name('Variant(UInt8, String)')

    assert v_type.data_size([]) == 1
    assert v_type.data_size([1, 2, 3]) == 2
    assert v_type.data_size([1, "hello"]) == 4
    assert v_type.data_size([typed_variant(1, 'UInt8'), typed_variant("a", 'String')]) == 2

def test_variant_invalid_data_size():
    v_type = get_from_name('Variant(UInt8, Int32)')

    with pytest.raises(DataError):
        v_type.data_size(["not an int"])
