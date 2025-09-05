import pytest

from clickhouse_connect.driver.common import get_rename_method

COL_NAME = "a.b.c d_e"


def test_none_does_nothing():
    method = get_rename_method(None)
    assert method is None


def test_remove_prefix():
    method = get_rename_method("remove_prefix")
    assert method(COL_NAME) == "c d_e"


def test_camelcase():
    method = get_rename_method("to_camelcase")
    assert method(COL_NAME) == "a.b.cDE"


def test_camelcase_without_prefix():
    method = get_rename_method("to_camelcase_without_prefix")
    assert method(COL_NAME) == "cDE"


def test_to_underscore():
    method = get_rename_method("to_underscore")
    assert method(COL_NAME) == "a.b.c_d_e"


def test_to_underscore_without_prefix():
    method = get_rename_method("to_underscore_without_prefix")
    assert method(COL_NAME) == "c_d_e"


def test_bad_option_raises():
    with pytest.raises(ValueError, match="Invalid option"):
        get_rename_method("not_an_option")
