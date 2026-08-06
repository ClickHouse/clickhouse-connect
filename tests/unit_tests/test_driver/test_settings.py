import pytest

from clickhouse_connect import common
from clickhouse_connect.driver.exceptions import ProgrammingError


def test_setting():
    try:
        assert common.get_setting("autogenerate_session_id")
        common.set_setting("autogenerate_session_id", False)
        assert common.get_setting("autogenerate_session_id") is False
    finally:
        common.set_setting("autogenerate_session_id", True)


def test_naive_datetime_binding_setting():
    original = common.get_setting("naive_datetime_binding")
    try:
        assert original == "wall"
        common.set_setting("naive_datetime_binding", "legacy")
        assert common.get_setting("naive_datetime_binding") == "legacy"
        common.set_setting("naive_datetime_binding", "wall")
        assert common.get_setting("naive_datetime_binding") == "wall"
        with pytest.raises(ProgrammingError, match="Unrecognized option"):
            common.set_setting("naive_datetime_binding", "other")
    finally:
        common.set_setting("naive_datetime_binding", original)
