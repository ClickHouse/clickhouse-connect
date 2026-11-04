from datetime import datetime

import pytest

from clickhouse_connect import common
from clickhouse_connect.driver.exceptions import ProgrammingError
from tests.integration_tests.conftest import nullable_tuple_settings

NAMED = "Tuple(protocol UInt8, port UInt16)"
VALUE = {"port": 79, "protocol": 13}


@pytest.mark.parametrize(
    ("type_name", "value", "expected"),
    [
        (NAMED, VALUE, (13, 79)),
        ("Tuple(`proto col` UInt8, `a\\`b` UInt16)", {"a`b": 79, "proto col": 13}, (13, 79)),
        (f"Array({NAMED})", [VALUE], [(13, 79)]),
        (f"Tuple(String, {NAMED})", ("user_1", VALUE), ("user_1", (13, 79))),
        (f"Array(Tuple(String, {NAMED}))", [("user_1", VALUE)], [("user_1", (13, 79))]),
        (f"Nullable({NAMED})", VALUE, (13, 79)),
        (f"Nullable({NAMED})", None, None),
        (f"Array(Nullable({NAMED}))", [None, VALUE], [None, (13, 79)]),
        (f"Map(String, {NAMED})", {"user_1": VALUE}, {"user_1": (13, 79)}),
        (
            "Tuple(stamp DateTime64(6, 'UTC'), protocol UInt8)",
            {"protocol": 13, "stamp": datetime(2024, 6, 13, 7, 8, 9, 123456)},
            (datetime(2024, 6, 13, 7, 8, 9, 123456), 13),
        ),
        (NAMED, (13, 79), (13, 79)),
    ],
)
@pytest.mark.parametrize("dict_format", ["json", "map"])
def test_named_tuple_parameters(param_client, call, type_name, value, expected, dict_format):
    if type_name.startswith("Map") and dict_format == "json":
        pytest.skip("Map parameters require dict_parameter_format='map'")
    settings = nullable_tuple_settings(param_client) if "Nullable(Tuple" in type_name else {}
    previous = common.get_setting("dict_parameter_format")
    common.set_setting("dict_parameter_format", dict_format)
    try:
        result = call(
            param_client.query, f"SELECT {{p:{type_name}}}", parameters={"p": value}, settings=settings, query_formats={"Tuple": "tuple"}
        )
        assert result.first_row[0] == expected
    finally:
        common.set_setting("dict_parameter_format", previous)


def test_bind_named_tuple_read_result(param_client, call):
    value = call(param_client.query, "SELECT CAST((13, 79), 'Tuple(protocol UInt8, port UInt16)')").first_row[0]
    assert value == {"protocol": 13, "port": 79}
    assert call(param_client.query, f"SELECT {{p:{NAMED}}}", parameters={"p": value}).first_row[0] == value


@pytest.mark.parametrize("value", [{"protocol": 13}, {**VALUE, "other": 1}])
def test_named_tuple_invalid_fields(param_client, call, value):
    with pytest.raises(ProgrammingError, match="Named Tuple fields do not match"):
        call(param_client.query, f"SELECT {{p:{NAMED}}}", parameters={"p": value})
