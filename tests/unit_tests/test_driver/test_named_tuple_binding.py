from collections import UserDict

import pytest

from clickhouse_connect import common
from clickhouse_connect.driver.binding import bind_query
from clickhouse_connect.driver.exceptions import ProgrammingError

NAMED = "Tuple(protocol UInt8, port UInt16)"
VALUE = {"port": 79, "protocol": 13}

SHAPES = [
    (NAMED, VALUE, "(13, 79)"),
    ("tuple(protocol UInt8, port UInt16)", VALUE, "(13, 79)"),
    ("TuPlE(protocol UInt8, port UInt16)", VALUE, "(13, 79)"),
    ('Tuple( `proto col` UInt8, "port" UInt16)', {"port": 79, "proto col": 13}, "(13, 79)"),
    ("Tuple(`a``b` UInt8, `c\\`d` UInt16)", {"c`d": 79, "a`b": 13}, "(13, 79)"),
    (f"Array({NAMED})", [VALUE], "[(13, 79)]"),
    (f"Tuple(String, {NAMED})", ("user_1", VALUE), "('user_1', (13, 79))"),
    (f"Tuple(label String, value {NAMED})", {"value": VALUE, "label": "user_1"}, "('user_1', (13, 79))"),
    (f"Array(Tuple(String, {NAMED}))", [("user_1", VALUE)], "[('user_1', (13, 79))]"),
    (f"Nullable({NAMED})", VALUE, "(13, 79)"),
    (f"Nullable({NAMED})", None, "\\N"),
    (f"LowCardinality(Nullable({NAMED}))", VALUE, "(13, 79)"),
    (f"Array(Nullable({NAMED}))", [None, VALUE], "[NULL, (13, 79)]"),
    (f"Map(String, {NAMED})", {"user_1": VALUE}, "{'user_1':(13, 79)}"),
    (NAMED, (13, 79), "(13, 79)"),
    (NAMED, UserDict(VALUE), "(13, 79)"),
]


@pytest.mark.parametrize(("type_name", "value", "expected"), SHAPES)
@pytest.mark.parametrize("dict_format", ["json", "map"])
def test_named_tuple_server_binding(type_name, value, expected, dict_format):
    previous = common.get_setting("dict_parameter_format")
    common.set_setting("dict_parameter_format", dict_format)
    try:
        if type_name.startswith("Map") and dict_format == "json":
            expected = '{"user_1":{"port":79,"protocol":13}}'
        _, parameters = bind_query(f"SELECT {{p:{type_name}}}", {"p": value})
        assert parameters == {"param_p": expected}
    finally:
        common.set_setting("dict_parameter_format", previous)


@pytest.mark.parametrize("value", [{"protocol": 13}, {"protocol": 13, "port": 79, "other": 1}, {"other": 13, "port": 79}])
def test_named_tuple_binding_rejects_wrong_keys(value):
    with pytest.raises(ProgrammingError, match="Named Tuple fields do not match"):
        bind_query(f"SELECT {{p:{NAMED}}}", {"p": value})


def test_untyped_dictionary_binding_is_unchanged():
    _, parameters = bind_query("SELECT {p:JSON}", {"p": VALUE})
    assert parameters == {"param_p": '{"port":79,"protocol":13}'}


@pytest.mark.parametrize(
    ("query", "value", "expected"),
    [
        ("SELECT '{p:Tuple(}', {p:String}", "user_1", "user_1"),
        ("SELECT {p:Tuple(`a}b` UInt8, c UInt8)}", (1, 2), "(1, 2)"),
    ],
)
def test_unparseable_tuple_hint_keeps_value(query, value, expected):
    _, parameters = bind_query(query, {"p": value})
    assert parameters == {"param_p": expected}
