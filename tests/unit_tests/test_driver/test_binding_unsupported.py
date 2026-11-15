from decimal import Decimal
from pathlib import Path

import numpy as np
import pytest

from clickhouse_connect.driver.binding import (
    bind_query,
    finalize_query,
    format_bind_value,
    format_query_value,
)
from clickhouse_connect.driver.exceptions import ProgrammingError


class InjectedString:
    def __str__(self):
        return "version()"


class InjectedNumber(int):
    def __str__(self):
        return "version()"


class FlippingNumber(int):
    def __str__(self):
        calls = getattr(self, "_calls", 0)
        self._calls = calls + 1
        return "1" if calls == 0 else "version()"


@pytest.mark.parametrize(
    "value",
    [Path("version()"), Path("/tmp/a b"), InjectedString(), InjectedNumber(1)],
)
def test_unsupported_top_level_values_fail_closed(value):
    with pytest.raises(ProgrammingError, match="Unsupported query parameter type"):
        format_query_value(value)
    with pytest.raises(ProgrammingError, match="Unsupported query parameter type"):
        format_bind_value(value)
    with pytest.raises(ProgrammingError, match="Unsupported query parameter type"):
        finalize_query("SELECT %s", [value])
    with pytest.raises(ProgrammingError, match="Unsupported query parameter type"):
        bind_query("SELECT {v:String}", {"v": value})


@pytest.mark.parametrize(
    "container",
    [lambda v: [v], lambda v: (v,), lambda v: {"key": v}],
    ids=["array", "tuple", "map"],
)
def test_unsupported_nested_values_fail_closed(container):
    from clickhouse_connect import common

    # In map mode, map values are serialized as SQL literals, not JSON.
    original = common.get_setting("dict_parameter_format")
    common.set_setting("dict_parameter_format", "map")
    try:
        value = container(Path("version()"))
        with pytest.raises(ProgrammingError, match="Unsupported query parameter type"):
            finalize_query("SELECT %s", [value])
        with pytest.raises(ProgrammingError, match="Unsupported query parameter type"):
            format_bind_value(value)
    finally:
        common.set_setting("dict_parameter_format", original)


@pytest.mark.parametrize(
    "value",
    [True, 0, -7, 1.25, Decimal("123.40"), np.int64(42), np.uint64(42), np.float32(1.5)],
)
def test_numbers_stay_unquoted(value):
    assert finalize_query("SELECT %s", [value]) == f"SELECT {value}"
    assert format_bind_value(value) == str(value)
    assert format_bind_value([value]) == f"[{value}]"


def test_supported_string_escaping_unchanged():
    assert finalize_query("SELECT %s", ["a'b"]) == r"SELECT 'a\'b'"
    assert format_bind_value(["a'b"]) == r"['a\'b']"


def test_numeric_subclass_is_rendered_only_once():
    # The validation call must not be followed by another __str__ that can
    # switch from a valid numeric literal to an injected SQL expression.
    assert finalize_query("SELECT %s", [FlippingNumber(1)]) == "SELECT 1"
    assert format_bind_value(FlippingNumber(1)) == "1"


def test_builtin_numeric_format_specifier_unchanged():
    assert finalize_query("SELECT %d", [7]) == "SELECT 7"
