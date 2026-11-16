from enum import Enum, IntEnum

import pytest

from clickhouse_connect.driver.exceptions import DataError
from tests.integration_tests.conftest import nullable_tuple_settings


class EnumLabel(str, Enum):
    ACTIVE = "active"


class EnumCode(IntEnum):
    ACTIVE = 1


@pytest.mark.parametrize("enum_name", ["Enum8", "Enum16"])
@pytest.mark.parametrize(
    "wrapper, wrap_value",
    [
        ("{}", lambda value: value),
        ("Nullable({})", lambda value: value),
        ("Array({})", lambda value: [value]),
        ("Tuple({}, String)", lambda value: (value, "user_1")),
        ("Array(Tuple({}, String))", lambda value: [(value, "user_1")]),
    ],
)
@pytest.mark.parametrize("zero_member", [False, True])
@pytest.mark.parametrize("bad_label", ["Active", "actve", ""])
def test_enum_unknown_label(param_client, call, table_context, enum_name, wrapper, wrap_value, zero_member, bad_label):
    members = "'active'=1, 'inactive'=2" + (", 'missing'=0" if zero_member else "")
    enum_type = wrapper.format(f"{enum_name}({members})")
    with table_context("test_enum_unknown_label", ["status " + enum_type], order_by="tuple()"):
        with pytest.raises(DataError) as exc:
            call(param_client.insert, "test_enum_unknown_label", [[wrap_value("active")], [wrap_value(bad_label)]])
        message = str(exc.value)
        assert "status" in message
        if bad_label:
            assert bad_label in message
        else:
            assert "''" in message or '""' in message
        assert call(param_client.command, "SELECT count() FROM test_enum_unknown_label") == 0


@pytest.mark.parametrize("enum_name", ["Enum8", "Enum16"])
@pytest.mark.parametrize(
    "wrapper, wrap_value",
    [
        ("{}", lambda value: value),
        ("Nullable({})", lambda value: value),
        ("Array({})", lambda value: [value]),
        ("Tuple({}, String)", lambda value: (value, "user_1")),
        ("Array(Tuple({}, String))", lambda value: [(value, "user_1")]),
    ],
)
@pytest.mark.parametrize("numeric_first", [False, True])
def test_enum_labels_and_codes(param_client, call, table_context, enum_name, wrapper, wrap_value, numeric_first):
    values = [EnumLabel.ACTIVE, 2, "missing", 0, "", EnumCode.ACTIVE]
    expected = ["active", "inactive", "missing", "missing", "", "active"]
    if numeric_first:
        values.reverse()
        expected.reverse()
    with table_context(
        "test_enum_labels_and_codes", ["id UInt8", "status " + wrapper.format(f"{enum_name}('active'=1, 'inactive'=2, 'missing'=0, ''=13)")]
    ):
        call(param_client.insert, "test_enum_labels_and_codes", [(i, wrap_value(value)) for i, value in enumerate(values)])
        result = call(param_client.query, "SELECT status FROM test_enum_labels_and_codes ORDER BY id")
        assert result.result_rows == [(wrap_value(value),) for value in expected]


@pytest.mark.parametrize("enum_name", ["Enum8", "Enum16"])
def test_enum_unknown_code_passthrough(param_client, call, table_context, enum_name):
    with table_context("test_enum_unknown_code", [f"status {enum_name}('active'=1)"], order_by="tuple()"):
        call(param_client.insert, "test_enum_unknown_code", [[99]])
        assert call(param_client.command, "SELECT toInt16(status) FROM test_enum_unknown_code") == 99
        assert call(param_client.query, "SELECT status FROM test_enum_unknown_code").result_rows == [(None,)]


@pytest.mark.parametrize("enum_name", ["Enum8", "Enum16"])
@pytest.mark.parametrize("values", [[None, None], ["active", None], [None, "active"]])
def test_enum_nullable_none(param_client, call, table_context, enum_name, values):
    with table_context("test_enum_nullable_none", ["id UInt8", f"status Nullable({enum_name}('active'=1))"]):
        call(param_client.insert, "test_enum_nullable_none", list(enumerate(values)))
        assert call(param_client.query, "SELECT status FROM test_enum_nullable_none ORDER BY id").result_rows == [
            (value,) for value in values
        ]


@pytest.mark.parametrize("enum_name", ["Enum8", "Enum16"])
@pytest.mark.parametrize("nullable, bad_value", [(False, None), (False, []), (False, {}), (True, []), (True, {})])
def test_enum_invalid_non_string(param_client, call, table_context, test_config, enum_name, bad_value, nullable):
    enum_type = f"{enum_name}('active'=1)"
    if nullable:
        enum_type = f"Nullable({enum_type})"
    with table_context("test_enum_invalid_non_string", [f"status {enum_type}"], order_by="tuple()"):
        with pytest.raises(DataError, match="status") as exc:
            call(param_client.insert, "test_enum_invalid_non_string", [["active"], [bad_value]])
        if bad_value is None and test_config.native_codec == "python":
            assert "type mismatch" in str(exc.value)
        assert call(param_client.command, "SELECT count() FROM test_enum_invalid_non_string") == 0


@pytest.mark.parametrize("enum_name", ["Enum8", "Enum16"])
@pytest.mark.parametrize("empty_member", [False, True])
def test_enum_nullable_numeric_first_empty_label(param_client, call, table_context, enum_name, empty_member):
    members = "'active'=1" + (", ''=13" if empty_member else "")
    with table_context("test_enum_numeric_first_empty", [f"status Nullable({enum_name}({members}))"], order_by="tuple()"):
        if empty_member:
            call(param_client.insert, "test_enum_numeric_first_empty", [[1], [""]])
            assert call(param_client.query, "SELECT status FROM test_enum_numeric_first_empty").result_rows == [("active",), ("",)]
        else:
            with pytest.raises(DataError, match="status"):
                call(param_client.insert, "test_enum_numeric_first_empty", [[1], [""]])
            assert call(param_client.command, "SELECT count() FROM test_enum_numeric_first_empty") == 0


@pytest.mark.parametrize("enum_name", ["Enum8", "Enum16"])
def test_enum_nullable_tuple_hidden_payload(param_client, call, table_context, test_config, enum_name):
    if test_config.cloud:
        pytest.skip("Cloud does not allow the experimental Nullable(Tuple(...)) setting")
    settings = nullable_tuple_settings(param_client)
    with table_context(
        "test_enum_nullable_tuple", ["id UInt8", f"status Nullable(Tuple({enum_name}('active'=1), String))"], settings=settings
    ):
        values = [None, ("active", "user_1"), None]
        call(param_client.insert, "test_enum_nullable_tuple", list(enumerate(values)), settings=settings)
        assert call(param_client.query, "SELECT status FROM test_enum_nullable_tuple ORDER BY id", settings=settings).result_rows == [
            (value,) for value in values
        ]


@pytest.mark.parametrize("enum_name", ["Enum8", "Enum16"])
@pytest.mark.parametrize("missing_first", [False, True])
@pytest.mark.parametrize("zero_member", [False, True])
def test_enum_dataframe_missing(param_client, call, table_context, enum_name, missing_first, zero_member):
    pd = pytest.importorskip("pandas")
    values = [None, "active"] if missing_first else ["active", None]
    expected = ["missing" if zero_member else None, "active"] if missing_first else ["active", "missing" if zero_member else None]
    members = "'active'=1" + (", 'missing'=0" if zero_member else "")
    with table_context("test_enum_dataframe_missing", ["id UInt8", f"status {enum_name}({members})"]):
        call(param_client.insert_df, "test_enum_dataframe_missing", pd.DataFrame({"id": [0, 1], "status": values}))
        assert call(param_client.query, "SELECT status FROM test_enum_dataframe_missing ORDER BY id").result_rows == [
            (value,) for value in expected
        ]


@pytest.mark.parametrize("enum_name", ["Enum8", "Enum16"])
def test_enum_dataframe_unknown_label(param_client, call, table_context, enum_name):
    pd = pytest.importorskip("pandas")
    with table_context("test_enum_dataframe_unknown", [f"status {enum_name}('active'=1, 'missing'=0)"], order_by="tuple()"):
        with pytest.raises(DataError) as exc:
            call(param_client.insert_df, "test_enum_dataframe_unknown", pd.DataFrame({"status": ["active", "Active"]}))
        assert "status" in str(exc.value)
        assert "Active" in str(exc.value)
        assert call(param_client.command, "SELECT count() FROM test_enum_dataframe_unknown") == 0
