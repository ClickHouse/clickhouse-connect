import logging
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.dbapi.cursor import Cursor, _NativeInsertPlan
from clickhouse_connect.driver.exceptions import DatabaseError, ProgrammingError


def create_mock_client(result_data):
    """Helper to create a mock client with query result"""
    client = Mock()
    query_result = Mock()
    query_result.result_set = result_data
    query_result.column_names = ["col1", "col2", "col3"]
    query_result.column_types = [Mock(name="String")] * 3
    query_result.summary = {"rows": len(result_data)}
    client.query.return_value = query_result
    return client


def create_mock_query_result(result_data, column_names=None, column_types=None):
    """Create a mock query result with optional metadata."""
    query_result = Mock()
    query_result.result_set = result_data
    query_result.column_names = column_names or []
    query_result.column_types = column_types or []
    query_result.summary = {"rows": len(result_data)}
    return query_result


def test_fetchall_respects_cursor_position():
    """Test that fetchall() returns only unread rows and respects cursor position"""
    test_data = [
        ("row1_col1", "row1_col2", "row1_col3"),
        ("row2_col1", "row2_col2", "row2_col3"),
        ("row3_col1", "row3_col2", "row3_col3"),
        ("row4_col1", "row4_col2", "row4_col3"),
        ("row5_col1", "row5_col2", "row5_col3"),
    ]

    client = create_mock_client(test_data)
    cursor = Cursor(client)

    # Execute a query to populate cursor data
    cursor.execute("SELECT * FROM test_table")

    # Fetch first two rows
    row1 = cursor.fetchone()
    row2 = cursor.fetchone()

    assert row1 == test_data[0]
    assert row2 == test_data[1]
    assert cursor._ix == 2  # Cursor should be at position 2

    # fetchall() should return remaining rows, not all rows
    remaining_rows = cursor.fetchall()

    # Should only get rows 3, 4, and 5 (indices 2, 3, 4)
    expected_remaining = test_data[2:]
    assert remaining_rows == expected_remaining
    assert len(remaining_rows) == 3

    # Cursor should now be at the end
    assert cursor._ix == cursor._rowcount

    # Another fetchall() should return empty list since all rows consumed
    empty_result = cursor.fetchall()
    assert empty_result == []


def test_fetchmany_respects_size_parameter():
    """Test that fetchmany() correctly handles the size parameter"""
    test_data = [
        ("row1",),
        ("row2",),
        ("row3",),
        ("row4",),
        ("row5",),
        ("row6",),
        ("row7",),
        ("row8",),
        ("row9",),
        ("row10",),
    ]

    client = create_mock_client(test_data)
    cursor = Cursor(client)
    cursor.execute("SELECT * FROM test_table")

    # Test fetchmany with explicit size
    batch1 = cursor.fetchmany(size=3)
    assert len(batch1) == 3
    assert batch1 == test_data[0:3]
    assert cursor._ix == 3

    # Test fetchmany with size larger than remaining rows
    batch2 = cursor.fetchmany(size=10)
    assert len(batch2) == 7  # Only 7 rows remaining
    assert batch2 == test_data[3:10]
    assert cursor._ix == 10

    # Test fetchmany when no rows remain
    batch3 = cursor.fetchmany(size=5)
    assert batch3 == []
    assert cursor._ix == 10


def test_fetchmany_negative_values():
    """Test fetchmany with various negative values"""
    test_data = [("row1",), ("row2",), ("row3",), ("row4",), ("row5",)]

    client = create_mock_client(test_data)
    cursor = Cursor(client)
    cursor.execute("SELECT * FROM test_table")

    # Advance cursor partway
    cursor.fetchone()  # Now at index 1

    # Any negative value should fetch all remaining
    remaining = cursor.fetchmany(-999)
    assert len(remaining) == 4
    assert remaining == test_data[1:]


def test_fetchmany_w_no_size_parameter_fetches_all_remaining():
    """Test default behavior or fetchmany"""
    test_data = [("A", 1), ("B", 2), ("C", 3), ("D", 4), ("E", 5), ("F", 6)]

    client = create_mock_client(test_data)
    cursor = Cursor(client)
    cursor.execute("SELECT * FROM test_table")

    # Fetch many (no size parameter)
    batch = cursor.fetchmany()
    assert batch == test_data

    # Reset cursor
    cursor.execute("SELECT * FROM test_table")

    # Fetch one
    row1 = cursor.fetchone()
    assert row1 == test_data[0]

    # Fetch remaining (fetchmany with no size parameter)
    batch = cursor.fetchmany()
    assert batch == test_data[1:]


def test_mixed_fetch_operations():
    """Test mixing different fetch operations"""
    test_data = [("A", 1), ("B", 2), ("C", 3), ("D", 4), ("E", 5), ("F", 6)]

    client = create_mock_client(test_data)
    cursor = Cursor(client)
    cursor.execute("SELECT * FROM test_table")

    # Fetch one
    row1 = cursor.fetchone()
    assert row1 == test_data[0]

    # Fetch many
    batch = cursor.fetchmany(2)
    assert batch == test_data[1:3]

    # Fetch all remaining
    remaining = cursor.fetchall()
    assert remaining == test_data[3:6]

    # All subsequent fetches should return empty/None
    assert cursor.fetchone() is None
    assert cursor.fetchone() is None  # Should continue returning None
    assert cursor.fetchmany(10) == []
    assert cursor.fetchall() == []


def test_cursor_reset_on_new_execute():
    """Test that cursor position resets on new execute"""
    test_data = [("row1",), ("row2",), ("row3",)]

    client = create_mock_client(test_data)
    cursor = Cursor(client)

    # First query
    cursor.execute("SELECT * FROM test_table")
    cursor.fetchmany(2)
    assert cursor._ix == 2

    # New query should reset cursor
    cursor.execute("SELECT * FROM test_table")
    assert cursor._ix == 0

    # Should be able to fetch all rows again
    all_rows = cursor.fetchall()
    assert len(all_rows) == 3
    assert all_rows == test_data


def test_check_valid():
    """Test that operations fail when cursor is not valid"""
    client = Mock()
    cursor = Cursor(client)

    # Cursor should be invalid before execute
    with pytest.raises(ProgrammingError):
        cursor.fetchone()

    with pytest.raises(ProgrammingError):
        cursor.fetchall()

    with pytest.raises(ProgrammingError):
        cursor.fetchmany()


def test_empty_result_set():
    """Test cursor behavior with empty result set"""
    client = create_mock_client([])
    cursor = Cursor(client)
    cursor.execute("SELECT * FROM empty_table")

    assert cursor.rowcount == 0
    assert cursor.fetchone() is None
    assert cursor.fetchall() == []
    assert cursor.fetchmany(5) == []


@pytest.mark.parametrize(
    "operation, rows",
    [
        (
            'INSERT INTO test_db . test_table ("id", "name") VALUES (%s, hex(%s))',
            [(13, b"user_1"), (79, b"user_2")],
        ),
        (
            "INSERT INTO test_table (id, name) SELECT %(id)s, hex(%(name)s) AS VALUES",
            [{"id": 13, "name": b"user_1"}, {"name": b"user_2", "id": 79}],
        ),
        (
            "INSERT INTO test_table (id, name) VALUESgarbage (%(id)s, %(name)s)",
            [{"id": 13, "name": "user_1"}, {"id": 79, "name": "user_2"}],
        ),
        ("INSERT INTO FUNCTION null('id UInt32') VALUES (%s)", [(13,), (79,)]),
        ("INSERT INTO TABLE FUNCTION null('id UInt32') VALUES (%s)", [(13,), (79,)]),
        ("INSERT INTO test_table\n(id)\nVALUES (%s)", [(13,), (79,)]),
        ("INSERT INTO test_table ", [(13,), (79,)]),
    ],
)
def test_executemany_preserves_operation_and_each_parameter_row(operation, rows):
    client = Mock()
    client.query.return_value = create_mock_query_result([])
    cursor = Cursor(client)

    cursor.executemany(operation, rows)

    assert [call.args for call in client.query.call_args_list] == [(operation, row) for row in rows]
    client.insert.assert_not_called()


def test_executemany_native_projects_ordered_mapping_keys_and_settings():
    client, _ = _mock_insert_client(written_rows=2)
    cursor = Cursor(client)
    plan = _NativeInsertPlan(
        "`test_db`.`test_table`",
        ("db_name", "db_id"),
        ("name", "id"),
        {"max_threads": 3},
    )
    rows = [{"id": 13, "name": "user_1"}, {"name": "user_2", "id": 79}]

    cursor._executemany_native(plan, rows)

    client.insert.assert_called_once_with(
        "`test_db`.`test_table`",
        [["user_1", 13], ["user_2", 79]],
        ("db_name", "db_id"),
        settings={"max_threads": 3},
    )
    assert cursor.rowcount == 2


@pytest.mark.parametrize(
    "rows",
    [
        [{"id": 13, "name": "user_1"}, {"id": 79}],
        [{"id": 13, "name": "user_1"}, {"id": 79, "name": "user_2", "extra": 97}],
        [{"id": 13, "name": "user_1"}, (79, "user_2")],
    ],
)
def test_executemany_native_validates_all_rows_before_insert(rows):
    client = Mock()
    cursor = Cursor(client)
    plan = _NativeInsertPlan("test_table", ("id", "name"), ("id", "name"))

    with pytest.raises(ProgrammingError, match="matching mapping parameters"):
        cursor._executemany_native(plan, rows)

    client.insert.assert_not_called()
    client.query.assert_not_called()


def test_executemany_native_validation_resets_previous_results():
    client = Mock()
    client.query.return_value = create_mock_query_result(
        [(13,)],
        column_names=["value"],
        column_types=[get_from_name("UInt32")],
    )
    cursor = Cursor(client)
    cursor.execute("SELECT 13")
    plan = _NativeInsertPlan("test_table", ("id", "name"), ("id", "name"))

    with pytest.raises(ProgrammingError, match="matching mapping parameters"):
        cursor._executemany_native(plan, [{"id": 79}])

    assert cursor.rowcount == 0
    assert cursor.description == []
    assert cursor.fetchall() == []


def test_executemany_native_rejects_non_sequence_parameters():
    client = Mock()
    cursor = Cursor(client)
    plan = _NativeInsertPlan("test_table", ("id",), ("id",))

    with pytest.raises(ProgrammingError, match="sequence of mapping parameters"):
        cursor._executemany_native(plan, {"id": 13})

    client.insert.assert_not_called()
    client.query.assert_not_called()


def test_executemany_native_dml_fetches_use_result_data_bounds():
    client, _ = _mock_insert_client(written_rows=2)
    cursor = Cursor(client)
    plan = _NativeInsertPlan("test_table", ("id",), ("id",))

    cursor._executemany_native(plan, [{"id": 13}, {"id": 79}])

    assert cursor.rowcount == 2
    assert cursor.fetchone() is None
    assert cursor.fetchmany() == []
    assert cursor.fetchall() == []


@pytest.mark.parametrize(
    "rows, expected_data",
    [
        ([(13, "user_1"), (79, "user_2")], [[13, "user_1"], [79, "user_2"]]),
        (
            [{"value%pct": "user_1", "id": 13}, {"id": 79, "value%pct": "user_2"}],
            [[13, "user_1"], [79, "user_2"]],
        ),
    ],
)
def test_executemany_bare_values_supports_sequence_and_mapping_rows(rows, expected_data):
    client, summary = _mock_insert_client(written_rows=2, summary_extra={"written_bytes": "64"})
    cursor = Cursor(client)

    cursor.executemany(
        '/* leading */ INSERT INTO TABLE `test%%db` . "events%%2026" (`id`, "value%%pct") VALUES; -- trailing',
        rows,
        settings={"max_threads": 3},
    )

    client.insert.assert_called_once_with(
        '`test%db`."events%2026"',
        expected_data,
        ("id", "value%pct"),
        settings={"max_threads": 3},
    )
    client.query.assert_not_called()
    assert cursor.rowcount == 2
    assert cursor.summary == [summary]
    assert cursor.fetchall() == []


@pytest.mark.parametrize(
    "rows, expected_columns, expected_data",
    [
        ([(13, "user_1"), (79, "user_2")], "*", [[13, "user_1"], [79, "user_2"]]),
        (
            [{"id": 13, "name": "user_1"}, {"name": "user_2", "id": 79}],
            ("id", "name"),
            [[13, "user_1"], [79, "user_2"]],
        ),
    ],
)
def test_executemany_bare_values_without_columns_uses_row_layout(rows, expected_columns, expected_data):
    client, _ = _mock_insert_client(written_rows=2)
    cursor = Cursor(client)

    cursor.executemany("INSERT INTO events VALUES", rows)

    client.insert.assert_called_once_with("events", expected_data, expected_columns, settings=None)


def test_executemany_bare_values_accepts_generator_rows():
    client, _ = _mock_insert_client(written_rows=2)
    cursor = Cursor(client)

    rows = ({"id": value} for value in (13, 79))
    cursor.executemany("INSERT INTO events (id) VALUES", rows)

    client.insert.assert_called_once_with("events", [[13], [79]], ("id",), settings=None)
    client.query.assert_not_called()


@pytest.mark.parametrize(
    "operation, expected_table, expected_columns",
    [
        ("INSERT INTO `event/*log*/` (id) VALUES", "`event/*log*/`", ("id",)),
        ("INSERT INTO `event--log` (id) VALUES", "`event--log`", ("id",)),
        ('INSERT INTO "event//log" (id) VALUES', '"event//log"', ("id",)),
        ("INSERT INTO events (`directory`.`id`) VALUES", "events", ("directory.id",)),
        ("INSERT INTO events (id) VALUES // trailing %(ignored)s", "events", ("id",)),
        ("INSERT INTO events (id) VALUES # trailing %s", "events", ("id",)),
        ("INSERT INTO events (id) VALUES -- trailing {ignored:UInt32}", "events", ("id",)),
    ],
)
def test_executemany_bare_values_preserves_quoted_comment_text_and_ignores_trailing_comments(
    operation,
    expected_table,
    expected_columns,
):
    client, _ = _mock_insert_client(written_rows=1)
    cursor = Cursor(client)

    cursor.executemany(operation, [(13,)])

    client.insert.assert_called_once_with(expected_table, [[13]], expected_columns, settings=None)
    client.query.assert_not_called()


def test_executemany_heredoc_insert_uses_sql_path():
    client = Mock()
    client.query.return_value = create_mock_query_result([])
    cursor = Cursor(client)
    operation = "INSERT INTO events (payload) VALUES ($text$line 1\nline 2$text$)"

    cursor.executemany(operation, [{}, {}])

    assert client.query.call_count == 2
    client.query.assert_any_call(operation, {}, settings=None, query_formats=None)
    client.insert.assert_not_called()


@pytest.mark.parametrize("bind_name", ["$name", "123name"])
def test_executemany_server_bind_name_uses_sql_path(bind_name):
    client = Mock()
    client.query.return_value = create_mock_query_result([])
    cursor = Cursor(client)
    operation = f"INSERT INTO events SELECT {{{bind_name}:UInt32}} AS VALUES"

    cursor.executemany(operation, [{bind_name: 13}])

    client.query.assert_called_once_with(
        operation,
        {bind_name: 13},
        settings=None,
        query_formats=None,
    )
    client.insert.assert_not_called()


@pytest.mark.parametrize(
    "rows, message",
    [
        ([{"id": 13, "name": "user_1"}, {"id": 79}], "matching mapping rows"),
        ([{"id": 13, "name": "user_1"}, (79, "user_2")], "matching mapping rows"),
        ([(13, "user_1"), (79,)], "equal-width sequence rows"),
        ([(13, "user_1"), {"id": 79, "name": "user_2"}], "equal-width sequence rows"),
    ],
)
def test_executemany_bare_values_validates_every_row_before_insert(rows, message):
    client = Mock()
    cursor = Cursor(client)

    with pytest.raises(ProgrammingError, match=message):
        cursor.executemany("INSERT INTO events (id, name) VALUES", rows)

    client.insert.assert_not_called()
    client.query.assert_not_called()


@pytest.mark.parametrize(
    "operation",
    [
        "INSERT INTO FUNCTION null('value%s UInt32') VALUES",
        "INSERT INTO events (id) VALUES /* unterminated",
        "INSERT INTO events FORMAT Values",
    ],
)
def test_executemany_unsupported_bare_values_fails_before_execution(operation):
    client = Mock()
    cursor = Cursor(client)

    with pytest.raises(ProgrammingError, match="cannot safely route this placeholder-less INSERT"):
        cursor.executemany(operation, [{"id": 13}])

    client.insert.assert_not_called()
    client.query.assert_not_called()


def test_execute_unescapes_double_percents_without_parameters():
    """Test that cursor.execute unescapes %% to % when no parameters are given.

    This is required by the PEP 249 pyformat paramstyle contract: callers
    (e.g. SQLAlchemy) escape literal percent signs as %% in the operation
    string.  When there are no parameters, the cursor must unescape them.
    See https://github.com/ClickHouse/clickhouse-connect/issues/297
    """
    client = create_mock_client([])
    cursor = Cursor(client)

    # Simulate what SQLAlchemy sends for:
    #   text("SELECT formatDateTime(toDate('2010-01-04'), '%g')")
    # with _double_percents=True (pyformat paramstyle)
    cursor.execute("SELECT formatDateTime(toDate('2010-01-04'), '%%g')")

    # The query passed to client.query should have %% unescaped to %
    actual_query = client.query.call_args[0][0]
    assert actual_query == "SELECT formatDateTime(toDate('2010-01-04'), '%g')"
    assert "%%" not in actual_query


def test_execute_preserves_percent_with_parameters():
    """Test that cursor.execute does NOT manually unescape %% when parameters
    are provided, since finalize_query handles it via Python's % operator.
    """
    client = create_mock_client([])
    cursor = Cursor(client)

    # Simulate what SQLAlchemy sends for:
    #   text("SELECT formatDateTime(toDate(:d), '%g')")
    # with _double_percents=True and bound parameter d
    cursor.execute("SELECT formatDateTime(toDate(%(d)s), '%%g')", {"d": "2010-01-04"})

    # Parameters are passed through to client.query; finalize_query handles
    # the %% -> % unescaping via the % operator during parameter substitution.
    actual_query = client.query.call_args[0][0]
    actual_params = client.query.call_args[0][1]
    assert actual_query == "SELECT formatDateTime(toDate(%(d)s), '%%g')"
    assert actual_params == {"d": "2010-01-04"}


def test_execute_unescapes_multiple_percents():
    """Test unescaping multiple %% occurrences in a single query."""
    client = create_mock_client([])
    cursor = Cursor(client)

    cursor.execute("SELECT formatDateTime(now(), '%%Y-%%m-%%d %%H:%%M:%%S')")

    actual_query = client.query.call_args[0][0]
    assert actual_query == "SELECT formatDateTime(now(), '%Y-%m-%d %H:%M:%S')"


def test_execute_preserves_raw_percents_when_not_pyformat_encoded():
    client = create_mock_client([])
    cursor = Cursor(client)

    cursor.execute("SELECT 'single% adjacent%% %(token)s tail%'", pyformat_encoded=False)

    assert client.query.call_args.args[0] == "SELECT 'single% adjacent%% %(token)s tail%'"


def test_execute_empty_result_fetches_metadata_with_parameters():
    """Empty SELECT results should still populate description metadata."""
    client = Mock()
    client.query.side_effect = [
        create_mock_query_result([]),
        create_mock_query_result(
            [],
            column_names=["value_1"],
            column_types=[SimpleNamespace(name="UInt64")],
        ),
    ]
    cursor = Cursor(client)

    cursor.execute(
        "SELECT value_1 FROM test_table WHERE value_1 = %(value_1)s LIMIT %(param_1)s",
        {"value_1": 13, "param_1": 1},
    )

    assert cursor.description == [("value_1", "UInt64", None, None, None, None, False)]
    assert client.query.call_args_list[1].args == (
        "SELECT * FROM (SELECT value_1 FROM test_table WHERE value_1 = %(value_1)s LIMIT %(param_1)s) LIMIT 0",
        {"value_1": 13, "param_1": 1},
    )


def test_execute_empty_with_query_fetches_metadata():
    """CTE queries should use the same metadata fallback."""
    client = Mock()
    client.query.side_effect = [
        create_mock_query_result([]),
        create_mock_query_result(
            [],
            column_names=["value_1"],
            column_types=[SimpleNamespace(name="UInt64")],
        ),
    ]
    cursor = Cursor(client)

    cursor.execute("WITH value_1 AS 13 SELECT value_1 WHERE value_1 = 79")

    assert cursor.description == [("value_1", "UInt64", None, None, None, None, False)]
    assert client.query.call_args_list[1].args == (
        "SELECT * FROM (WITH value_1 AS 13 SELECT value_1 WHERE value_1 = 79) LIMIT 0",
        None,
    )


@pytest.mark.parametrize(
    "type_code, expected_null_ok",
    [
        ("String", False),
        ("Nullable(Int32)", True),
        ("LowCardinality(Nullable(String))", True),
        (get_from_name("String"), False),
        (get_from_name("Nullable(String)"), True),
        ("Variant(String, UInt64)", True),
        ("Dynamic", True),
        ("SimpleAggregateFunction(any, Nullable(String))", True),
        ("SimpleAggregateFunction(any, Dynamic)", True),
        ("SimpleAggregateFunction(any, Variant(String, UInt64))", True),
        ("SimpleAggregateFunction(any, UInt64)", False),
        ("Array(Nullable(String))", False),
        ("Tuple(Nullable(String), UInt32)", False),
        ("Array(Tuple(Nullable(String), UInt32))", False),
        ("Map(String, Nullable(UInt32))", False),
        ("JSON", False),
        ("Nothing", True),
        ("Nullable(Nothing)", True),
        ("Tuple(", None),
        ("SimpleAggregateFunction(any)", None),
        ("FixedString(x)", None),
        (object(), None),
    ],
)
def test_description_null_ok_uses_type_metadata_without_changing_type_code(type_code, expected_null_ok):
    cursor = Cursor(Mock())
    cursor.names = ["value_1"]
    cursor.types = [type_code]

    description = cursor.description

    assert description[0][1] is type_code
    assert description[0][6] is expected_null_ok


@pytest.mark.parametrize(
    "type_name, expected_null_ok",
    [
        ("UInt32", False),
        ("Variant(UInt32, String)", True),
    ],
)
def test_executemany_resets_description_and_keeps_type_code_object(type_name, expected_null_ok):
    col_type = get_from_name(type_name)
    client = Mock()
    client.query.side_effect = [
        create_mock_query_result([(13,)], column_names=["old_value"], column_types=[get_from_name("UInt32")]),
        create_mock_query_result([(79,)], column_names=["value_1"], column_types=[col_type]),
    ]
    cursor = Cursor(client)
    cursor.execute("SELECT 13 AS old_value")

    cursor.executemany("SELECT %(value_1)s AS value_1", [{"value_1": 79}])

    assert cursor.description == [("value_1", col_type, None, None, None, None, expected_null_ok)]


def test_execute_empty_with_leading_comments_fetches_metadata():
    client = Mock()
    client.query.side_effect = [
        create_mock_query_result([]),
        create_mock_query_result(
            [],
            column_names=["value_1"],
            column_types=[SimpleNamespace(name="UInt64")],
        ),
    ]
    cursor = Cursor(client)
    operation = "\n/* outer /* nested */ done */\n-- line\n#! shell\n# hash\n// slash\nWITH value_1 AS 13 SELECT value_1 WHERE 0"

    cursor.execute(operation)

    assert cursor.description == [("value_1", "UInt64", None, None, None, None, False)]
    assert client.query.call_count == 2


def test_execute_empty_metadata_probe_failure_leaves_description_empty(caplog):
    client = Mock()
    client.query.side_effect = [
        create_mock_query_result([], column_names=["old_value"], column_types=[SimpleNamespace(name="UInt64")]),
        create_mock_query_result([]),
        DatabaseError("Code: 62. Syntax error"),
    ]
    cursor = Cursor(client)
    cursor.execute("SELECT 13 AS old_value")

    with caplog.at_level(logging.DEBUG, logger="clickhouse_connect.dbapi.cursor"):
        cursor.execute("/* leading */ SELECT 13 WHERE 0;\n-- trailing")

    assert cursor.description == []
    assert client.query.call_count == 3
    assert "DB-API cursor metadata probe failed; leaving description empty" in caplog.messages


def test_execute_empty_ignores_select_inside_leading_comment_for_metadata_probe():
    client = Mock()
    client.query.return_value = create_mock_query_result([])
    cursor = Cursor(client)

    cursor.execute("/* SELECT 13 /* nested */ */ OPTIMIZE TABLE tbl")

    assert cursor.description == []
    client.query.assert_called_once()


@pytest.mark.parametrize(
    "operation",
    [
        "#token\nSELECT 13",
        "#\nSELECT 13",
        "#\tcomment\nSELECT 13",
    ],
)
def test_execute_empty_does_not_treat_bare_hash_as_comment(operation):
    client = Mock()
    client.query.return_value = create_mock_query_result([])
    cursor = Cursor(client)

    cursor.execute(operation)

    assert cursor.description == []
    client.query.assert_called_once()


def test_execute_empty_metadata_probe_unexpected_error_propagates():
    client = Mock()
    client.query.side_effect = [create_mock_query_result([]), RuntimeError("unexpected failure")]
    cursor = Cursor(client)

    with pytest.raises(RuntimeError, match="unexpected failure"):
        cursor.execute("/* leading */ SELECT 13 WHERE 0")


def _mock_insert_client(written_rows: int, summary_extra: dict | None = None):
    """Return a mock client whose insert() returns a QuerySummary-like object."""
    summary_dict = {"written_rows": str(written_rows), **(summary_extra or {})}
    insert_summary = Mock()
    insert_summary.written_rows = written_rows
    insert_summary.summary = summary_dict
    client = Mock()
    client.insert.return_value = insert_summary
    return client, summary_dict


def test_executemany_insert_rowcount_aggregates_written_rows():
    client = Mock()
    first = create_mock_query_result([])
    first.summary = {"written_rows": "1"}
    second = create_mock_query_result([])
    second.summary = {"written_rows": "2"}
    client.query.side_effect = [first, second]
    cursor = Cursor(client)

    rows = [(13, "user_1"), (79, "user_2")]
    cursor.executemany("INSERT INTO test_table (id, name) VALUES (%s, %s)", rows)

    assert cursor.rowcount == 3


def test_executemany_generic_dml_fetches_use_result_data_bounds():
    client = Mock()
    result = create_mock_query_result([])
    result.summary = {"written_rows": "1"}
    client.query.return_value = result
    cursor = Cursor(client)

    cursor.executemany("INSERT INTO test_table (id) VALUES (%s)", [(13,), (79,)])

    assert cursor.rowcount == 2
    assert cursor.fetchone() is None
    assert cursor.fetchmany() == []
    assert cursor.fetchall() == []


def test_executemany_with_insert_uses_written_rows_summary():
    client = Mock()
    result = create_mock_query_result([])
    result.summary = {"written_rows": "1"}
    client.query.return_value = result
    cursor = Cursor(client)

    cursor.executemany("WITH 13 AS value INSERT INTO test_table SELECT value", [{}, {}])

    assert cursor.rowcount == 2


@pytest.mark.parametrize(
    "operation, expected_rowcount",
    [
        ("WITH 13 AS value INSERT INTO test_table SELECT value", -1),
        ("WITH 13 AS value SELECT value WHERE 0", 0),
    ],
)
def test_executemany_with_prefix_without_written_rows_uses_statement_type(operation, expected_rowcount):
    client = Mock()
    client.query.return_value = create_mock_query_result([])
    cursor = Cursor(client)

    cursor.executemany(operation, [{}, {}])

    assert cursor.rowcount == expected_rowcount


def test_executemany_insert_rowcount_is_unknown_without_written_rows():
    client = Mock()
    client.query.return_value = create_mock_query_result([])
    cursor = Cursor(client)

    cursor.executemany("INSERT INTO test_table (id) VALUES (%s)", [(13,), (79,)])

    assert cursor.rowcount == -1


def test_executemany_result_rowcount_counts_returned_rows():
    client = Mock()
    client.query.side_effect = [
        create_mock_query_result([(13,)], ["value"], [get_from_name("UInt32")]),
        create_mock_query_result([(79,)], ["value"], [get_from_name("UInt32")]),
    ]
    cursor = Cursor(client)

    cursor.executemany("SELECT %(value)s AS value", [{"value": 13}, {"value": 79}])

    assert cursor.rowcount == 2
    assert cursor.fetchall() == [(13,), (79,)]


def test_executemany_generator_falls_through_to_row_by_row():
    """A generator passed to executemany falls through to the row-by-row path without raising TypeError."""
    client = Mock()
    client.query.return_value = create_mock_query_result([])

    cursor = Cursor(client)

    def row_generator():
        yield {"id": 1, "name": "user_1"}
        yield {"id": 2, "name": "user_2"}

    cursor.executemany("INSERT INTO test_table (id, name) VALUES (%(id)s, %(name)s)", row_generator())

    client.insert.assert_not_called()
    assert client.query.call_count == 2
