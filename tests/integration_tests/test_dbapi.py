from collections.abc import Callable

import pytest
from pytest import fixture

from clickhouse_connect import dbapi
from clickhouse_connect.driver.exceptions import DatabaseError
from tests.integration_tests.conftest import TestConfig


@fixture(name="dbapi_connection")
def dbapi_connection_fixture(test_config: TestConfig, test_db: str):
    settings = {}
    if test_config.insert_quorum:
        settings["insert_quorum"] = test_config.insert_quorum
    elif test_config.cloud:
        settings["select_sequential_consistency"] = 1
    connection = dbapi.connect(
        host=test_config.host,
        port=test_config.port,
        username=test_config.username,
        password=test_config.password,
        database=test_db,
        compress=test_config.compress,
        **settings,
    )
    yield connection
    connection.close()


def test_executemany_with_tuple_rows(dbapi_connection, table_context: Callable):
    """Regression test: executemany with sequence rows (e.g. Airflow's
    DbApiHook.insert_rows) used to crash with AttributeError in _try_bulk_insert.
    """
    with table_context("dbapi_executemany_tuples", ["id UInt32", "name String"]):
        cursor = dbapi_connection.cursor()
        cursor.executemany(
            "INSERT INTO dbapi_executemany_tuples (id, name) VALUES (%s, %s)",
            [(13, "user_1"), (79, "user_2")],
        )
        cursor.execute("SELECT id, name FROM dbapi_executemany_tuples ORDER BY id")
        assert cursor.fetchall() == [(13, "user_1"), (79, "user_2")]


def test_executemany_with_dict_rows(dbapi_connection, table_context: Callable):
    with table_context("dbapi_executemany_dicts", ["id UInt32", "name String"]):
        cursor = dbapi_connection.cursor()
        cursor.executemany(
            "INSERT INTO dbapi_executemany_dicts (id, name) VALUES (%(id)s, %(name)s)",
            [{"id": 13, "name": "user_1"}, {"id": 79, "name": "user_2"}],
        )
        cursor.execute("SELECT id, name FROM dbapi_executemany_dicts ORDER BY id")
        assert cursor.fetchall() == [(13, "user_1"), (79, "user_2")]


def test_executemany_bare_values_with_dict_rows(dbapi_connection, table_context: Callable):
    with table_context("dbapi_executemany_bare_dicts", ["id UInt32", "name String"]):
        cursor = dbapi_connection.cursor()
        cursor.executemany(
            "INSERT INTO dbapi_executemany_bare_dicts (id, name) VALUES",
            [{"id": 13, "name": "user_1"}, {"name": "user_2", "id": 79}],
        )
        assert cursor.rowcount == 2
        cursor.execute("SELECT id, name FROM dbapi_executemany_bare_dicts ORDER BY id")
        assert cursor.fetchall() == [(13, "user_1"), (79, "user_2")]


def test_executemany_bare_values_preserves_comment_markers_in_identifier(dbapi_connection, table_context: Callable):
    with table_context("dbapi/*event--log*/", ["id UInt32"]):
        cursor = dbapi_connection.cursor()
        cursor.executemany("INSERT INTO `dbapi/*event--log*/` (id) VALUES // trailing", [(13,), (79,)])
        assert cursor.rowcount == 2
        cursor.execute("SELECT id FROM `dbapi/*event--log*/` ORDER BY id")
        assert cursor.fetchall() == [(13,), (79,)]


def test_executemany_with_percent_identifiers(dbapi_connection, table_context: Callable):
    with table_context("dbapi%executemany", ["value%pct UInt32"]):
        cursor = dbapi_connection.cursor()
        cursor.executemany(
            "INSERT INTO `dbapi%%executemany` (`value%%pct`) VALUES (%s)",
            [(13,), (79,)],
        )
        cursor.executemany("INSERT INTO `dbapi%%executemany` (`value%%pct`) VALUES", [(97,)])
        cursor.executemany("INSERT INTO `dbapi%%executemany` (`value%%pct`) VALUES", [{"value%pct": 101}])
        cursor.execute("SELECT `value%%pct` FROM `dbapi%%executemany` ORDER BY `value%%pct`")
        assert cursor.fetchall() == [(13,), (79,), (97,), (101,)]


@pytest.mark.parametrize("pyformat_encoded", [False, True])
@pytest.mark.parametrize("mapping_rows", [False, True], ids=["tuples", "mappings"])
def test_executemany_bare_values_preserves_percent_provenance(
    dbapi_connection,
    table_context: Callable,
    pyformat_encoded: bool,
    mapping_rows: bool,
):
    columns = ["value%pct UInt32", "value%%pct UInt32"]
    with (
        table_context("dbapi%bare_values", columns),
        table_context("dbapi%%bare_values", columns),
    ):
        cursor = dbapi_connection.cursor()
        column = "value%pct" if pyformat_encoded else "value%%pct"
        rows = [{column: 13}, {column: 79}] if mapping_rows else [(13,), (79,)]
        cursor.executemany(
            "INSERT INTO `dbapi%%bare_values` (`value%%pct`) VALUES",
            rows,
            pyformat_encoded=pyformat_encoded,
        )
        assert cursor.rowcount == 2
        cursor.execute(
            "SELECT `value%pct`, `value%%pct` FROM `dbapi%bare_values` ORDER BY `value%pct`",
            pyformat_encoded=False,
        )
        assert cursor.fetchall() == ([(13, 0), (79, 0)] if pyformat_encoded else [])
        cursor.execute(
            "SELECT `value%pct`, `value%%pct` FROM `dbapi%%bare_values` ORDER BY `value%%pct`",
            pyformat_encoded=False,
        )
        assert cursor.fetchall() == ([] if pyformat_encoded else [(0, 13), (0, 79)])


def test_executemany_preserves_values_expressions_and_rowcount(dbapi_connection, table_context: Callable):
    with table_context("dbapi_executemany_expr", ["raw String", "encoded String", "fixed UInt32"]):
        cursor = dbapi_connection.cursor()
        cursor.executemany(
            "INSERT INTO dbapi_executemany_expr (raw, encoded, fixed) VALUES (%s, hex(%s), 79)",
            [("user_1", "user_1"), ("user_2", "user_2")],
        )
        assert cursor.rowcount == 2
        cursor.execute("SELECT raw, encoded, fixed FROM dbapi_executemany_expr ORDER BY raw")
        assert cursor.fetchall() == [
            ("user_1", "757365725F31", 79),
            ("user_2", "757365725F32", 79),
        ]


def test_executemany_preserves_target_grammar_and_quoted_columns(
    dbapi_connection,
    table_context: Callable,
    test_db: str,
):
    with table_context("dbapi_executemany_target", ["id UInt32", "name String"]):
        cursor = dbapi_connection.cursor()
        cursor.executemany(
            f'INSERT INTO TABLE {test_db} . dbapi_executemany_target ("id", "name") VALUES (%s, %s)',
            [(13, "user_1"), (79, "user_2")],
        )
        cursor.execute("SELECT id, name FROM dbapi_executemany_target ORDER BY id")
        assert cursor.fetchall() == [(13, "user_1"), (79, "user_2")]


def test_executemany_preserves_named_bind_order_and_reuse(dbapi_connection, table_context: Callable):
    with table_context("dbapi_executemany_named", ["first UInt32", "second UInt32", "again UInt32"]):
        cursor = dbapi_connection.cursor()
        cursor.executemany(
            "INSERT INTO dbapi_executemany_named (first, second, again) VALUES (%(second)s, %(first)s, %(first)s)",
            [
                {"first": 13, "second": 79, "again": 211},
                {"second": 97, "again": 223, "first": 31},
            ],
        )
        cursor.execute("SELECT first, second, again FROM dbapi_executemany_named ORDER BY first")
        assert cursor.fetchall() == [(79, 13, 13), (97, 31, 31)]


def test_executemany_preserves_insert_select_values_alias(dbapi_connection, table_context: Callable):
    with table_context("dbapi_executemany_select", ["raw String", "encoded String"]):
        cursor = dbapi_connection.cursor()
        cursor.executemany(
            "INSERT INTO dbapi_executemany_select (raw, encoded) SELECT %(raw)s, hex(%(encoded)s) AS VALUES",
            [
                {"raw": "user_1", "encoded": "user_1"},
                {"encoded": "user_2", "raw": "user_2"},
            ],
        )
        cursor.execute("SELECT raw, encoded FROM dbapi_executemany_select ORDER BY raw")
        assert cursor.fetchall() == [("user_1", "757365725F31"), ("user_2", "757365725F32")]


def test_executemany_rejects_invalid_values_suffix(dbapi_connection, table_context: Callable):
    with table_context("dbapi_executemany_invalid", ["id UInt32"]):
        cursor = dbapi_connection.cursor()
        with pytest.raises(DatabaseError):
            cursor.executemany(
                "INSERT INTO dbapi_executemany_invalid (id) VALUESgarbage (%s)",
                [(13,), (79,)],
            )
        cursor.execute("SELECT count() FROM dbapi_executemany_invalid")
        assert cursor.fetchone() == (0,)


def test_executemany_uses_sql_coercion(dbapi_connection, table_context: Callable):
    with table_context("dbapi_executemany_coercion", ["value String"]):
        cursor = dbapi_connection.cursor()
        cursor.executemany("INSERT INTO dbapi_executemany_coercion (value) VALUES (%s)", [(13,), (79,)])
        cursor.execute("SELECT value FROM dbapi_executemany_coercion ORDER BY value")
        assert cursor.fetchall() == [("13",), ("79",)]


def test_description_null_ok_reflects_result_type(dbapi_connection):
    cursor = dbapi_connection.cursor()
    cursor.execute(
        "SELECT "
        "CAST(13, 'UInt32') AS plain, "
        "CAST(NULL, 'Nullable(String)') AS nullable, "
        "CAST(NULL, 'Variant(UInt32, String)') AS variant_null, "
        "CAST(NULL, 'Dynamic') AS dynamic_null, "
        "CAST(NULL, 'SimpleAggregateFunction(any, Nullable(UInt32))') AS aggregate_null",
        settings={
            "allow_experimental_variant_type": 1,
            "allow_experimental_dynamic_type": 1,
        },
    )

    assert cursor.fetchone() == (13, None, None, None, None)
    assert [(item[1], item[6]) for item in cursor.description] == [
        ("UInt32", False),
        ("Nullable(String)", True),
        ("Variant(String, UInt32)", True),
        ("Dynamic", True),
        ("SimpleAggregateFunction(any, Nullable(UInt32))", True),
    ]


def test_description_metadata_requery_with_leading_and_trailing_comments(dbapi_connection):
    cursor = dbapi_connection.cursor()
    cursor.execute("/* leading /* nested */ comment */ SELECT 13 AS value_1 WHERE 0 -- trailing")

    assert cursor.fetchall() == []
    assert cursor.description == []
