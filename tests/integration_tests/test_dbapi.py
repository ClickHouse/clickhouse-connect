from collections.abc import Callable

import pytest
from pytest import fixture

from clickhouse_connect import dbapi
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


def test_executemany_with_percent_identifiers(dbapi_connection, table_context: Callable):
    with table_context("dbapi%executemany", ["value%pct UInt32"]):
        cursor = dbapi_connection.cursor()
        cursor.executemany(
            "INSERT INTO `dbapi%%executemany` (`value%%pct`) VALUES (%s)",
            [(13,), (79,)],
        )
        cursor.execute("SELECT `value%%pct` FROM `dbapi%%executemany` ORDER BY `value%%pct`")
        assert cursor.fetchall() == [(13,), (79,)]


def test_description_null_ok_reflects_result_type(dbapi_connection):
    if not dbapi_connection.client.min_version("24.8"):
        pytest.skip("Variant and Dynamic require ClickHouse 24.8 or newer")
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
