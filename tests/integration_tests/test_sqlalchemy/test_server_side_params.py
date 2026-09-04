"""End-to-end SQLAlchemy parameter and multi-row INSERT tests."""

from collections.abc import Iterator

import pytest
from pytest import fixture
from sqlalchemy import Integer, MetaData, Table, bindparam, func, insert, inspect, select, text, tuple_
from sqlalchemy.engine import Engine, create_engine

from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import Int32
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import String as ChString
from clickhouse_connect.driver.binding import format_str
from tests.integration_tests.conftest import TestConfig
from tests.integration_tests.test_sqlalchemy.conftest import verify_tables_ready

TABLE = "server_side_params_test"


@fixture(scope="module", name="server_side_engine")
def server_side_engine_fixture(test_config: TestConfig) -> Iterator[Engine]:
    conn_str = (
        f"clickhousedb://{test_config.username}:{test_config.password}@{test_config.host}:"
        f"{test_config.port}/{test_config.test_database}?ca_cert=certifi"
    )
    if test_config.cloud:
        conn_str += "&select_sequential_consistency=1"
    engine = create_engine(conn_str, server_side_params=True)
    yield engine
    engine.dispose()


@fixture(scope="module", autouse=True, name="ssp_table")
def ssp_table_fixture(test_engine: Engine, server_side_engine: Engine, test_db: str):
    with test_engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {test_db}.{TABLE}"))
        conn.execute(
            text(
                f"""
            CREATE TABLE {test_db}.{TABLE} (
                id UInt32,
                name String
            ) ENGINE MergeTree() ORDER BY id
        """
            )
        )
        conn.execute(text(f"INSERT INTO {test_db}.{TABLE} (id, name) VALUES (13, 'user_1'), (79, 'user_2'), (5, 'O''Brien')"))
        verify_tables_ready(conn, {f"{test_db}.{TABLE}": 3})

    # Autoload through the server-side engine to also cover reflection in this mode.
    md = MetaData(schema=test_db)
    tbl = Table(TABLE, md, autoload_with=server_side_engine)
    yield tbl
    with test_engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {test_db}.{TABLE}"))


def _ids(engine, stmt):
    with engine.connect() as conn:
        return sorted(row[0] for row in conn.execute(stmt))


def test_scalar_where(server_side_engine: Engine, ssp_table: Table):
    stmt = select(ssp_table.c.id).where(ssp_table.c.name == "user_1")
    assert _ids(server_side_engine, stmt) == [13]


@pytest.mark.parametrize("name", ["id$x", "$x", "id$", "a$$b", "$1", "13_"])
def test_dollar_bind_names(server_side_engine: Engine, name: str):
    stmt = select(bindparam(name, value=13, type_=Integer()))
    with server_side_engine.connect() as conn:
        assert conn.execute(stmt).scalar_one() == 13


def test_scalar_matches_client_side(server_side_engine: Engine, test_engine: Engine, ssp_table: Table):
    stmt = select(ssp_table.c.id).where(ssp_table.c.id > 5).where(ssp_table.c.id < 79)
    assert _ids(server_side_engine, stmt) == _ids(test_engine, stmt)


def test_in_clause(server_side_engine: Engine, ssp_table: Table):
    stmt = select(ssp_table.c.id).where(ssp_table.c.id.in_([13, 5, 999]))
    assert _ids(server_side_engine, stmt) == [5, 13]


def test_not_in_clause(server_side_engine: Engine, ssp_table: Table):
    stmt = select(ssp_table.c.id).where(ssp_table.c.id.notin_([13, 79]))
    assert _ids(server_side_engine, stmt) == [5]


def test_empty_in_clause(server_side_engine: Engine, ssp_table: Table):
    stmt = select(ssp_table.c.id).where(ssp_table.c.id.in_([]))
    assert _ids(server_side_engine, stmt) == []


def test_tuple_in_clause(server_side_engine: Engine, ssp_table: Table):
    stmt = select(ssp_table.c.id).where(tuple_(ssp_table.c.id, ssp_table.c.name).in_([(13, "user_1"), (79, "nope")]))
    assert _ids(server_side_engine, stmt) == [13]


def test_string_value_with_quote(server_side_engine: Engine, ssp_table: Table):
    stmt = select(ssp_table.c.id).where(ssp_table.c.name == "O'Brien")
    assert _ids(server_side_engine, stmt) == [5]


def test_limit_offset(server_side_engine: Engine, ssp_table: Table):
    stmt = select(ssp_table.c.id).order_by(ssp_table.c.id).limit(1).offset(1)
    assert _ids(server_side_engine, stmt) == [13]


def test_insert_then_select(server_side_engine: Engine, ssp_table: Table):
    with server_side_engine.begin() as conn:
        conn.execute(insert(ssp_table).values(id=21, name="user_3"))
    stmt = select(ssp_table.c.name).where(ssp_table.c.id == 21)
    with server_side_engine.connect() as conn:
        assert [row[0] for row in conn.execute(stmt)] == ["user_3"]


@pytest.mark.parametrize(
    ("server_side", "first_id"),
    [(False, 101), (True, 111)],
    ids=["client-side", "server-side"],
)
def test_multivalues_insert(
    server_side: bool,
    first_id: int,
    test_engine: Engine,
    server_side_engine: Engine,
    ssp_table: Table,
):
    engine = server_side_engine if server_side else test_engine
    statements = [
        insert(ssp_table).values(
            [{"id": first_id, "name": "dict_1"}, {"id": first_id + 1, "name": "dict_2"}],
        ),
        insert(ssp_table).values([(first_id + 2, "tuple_1"), (first_id + 3, "tuple_2")]),
        insert(ssp_table).values(
            [
                {"id": first_id + 4, "name": func.lower("EXPRESSION_1")},
                {"id": first_id + 5, "name": func.lower("EXPRESSION_2")},
            ],
        ),
    ]

    with engine.begin() as conn:
        for statement in statements:
            conn.execute(statement)
        rows = conn.execute(
            select(ssp_table.c.id, ssp_table.c.name)
            .where(ssp_table.c.id >= first_id)
            .where(ssp_table.c.id < first_id + 6)
            .order_by(ssp_table.c.id),
        ).all()

    assert rows == [
        (first_id, "dict_1"),
        (first_id + 1, "dict_2"),
        (first_id + 2, "tuple_1"),
        (first_id + 3, "tuple_2"),
        (first_id + 4, "expression_1"),
        (first_id + 5, "expression_2"),
    ]


def test_reflection_has_table(server_side_engine: Engine, test_db: str):
    with server_side_engine.connect() as conn:
        inspector = inspect(conn)
        assert inspector.has_table(TABLE, schema=test_db)
        assert {c["name"] for c in inspector.get_columns(TABLE, schema=test_db)} == {"id", "name"}


@pytest.mark.parametrize("server_side", [False, True], ids=["client-side", "server-side"])
@pytest.mark.parametrize("remaining_bind", [False, True], ids=["no-remaining-bind", "remaining-bind"])
def test_literal_execute_percent_roundtrip(
    server_side: bool,
    remaining_bind: bool,
    test_engine: Engine,
    server_side_engine: Engine,
):
    payload = "single% adjacent%% %(token)s quote'tail%"
    literal_value = bindparam("literal_value", payload, type_=ChString(), literal_execute=True)
    if remaining_bind:
        statement = select(literal_value, bindparam("remaining", 13, type_=Int32()))
        expected = (payload, 13)
    else:
        statement = select(literal_value)
        expected = (payload,)
    engine = server_side_engine if server_side else test_engine

    with engine.connect() as conn:
        row = conn.execute(statement).one()

    assert tuple(row) == expected


@pytest.mark.parametrize("no_parameters", [False, True], ids=["normal", "no-parameters"])
def test_server_side_exec_driver_sql_decodes_pyformat_percents(server_side_engine: Engine, no_parameters: bool):
    payload = "single% adjacent%% %(token)s quote'tail%"
    statement = f"SELECT {format_str(payload).replace('%', '%%')}"

    with server_side_engine.connect() as conn:
        if no_parameters:
            conn = conn.execution_options(no_parameters=True)
        value = conn.exec_driver_sql(statement).scalar_one()

    assert value == payload


def test_server_side_compiled_text_preserves_raw_percents(server_side_engine: Engine):
    payload = "single% adjacent%% %(token)s quote'tail%"

    with server_side_engine.connect() as conn:
        value = conn.execute(text(f"SELECT {format_str(payload)}")).scalar_one()

    assert value == payload
