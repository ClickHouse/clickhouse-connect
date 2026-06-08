"""End-to-end tests for the opt-in server_side_params mode (issue #735)."""

from collections.abc import Iterator

from pytest import fixture
from sqlalchemy import MetaData, Table, insert, inspect, select, text, tuple_
from sqlalchemy.engine import Engine, create_engine

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


def test_reflection_has_table(server_side_engine: Engine, test_db: str):
    with server_side_engine.connect() as conn:
        inspector = inspect(conn)
        assert inspector.has_table(TABLE, schema=test_db)
        assert {c["name"] for c in inspector.get_columns(TABLE, schema=test_db)} == {"id", "name"}
