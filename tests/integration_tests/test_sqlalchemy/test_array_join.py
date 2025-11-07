from pytest import fixture
from sqlalchemy import MetaData, Table, literal_column, select, text
from sqlalchemy.engine.base import Engine

from clickhouse_connect.cc_sqlalchemy.sql.clauses import array_join
from tests.integration_tests.test_sqlalchemy.conftest import verify_tables_ready


@fixture(scope="module", autouse=True)
def test_tables(test_engine: Engine, test_db: str):
    """Create test tables for ARRAY JOIN tests"""
    with test_engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {test_db}.test_array_join"))
        conn.execute(
            text(
                f"""
            CREATE TABLE {test_db}.test_array_join (
                id Int32,
                name String,
                tags Array(String)
            ) ENGINE MergeTree() ORDER BY id
        """
            )
        )

        conn.execute(
            text(
                f"""
            INSERT INTO {test_db}.test_array_join VALUES
            (1, 'Alice', ['python', 'sql', 'clickhouse']),
            (2, 'Bob', ['java', 'sql']),
            (3, 'Joe', ['python', 'javascript']),
            (4, 'Charlie', [])
        """
            )
        )

        # Verify data is actually queryable before yielding to tests
        verify_tables_ready(conn, {f"{test_db}.test_array_join": 4})

        yield

        conn.execute(text(f"DROP TABLE IF EXISTS {test_db}.test_array_join"))


def test_array_join(test_engine: Engine, test_db: str):
    """Test ARRAY JOIN clause"""
    with test_engine.begin() as conn:
        metadata = MetaData(schema=test_db)
        test_table = Table("test_array_join", metadata, autoload_with=test_engine)

        query = (
            select(test_table.c.id, test_table.c.name, test_table.c.tags)
            .select_from(array_join(test_table, test_table.c.tags))
            .order_by(test_table.c.id)
            .order_by(test_table.c.tags)
        )

        compiled = query.compile(dialect=test_engine.dialect)
        assert "ARRAY JOIN" in str(compiled).upper()

        result = conn.execute(query)
        rows = result.fetchall()
        assert len(rows) == 7
        assert rows[0].id == 1
        assert rows[0].name == "Alice"
        assert rows[0].tags == "clickhouse"
        # ARRAY JOIN should not contain items with empty lists
        assert "Charlie" not in [row.name for row in rows]


def test_left_array_join_with_alias(test_engine: Engine, test_db: str):
    """Test LEFT ARRAY JOIN with alias"""
    with test_engine.begin() as conn:
        metadata = MetaData(schema=test_db)
        test_table = Table("test_array_join", metadata, autoload_with=test_engine)

        query = (
            select(
                test_table.c.id,
                test_table.c.name,
                literal_column("tag"),  # Needed when using alias
            )
            .select_from(array_join(test_table, test_table.c.tags, alias="tag", is_left=True))
            .order_by(test_table.c.id)
            .order_by(literal_column("tag"))
        )

        compiled = query.compile(dialect=test_engine.dialect)
        compiled_str = str(compiled).upper()
        assert "LEFT ARRAY JOIN" in compiled_str
        assert "AS" in compiled_str

        result = conn.execute(query)
        rows = result.fetchall()
        assert len(rows) == 8

        alice_tags = [row.tag for row in rows if row.name == "Alice"]
        assert len(alice_tags) == 3
        assert alice_tags == sorted(["python", "sql", "clickhouse"])

        bob_tags = [row.tag for row in rows if row.name == "Bob"]
        assert len(bob_tags) == 2
        assert bob_tags == sorted(["java", "sql"])

        charlie_rows = [row for row in rows if row.name == "Charlie"]
        assert len(charlie_rows) == 1
        assert charlie_rows[0].tag == ""
