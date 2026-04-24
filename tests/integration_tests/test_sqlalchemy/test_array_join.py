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
        conn.execute(text(f"DROP TABLE IF EXISTS {test_db}.test_multi_array_join"))

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
            CREATE TABLE {test_db}.test_multi_array_join (
                id UInt32,
                names Array(String),
                prices Array(UInt32),
                quantities Array(UInt32)
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

        conn.execute(
            text(
                f"""
            INSERT INTO {test_db}.test_multi_array_join VALUES
            (1, ['widget_a', 'widget_b'], [100, 200], [5, 10]),
            (2, ['widget_c'], [300], [15]),
            (3, [], [], [])
        """
            )
        )

        verify_tables_ready(
            conn,
            {
                f"{test_db}.test_array_join": 4,
                f"{test_db}.test_multi_array_join": 3,
            },
        )

        yield

        conn.execute(text(f"DROP TABLE IF EXISTS {test_db}.test_array_join"))
        conn.execute(text(f"DROP TABLE IF EXISTS {test_db}.test_multi_array_join"))


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


def test_multi_column_array_join(test_engine: Engine, test_db: str):
    """Test ARRAY JOIN with multiple columns expanded in parallel"""
    with test_engine.begin() as conn:
        metadata = MetaData(schema=test_db)
        test_table = Table("test_multi_array_join", metadata, autoload_with=test_engine)

        query = (
            select(
                test_table.c.id,
                literal_column("item_name"),
                literal_column("price"),
                literal_column("qty"),
            )
            .select_from(
                array_join(
                    test_table,
                    [test_table.c.names, test_table.c.prices, test_table.c.quantities],
                    alias=["item_name", "price", "qty"],
                )
            )
            .order_by(test_table.c.id, literal_column("item_name"))
        )

        compiled_str = str(query.compile(dialect=test_engine.dialect))
        assert "ARRAY JOIN" in compiled_str.upper()
        # All three columns should appear comma-separated after ARRAY JOIN
        assert "AS `item_name`" in compiled_str
        assert "AS `price`" in compiled_str
        assert "AS `qty`" in compiled_str

        result = conn.execute(query)
        rows = result.fetchall()

        # id=1 has 2 elements, id=2 has 1 element -> 3 rows total
        assert len(rows) == 3
        assert rows[0] == (1, "widget_a", 100, 5)
        assert rows[1] == (1, "widget_b", 200, 10)
        assert rows[2] == (2, "widget_c", 300, 15)


def test_multi_column_array_join_no_aliases(test_engine: Engine, test_db: str):
    """Test multi-column ARRAY JOIN without aliases"""
    with test_engine.begin() as conn:
        metadata = MetaData(schema=test_db)
        test_table = Table("test_multi_array_join", metadata, autoload_with=test_engine)

        query = (
            select(test_table.c.id, test_table.c.names, test_table.c.prices)
            .select_from(
                array_join(
                    test_table,
                    [test_table.c.names, test_table.c.prices],
                )
            )
            .order_by(test_table.c.id, test_table.c.names)
        )

        compiled_str = str(query.compile(dialect=test_engine.dialect))
        assert "ARRAY JOIN" in compiled_str.upper()
        assert "AS" not in compiled_str.split("ARRAY JOIN")[1]

        result = conn.execute(query)
        rows = result.fetchall()
        assert len(rows) == 3
        assert rows[0] == (1, "widget_a", 100)
        assert rows[1] == (1, "widget_b", 200)
        assert rows[2] == (2, "widget_c", 300)


def test_multi_column_left_array_join(test_engine: Engine, test_db: str):
    """Test LEFT ARRAY JOIN with multiple columns preserves empty-array rows"""
    with test_engine.begin() as conn:
        metadata = MetaData(schema=test_db)
        test_table = Table("test_multi_array_join", metadata, autoload_with=test_engine)

        query = (
            select(
                test_table.c.id,
                literal_column("item_name"),
                literal_column("price"),
            )
            .select_from(
                array_join(
                    test_table,
                    [test_table.c.names, test_table.c.prices],
                    alias=["item_name", "price"],
                    is_left=True,
                )
            )
            .order_by(test_table.c.id, literal_column("item_name"))
        )

        compiled_str = str(query.compile(dialect=test_engine.dialect))
        assert "LEFT ARRAY JOIN" in compiled_str.upper()

        result = conn.execute(query)
        rows = result.fetchall()

        # id=1 has 2, id=2 has 1, id=3 has 0 (preserved by LEFT) = 4
        assert len(rows) == 4
        empty_rows = [r for r in rows if r.id == 3]
        assert len(empty_rows) == 1
        assert empty_rows[0].item_name == ""  # default for String
        assert empty_rows[0].price == 0  # default for UInt32
