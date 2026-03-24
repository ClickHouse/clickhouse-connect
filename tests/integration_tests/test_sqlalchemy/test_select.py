# pylint: disable=no-member
from pytest import fixture
from sqlalchemy import MetaData, Table, func, literal_column, select, text
from sqlalchemy.engine import Engine

from clickhouse_connect import common
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import (
    DateTime64,
    String,
    UInt32,
)
from clickhouse_connect.cc_sqlalchemy.sql.clauses import ch_join
from tests.integration_tests.test_sqlalchemy.conftest import verify_tables_ready


@fixture(scope="module", autouse=True)
def test_tables(test_engine: Engine, test_db: str):
    """Create test tables for SELECT and JOIN tests"""
    common.set_setting("invalid_setting_action", "drop")

    with test_engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {test_db}.select_test_users"))
        conn.execute(
            text(
                f"""
            CREATE TABLE {test_db}.select_test_users (
                id UInt32,
                name String,
                created_at DateTime64(3)
            ) ENGINE MergeTree() ORDER BY (id, name) SAMPLE by id
        """
            )
        )

        conn.execute(text(f"DROP TABLE IF EXISTS {test_db}.select_test_orders"))
        conn.execute(
            text(
                f"""
            CREATE TABLE {test_db}.select_test_orders (
                id UInt32,
                user_id UInt32,
                product String,
                amount UInt32
            ) ENGINE MergeTree() ORDER BY tuple()
        """
            )
        )

        conn.execute(text(f"DROP TABLE IF EXISTS {test_db}.test_argmax"))
        conn.execute(
            text(
                f"""
            CREATE TABLE {test_db}.test_argmax (
                id Int32,
                name String,
                value Int32,
                updated_at DateTime
            ) ENGINE MergeTree() ORDER BY id
        """
            )
        )

        conn.execute(
            text(
                f"""
            INSERT INTO {test_db}.select_test_users VALUES
            (1, 'Alice', '2023-01-01 10:00:00.000'),
            (2, 'Bob', '2023-01-02 11:00:00.000'),
            (3, 'Charlie', '2023-01-03 12:00:00.000')
        """
            )
        )

        conn.execute(
            text(
                f"""
            INSERT INTO {test_db}.select_test_orders VALUES
            (101, 1, 'Laptop', 1500),
            (102, 2, 'Mouse', 25),
            (103, 1, 'Keyboard', 75),
            (104, 3, 'Monitor', 300)
        """
            )
        )

        conn.execute(
            text(
                f"""
            INSERT INTO {test_db}.test_argmax VALUES
            (1, 'Alice_v1', 100, '2024-01-01 00:00:00'),
            (1, 'Alice_v2', 150, '2025-01-02 00:00:00'),
            (1, 'Alice_v3', 200, '2024-01-03 00:00:00'),
            (2, 'Bob_v1', 300, '2024-01-01 00:00:00'),
            (2, 'Bob_v2', 250, '2024-01-02 00:00:00')
        """
            )
        )

        conn.execute(text(f"DROP TABLE IF EXISTS {test_db}.test_using_sales"))
        conn.execute(text(f"DROP TABLE IF EXISTS {test_db}.test_using_returns"))

        conn.execute(
            text(
                f"""
            CREATE TABLE {test_db}.test_using_sales (
                product_id UInt32,
                sold UInt32
            ) ENGINE MergeTree() ORDER BY product_id
        """
            )
        )
        conn.execute(
            text(
                f"""
            CREATE TABLE {test_db}.test_using_returns (
                product_id UInt32,
                returned UInt32
            ) ENGINE MergeTree() ORDER BY product_id
        """
            )
        )

        conn.execute(
            text(
                f"""
            INSERT INTO {test_db}.test_using_sales VALUES
            (1, 10), (2, 20), (3, 30)
        """
            )
        )
        conn.execute(
            text(
                f"""
            INSERT INTO {test_db}.test_using_returns VALUES
            (2, 5), (3, 10), (4, 15)
        """
            )
        )

        verify_tables_ready(conn, {
            f"{test_db}.select_test_users": 3,
            f"{test_db}.select_test_orders": 4,
            f"{test_db}.test_argmax": 5,
            f"{test_db}.test_using_sales": 3,
            f"{test_db}.test_using_returns": 3,
        })

        yield

        conn.execute(text(f"DROP TABLE IF EXISTS {test_db}.select_test_users"))
        conn.execute(text(f"DROP TABLE IF EXISTS {test_db}.select_test_orders"))
        conn.execute(text(f"DROP TABLE IF EXISTS {test_db}.test_argmax"))
        conn.execute(text(f"DROP TABLE IF EXISTS {test_db}.test_using_sales"))
        conn.execute(text(f"DROP TABLE IF EXISTS {test_db}.test_using_returns"))


def test_basic_select(test_engine: Engine, test_db: str):
    """Basic SELECT statement compilation and execution"""
    with test_engine.begin() as conn:
        metadata = MetaData(schema=test_db)
        users = Table("select_test_users", metadata, autoload_with=test_engine)

        query = select(users.c.id, users.c.name)
        result = conn.execute(query)
        rows = result.fetchall()

        assert len(rows) == 3
        assert rows[0].id == 1
        assert rows[0].name == "Alice"


def test_select_with_where(test_engine: Engine, test_db: str):
    """SELECT with WHERE clause"""
    with test_engine.begin() as conn:
        metadata = MetaData(schema=test_db)
        users = Table("select_test_users", metadata, autoload_with=test_engine)

        query = select(users.c.id, users.c.name).where(users.c.id == 2)
        result = conn.execute(query)
        rows = result.fetchall()

        assert len(rows) == 1
        assert rows[0].id == 2
        assert rows[0].name == "Bob"


def test_select_all_columns(test_engine: Engine, test_db: str):
    """SELECT * functionality"""
    with test_engine.begin() as conn:
        metadata = MetaData(schema=test_db)
        users = Table("select_test_users", metadata, autoload_with=test_engine)

        query = select(users)
        result = conn.execute(query)
        rows = result.fetchall()

        assert len(rows) == 3
        assert hasattr(rows[0], "id")
        assert hasattr(rows[0], "name")
        assert hasattr(rows[0], "created_at")


def test_basic_select_with_sample(test_engine: Engine, test_db: str):
    metadata = MetaData(schema=test_db)
    users = Table("select_test_users", metadata, autoload_with=test_engine)
    query = select(users).sample("1")
    compiled = query.compile(dialect=test_engine.dialect)
    compiled_str = str(compiled)
    assert compiled_str.endswith("SAMPLE 1")


def test_final_and_sample_chained(test_engine: Engine, test_db: str):
    """Chaining .final() and .sample() in either order should produce both clauses."""
    metadata = MetaData(schema=test_db)
    users = Table("select_test_users", metadata, autoload_with=test_engine)

    # final() then sample()
    query_fs = select(users).final().sample(0.1)
    compiled_fs = str(query_fs.compile(dialect=test_engine.dialect))
    assert "FINAL" in compiled_fs
    assert "SAMPLE 0.1" in compiled_fs
    assert compiled_fs.index("FINAL") < compiled_fs.index("SAMPLE")

    # sample() then final()
    query_sf = select(users).sample(0.1).final()
    compiled_sf = str(query_sf.compile(dialect=test_engine.dialect))
    assert "FINAL" in compiled_sf
    assert "SAMPLE 0.1" in compiled_sf
    assert compiled_sf.index("FINAL") < compiled_sf.index("SAMPLE")


def test_final_and_sample_with_alias(test_engine: Engine, test_db: str):
    """FINAL/SAMPLE on aliased tables renders after the alias suffix."""
    metadata = MetaData(schema=test_db)
    users = Table("select_test_users", metadata, autoload_with=test_engine)
    alias = users.alias("u")

    compiled = str(select(alias).final().sample(0.1).compile(dialect=test_engine.dialect))
    assert "AS `u` FINAL SAMPLE 0.1" in compiled
    assert "FINAL AS" not in compiled

    # Reversed order produces the same output
    compiled_rev = str(select(alias).sample(0.1).final().compile(dialect=test_engine.dialect))
    assert "AS `u` FINAL SAMPLE 0.1" in compiled_rev


def test_final_with_explicit_table_on_join(test_engine: Engine, test_db: str):
    """FINAL applied to a specific table in a join renders correctly."""
    metadata = MetaData(schema=test_db)
    users = Table("select_test_users", metadata, autoload_with=test_engine)
    orders = Table("select_test_orders", metadata, autoload_with=test_engine)

    join = users.join(orders, users.c.id == orders.c.user_id)
    query = select(users.c.id, orders.c.product).select_from(join).final(users)
    compiled = str(query.compile(dialect=test_engine.dialect))
    # FINAL should appear between the users table and the JOIN keyword
    from_clause = compiled[compiled.index("FROM"):]
    assert "select_test_users` FINAL" in from_clause
    assert "FINAL" not in from_clause[from_clause.index("JOIN"):]


def test_select_with_where_with_sample(test_engine: Engine, test_db: str):
    with test_engine.begin() as conn:
        metadata = MetaData(schema=test_db)
        users = Table("select_test_users", metadata, autoload_with=test_engine)

        query = select(users.c.id, users.c.name).sample(1).where(users.c.id == 2)
        compiled = query.compile(dialect=test_engine.dialect)
        compiled_str = str(compiled)
        assert "SAMPLE 1" in compiled_str

        result = conn.execute(query)
        rows = result.fetchall()

        assert len(rows) == 1
        assert rows[0].id == 2
        assert rows[0].name == "Bob"


def test_inner_join(test_engine: Engine, test_db: str):
    """Test INNER JOIN functionality"""
    with test_engine.begin() as conn:
        metadata = MetaData(schema=test_db)
        users = Table("select_test_users", metadata, autoload_with=test_engine)
        orders = Table("select_test_orders", metadata, autoload_with=test_engine)

        query = select(users.c.id, users.c.name, orders.c.product).select_from(users.join(orders, users.c.id == orders.c.user_id))
        result = conn.execute(query)
        rows = result.fetchall()

        assert len(rows) == 4

        alice_orders = [row for row in rows if row.name == "Alice"]
        assert len(alice_orders) == 2
        alice_products = {row.product for row in alice_orders}
        assert alice_products == {"Laptop", "Keyboard"}


def test_outer_join(test_engine: Engine, test_db: str):
    """Test LEFT OUTER JOIN functionality"""
    with test_engine.begin() as conn:
        metadata = MetaData(schema=test_db)
        users = Table("select_test_users", metadata, autoload_with=test_engine)
        orders = Table("select_test_orders", metadata, autoload_with=test_engine)

        query = select(users.c.id, users.c.name).outerjoin(orders, orders.c.user_id == users.c.id)
        result = conn.execute(query)
        rows = result.fetchall()

        assert len(rows) >= 3

        user_names = {row.name for row in rows}
        expected_names = {"Alice", "Bob", "Charlie"}
        assert expected_names.issubset(user_names)


def test_complex_join_with_conditions(test_engine: Engine, test_db: str):
    """Test complex JOIN with additional WHERE conditions"""
    with test_engine.begin() as conn:
        metadata = MetaData(schema=test_db)
        users = Table("select_test_users", metadata, autoload_with=test_engine)
        orders = Table("select_test_orders", metadata, autoload_with=test_engine)

        query = (
            select(users.c.name, orders.c.product, orders.c.amount)
            .select_from(users.join(orders, users.c.id == orders.c.user_id))
            .where(orders.c.amount > 50)
        )

        result = conn.execute(query)
        rows = result.fetchall()

        assert len(rows) == 3
        for row in rows:
            assert row.amount > 50


def test_select_distinct(test_engine: Engine, test_db: str):
    """Test SELECT DISTINCT functionality"""
    with test_engine.begin() as conn:
        metadata = MetaData(schema=test_db)
        orders = Table("select_test_orders", metadata, autoload_with=test_engine)

        query = select(orders.c.user_id).distinct()
        result = conn.execute(query)
        rows = result.fetchall()

        unique_user_ids = {row.user_id for row in rows}
        assert unique_user_ids == {1, 2, 3}


def test_select_order_by(test_engine: Engine, test_db: str):
    """Test SELECT with ORDER BY clause"""
    with test_engine.begin() as conn:
        metadata = MetaData(schema=test_db)
        users = Table("select_test_users", metadata, autoload_with=test_engine)

        query = select(users.c.id, users.c.name).order_by(users.c.name.desc())
        result = conn.execute(query)
        rows = result.fetchall()

        names = [row.name for row in rows]
        assert names == ["Charlie", "Bob", "Alice"]


def test_select_limit_offset(test_engine: Engine, test_db: str):
    """Test SELECT with LIMIT and OFFSET"""
    with test_engine.begin() as conn:
        metadata = MetaData(schema=test_db)
        users = Table("select_test_users", metadata, autoload_with=test_engine)

        query = select(users.c.id, users.c.name).order_by(users.c.id).limit(2)
        result = conn.execute(query)
        rows = result.fetchall()

        assert len(rows) == 2
        assert rows[0].id == 1
        assert rows[1].id == 2

        query = select(users.c.id, users.c.name).order_by(users.c.id).offset(1).limit(2)
        result = conn.execute(query)
        rows = result.fetchall()

        assert len(rows) == 2
        assert rows[0].id == 2
        assert rows[1].id == 3


def test_reflection_integration(test_engine: Engine, test_db: str):
    """Test that SELECT and JOIN work properly with reflected table schemas"""
    with test_engine.begin() as conn:
        metadata = MetaData(schema=test_db)

        users = Table("select_test_users", metadata, autoload_with=test_engine)
        orders = Table("select_test_orders", metadata, autoload_with=test_engine)

        assert isinstance(users.c.id.type, UInt32)
        assert isinstance(users.c.name.type, String)
        assert isinstance(users.c.created_at.type, DateTime64)

        query = select(users.c.id, users.c.name, orders.c.product).outerjoin(orders, orders.c.user_id == users.c.id)

        result = conn.execute(query)
        rows = result.fetchall()

        assert len(rows) >= 3

        for row in rows:
            assert isinstance(row.id, int)
            assert isinstance(row.name, str)


def test_argmax_aggregate_function(test_engine: Engine, test_db: str):
    """Test ClickHouse argMax aggregate function"""
    with test_engine.begin() as conn:
        metadata = MetaData(schema=test_db)
        test_table = Table("test_argmax", metadata, autoload_with=test_engine)

        query = (
            select(
                test_table.c.id,
                func.argMax(test_table.c.name, test_table.c.updated_at).label("latest_name"),
                func.argMax(test_table.c.value, test_table.c.updated_at).label("latest_value"),
            )
            .group_by(test_table.c.id)
            .order_by(test_table.c.id)
        )

        result = conn.execute(query)
        rows = result.fetchall()

        assert len(rows) == 2
        assert rows[0].id == 1
        assert rows[0].latest_name == "Alice_v2"
        assert rows[0].latest_value == 150
        assert rows[1].id == 2
        assert rows[1].latest_name == "Bob_v2"
        assert rows[1].latest_value == 250


def test_all_inner_ch_join(test_engine: Engine, test_db: str):
    """ALL INNER JOIN returns all matching rows"""
    with test_engine.begin() as conn:
        metadata = MetaData(schema=test_db)
        users = Table("select_test_users", metadata, autoload_with=test_engine)
        orders = Table("select_test_orders", metadata, autoload_with=test_engine)

        query = select(users.c.id, users.c.name, orders.c.product).select_from(
            ch_join(users, orders, users.c.id == orders.c.user_id, strictness="ALL")
        )

        compiled = query.compile(dialect=test_engine.dialect)
        assert "ALL INNER JOIN" in str(compiled).upper()

        result = conn.execute(query)
        rows = result.fetchall()
        assert len(rows) == 4


def test_any_left_ch_join(test_engine: Engine, test_db: str):
    """ANY LEFT JOIN returns at most one match per left row"""
    with test_engine.begin() as conn:
        metadata = MetaData(schema=test_db)
        users = Table("select_test_users", metadata, autoload_with=test_engine)
        orders = Table("select_test_orders", metadata, autoload_with=test_engine)

        query = select(users.c.id, users.c.name, orders.c.product).select_from(
            ch_join(users, orders, users.c.id == orders.c.user_id, isouter=True, strictness="ANY")
        )

        compiled = query.compile(dialect=test_engine.dialect)
        sql_str = str(compiled).upper()
        assert "ANY LEFT OUTER JOIN" in sql_str

        result = conn.execute(query)
        rows = result.fetchall()
        # ANY returns at most one order per user; user_id=1 has 2 orders but gets 1
        assert len(rows) == 3
        user_ids = [row.id for row in rows]
        assert sorted(user_ids) == [1, 2, 3]


def test_global_all_left_ch_join(test_engine: Engine, test_db: str):
    """GLOBAL ALL LEFT OUTER JOIN compiles and executes correctly"""
    with test_engine.begin() as conn:
        metadata = MetaData(schema=test_db)
        users = Table("select_test_users", metadata, autoload_with=test_engine)
        orders = Table("select_test_orders", metadata, autoload_with=test_engine)

        query = select(users.c.id, users.c.name, orders.c.product).select_from(
            ch_join(users, orders, users.c.id == orders.c.user_id, isouter=True, strictness="ALL", distribution="GLOBAL")
        )

        compiled = query.compile(dialect=test_engine.dialect)
        sql_str = str(compiled).upper()
        assert "GLOBAL ALL LEFT OUTER JOIN" in sql_str

        result = conn.execute(query)
        rows = result.fetchall()
        # LEFT JOIN: at least all 3 users returned
        assert len(rows) >= 3
        user_names = {row.name for row in rows}
        assert {"Alice", "Bob", "Charlie"}.issubset(user_names)


def test_using_inner_join(test_engine: Engine, test_db: str):
    """INNER JOIN USING on a shared column name"""
    with test_engine.begin() as conn:
        metadata = MetaData(schema=test_db)
        sales = Table("test_using_sales", metadata, autoload_with=test_engine)
        returns = Table("test_using_returns", metadata, autoload_with=test_engine)

        query = (
            select(sales.c.product_id, sales.c.sold, returns.c.returned)
            .select_from(ch_join(sales, returns, using=["product_id"]))
            .order_by(sales.c.product_id)
        )

        compiled_str = str(query.compile(dialect=test_engine.dialect))
        assert "USING" in compiled_str
        assert "ON" not in compiled_str

        result = conn.execute(query)
        rows = result.fetchall()
        # Only product_id 2 and 3 exist in both tables
        assert len(rows) == 2
        assert rows[0] == (2, 20, 5)
        assert rows[1] == (3, 30, 10)


def test_using_full_outer_join(test_engine: Engine, test_db: str):
    """FULL OUTER JOIN USING merges the join column correctly."""
    with test_engine.begin() as conn:
        metadata = MetaData(schema=test_db)
        sales = Table("test_using_sales", metadata, autoload_with=test_engine)
        returns = Table("test_using_returns", metadata, autoload_with=test_engine)

        # Use unqualified product_id to get the merged USING column
        pid = literal_column("product_id")
        query = (
            select(pid, sales.c.sold, returns.c.returned)
            .select_from(ch_join(sales, returns, using=["product_id"], full=True))
            .order_by(pid)
        )

        compiled_str = str(query.compile(dialect=test_engine.dialect))
        assert "FULL OUTER JOIN" in compiled_str
        assert "USING" in compiled_str

        result = conn.execute(query)
        rows = result.fetchall()
        assert len(rows) == 4

        by_pid = {row.product_id: row for row in rows}
        # product_id=4 only in returns. With USING, product_id is 4 (correct).
        # With ON, it would be 0 (wrong).
        assert by_pid[4].product_id == 4
        assert by_pid[4].sold == 0
        assert by_pid[4].returned == 15
        # product_id=1 only in sales
        assert by_pid[1].sold == 10
        assert by_pid[1].returned == 0


def test_using_with_strictness_integration(test_engine: Engine, test_db: str):
    """ANY INNER JOIN with USING compiles and executes"""
    with test_engine.begin() as conn:
        metadata = MetaData(schema=test_db)
        sales = Table("test_using_sales", metadata, autoload_with=test_engine)
        returns = Table("test_using_returns", metadata, autoload_with=test_engine)

        query = (
            select(sales.c.product_id, sales.c.sold, returns.c.returned)
            .select_from(ch_join(sales, returns, using=["product_id"], strictness="ANY"))
            .order_by(sales.c.product_id)
        )

        compiled_str = str(query.compile(dialect=test_engine.dialect))
        assert "ANY INNER JOIN" in compiled_str
        assert "USING" in compiled_str

        result = conn.execute(query)
        rows = result.fetchall()
        assert len(rows) == 2
