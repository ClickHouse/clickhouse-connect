# pylint: disable=no-member
import time

from pytest import fixture
from sqlalchemy import MetaData, Table, select, text
from sqlalchemy.engine import Engine

from clickhouse_connect import common
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import (
    DateTime64,
    String,
    UInt32,
)


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
            ) ENGINE MergeTree() ORDER BY tuple()
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

        # Verify data is actually queryable before yielding to tests--been an issue in cloud env
        max_retries = 30
        retry_count = 0
        while retry_count < max_retries:
            try:
                user_count = conn.execute(text(f"SELECT COUNT(*) FROM {test_db}.select_test_users")).scalar()
                order_count = conn.execute(text(f"SELECT COUNT(*) FROM {test_db}.select_test_orders")).scalar()
                if user_count == 3 and order_count == 4:
                    break
                retry_count += 1
                if retry_count < max_retries:
                    time.sleep(0.1)
                else:
                    raise RuntimeError(f"Data verification failed: users={user_count}, orders={order_count}")
            except Exception as e:  # pylint: disable=broad-exception-caught
                retry_count += 1
                if retry_count >= max_retries:
                    raise RuntimeError(f"Failed to verify test data after {max_retries} retries.") from e
                time.sleep(0.1)

        yield

        conn.execute(text(f"DROP TABLE IF EXISTS {test_db}.select_test_users"))
        conn.execute(text(f"DROP TABLE IF EXISTS {test_db}.select_test_orders"))


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
