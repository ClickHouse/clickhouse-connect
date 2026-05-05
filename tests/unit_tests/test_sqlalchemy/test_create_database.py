import pytest
from sqlalchemy.exc import ArgumentError

from clickhouse_connect.cc_sqlalchemy.ddl.custom import CreateDatabase, DropDatabase


def _ddl(stmt):
    return stmt.statement if isinstance(stmt.statement, str) else str(stmt.statement)


def test_create_database_plain():
    assert _ddl(CreateDatabase("mydb")) == "CREATE DATABASE `mydb`"


def test_create_database_if_not_exists():
    assert _ddl(CreateDatabase("mydb", exists_ok=True)) == "CREATE DATABASE IF NOT EXISTS `mydb`"


def test_create_database_atomic_engine():
    assert _ddl(CreateDatabase("mydb", engine="Atomic")) == "CREATE DATABASE `mydb` Engine Atomic"


def test_create_database_unknown_engine():
    with pytest.raises(ArgumentError):
        CreateDatabase("mydb", engine="Bogus")


def test_create_database_replicated_requires_zoo_path():
    with pytest.raises(ArgumentError):
        CreateDatabase("mydb", engine="Replicated")


def test_create_database_replicated_default_macros():
    ddl = _ddl(CreateDatabase("mydb", engine="Replicated", zoo_path="/clickhouse/databases/mydb"))
    assert ddl == ("CREATE DATABASE `mydb` Engine Replicated ('/clickhouse/databases/mydb', '{shard}', '{replica}')")


def test_create_database_replicated_explicit_args():
    ddl = _ddl(
        CreateDatabase(
            "mydb",
            engine="Replicated",
            zoo_path="/clickhouse/databases/mydb",
            shard_name="shard_1",
            replica_name="replica_a",
        )
    )
    assert ddl == ("CREATE DATABASE `mydb` Engine Replicated ('/clickhouse/databases/mydb', 'shard_1', 'replica_a')")


def test_create_database_replicated_escapes_quote():
    ddl = _ddl(
        CreateDatabase(
            "mydb",
            engine="Replicated",
            zoo_path="/clickhouse/'evil",
            shard_name="shard'1",
            replica_name="replica\\a",
        )
    )
    assert ddl == ("CREATE DATABASE `mydb` Engine Replicated ('/clickhouse/\\'evil', 'shard\\'1', 'replica\\\\a')")


def test_drop_database_plain():
    assert _ddl(DropDatabase("mydb")) == "DROP DATABASE `mydb`"


def test_drop_database_if_exists():
    assert _ddl(DropDatabase("mydb", missing_ok=True)) == "DROP DATABASE IF EXISTS `mydb`"
