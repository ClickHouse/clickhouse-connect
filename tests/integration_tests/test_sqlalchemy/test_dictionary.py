from unittest.mock import Mock

import pytest
import sqlalchemy as sa

from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import String, UInt64
from clickhouse_connect.cc_sqlalchemy.ddl.dictionary import Dictionary
from clickhouse_connect.cc_sqlalchemy.ddl.tableengine import MergeTree

# ruff: noqa: F811
from tests.integration_tests.test_sqlalchemy.test_alembic import ch_name  # noqa: F401
from tests.integration_tests.test_sqlalchemy.test_skip_indexes import run_sqlalchemy  # noqa: F401


def test_dictionary_metadata_lifecycle(run_sqlalchemy, test_db, ch_name, test_config):
    if test_config.cloud:
        pytest.skip("Dictionary source references a local table not accessible on Cloud")
    metadata = sa.MetaData(schema=test_db)
    source = sa.Table(
        ch_name("z_dictionary_source"),
        metadata,
        sa.Column("id", UInt64),
        sa.Column("value", String),
        MergeTree(order_by="id"),
    )
    dictionary = Dictionary(
        ch_name("a_dictionary"),
        metadata,
        sa.Column("id", UInt64),
        sa.Column("value", String, server_default="user_2"),
        primary_key="id",
        source=f"CLICKHOUSE(DB '{test_db}' TABLE '{source.name}')",
        layout="FLAT",
        lifetime="0",
    )
    dictionary.add_is_dependent_on(source)
    assert list(metadata.sorted_tables) == [source, dictionary]

    def workflow(connection):
        source.create(connection)
        connection.execute(source.insert(), [{"id": 13, "value": "user_1"}, {"id": 79, "value": "user_2"}])
        metadata.create_all(connection)
        inspector = sa.inspect(connection)
        assert connection.execute(sa.text(f"EXISTS DICTIONARY `{test_db}`.`{dictionary.name}`")).scalar() == 1
        assert inspector.has_table(dictionary.name, schema=test_db)
        assert connection.execute(sa.select(dictionary).order_by(dictionary.c.id)).fetchall() == [(13, "user_1"), (79, "user_2")]

        executed = Mock()
        sa.event.listen(connection, "before_cursor_execute", executed)
        try:
            metadata.create_all(connection)
            dictionary.create(connection, checkfirst=True)
            assert all(not args[2].lstrip().upper().startswith("CREATE") for args, _ in executed.call_args_list)
            metadata.drop_all(connection)
            executed.reset_mock()
            dictionary.drop(connection, checkfirst=True)
            metadata.drop_all(connection, checkfirst=True)
            assert all(not args[2].lstrip().upper().startswith("DROP") for args, _ in executed.call_args_list)
        finally:
            sa.event.remove(connection, "before_cursor_execute", executed)
        assert not inspector.has_table(dictionary.name, schema=test_db)
        assert not inspector.has_table(source.name, schema=test_db)

    run_sqlalchemy(workflow)


def test_invalid_dictionary_column_sends_no_sql(run_sqlalchemy, test_db, ch_name):
    name = ch_name("invalid_dictionary")
    dictionary = Dictionary(name, sa.MetaData(schema=test_db), sa.Column("id", UInt64, comment="label"))

    def workflow(connection):
        executed = Mock()
        sa.event.listen(connection, "before_cursor_execute", executed)
        try:
            with pytest.raises(sa.exc.CompileError, match=f"Dictionary '{name}' column 'id'.*COMMENT"):
                dictionary.metadata.create_all(connection, checkfirst=False)
            executed.assert_not_called()
        finally:
            sa.event.remove(connection, "before_cursor_execute", executed)
        assert not sa.inspect(connection).has_table(name, schema=test_db)

    run_sqlalchemy(workflow)


def test_dictionary_attribute_reflection(run_sqlalchemy, test_db, ch_name, test_config):
    if test_config.cloud:
        pytest.skip("Dictionary source references a local table not accessible on Cloud")
    dictionary = ch_name("dictionary_attributes")
    source = ch_name("dictionary_attributes_source")
    attributes = [
        ("parent_id", "UInt64 DEFAULT 0 HIERARCHICAL", "UInt64", "0"),
        ("label", "String DEFAULT 'user_1' EXPRESSION upper(name) INJECTIVE", "String", "'user_1'"),
        ("note", "Nullable(String) INJECTIVE", "Nullable(String)", None),
        ("tags", "Array(String) DEFAULT ['user_1', 'user_2']", "Array(String)", "['user_1', 'user_2']"),
        ("names", "Array(String) EXPRESSION [name, upper(name)]", "Array(String)", None),
        ("title", "String DEFAULT '\u00df\u00df' EXPRESSION name INJECTIVE", "String", "'\u00df\u00df'"),
        # Six U+FB03 is exact. After str.upper() the quoted option text is at the index of the closing bracket.
        ("aliases", "Array(String) DEFAULT ['" + "\ufb03" * 6 + " INJECTIVE ']", "Array(String)", "['" + "\ufb03" * 6 + " INJECTIVE ']"),
    ]
    expected = [("id", "UInt64", None)] + [(name, type_name, default) for name, _sql, type_name, default in attributes]

    def workflow(connection):
        connection.execute(
            sa.text(
                f"CREATE TABLE `{test_db}`.`{source}` (id UInt64, parent_id UInt64, name String, note Nullable(String), "
                "tags Array(String), aliases Array(String)) ENGINE MergeTree ORDER BY id"
            )
        )
        connection.execute(
            sa.text(
                f"CREATE DICTIONARY `{test_db}`.`{dictionary}` (id UInt64, "
                + ", ".join(f"{name} {sql}" for name, sql, _type, _default in attributes)
                + f") PRIMARY KEY id SOURCE(CLICKHOUSE(DB '{test_db}' TABLE '{source}')) LAYOUT(HASHED()) LIFETIME(0)"
            )
        )
        columns = sa.inspect(connection).get_columns(dictionary, schema=test_db)
        assert [(column["name"], column["type"].name, column.get("server_default")) for column in columns] == expected
        table = sa.Table(dictionary, sa.MetaData(), schema=test_db, autoload_with=connection)
        reflected = [(column.name, column.type.name, column.server_default and column.server_default.arg.text) for column in table.c]
        assert reflected == expected

    run_sqlalchemy(workflow)
