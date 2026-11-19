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
