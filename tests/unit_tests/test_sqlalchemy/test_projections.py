import subprocess
import sys
from dataclasses import FrozenInstanceError
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from sqlalchemy import Column, MetaData, Table
from sqlalchemy import __version__ as sqlalchemy_version
from sqlalchemy.exc import ArgumentError, CompileError, InvalidRequestError, NoSuchTableError
from sqlalchemy.schema import CreateTable

from clickhouse_connect.cc_sqlalchemy import ClickHouseProjection
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import UInt32
from clickhouse_connect.cc_sqlalchemy.ddl.dictionary import Dictionary
from clickhouse_connect.cc_sqlalchemy.ddl.projection import table_projections
from clickhouse_connect.cc_sqlalchemy.ddl.tableengine import MergeTree
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect
from clickhouse_connect.cc_sqlalchemy.inspector import get_clickhouse_projections


def _table(*projections, **kwargs):
    return Table("events", MetaData(), Column("value", UInt32), *projections, MergeTree(order_by="value"), **kwargs)


def test_declaration_create_and_copy():
    projection = ClickHouseProjection("quoted projection", "SELECT value ORDER BY value", True, True)
    table = _table(projection, schema="quoted database")
    assert table_projections(table) == (projection,)
    copied = table.to_metadata(MetaData(), name="events_copy")
    assert table_projections(copied) == (projection,)
    assert copied.kwargs["clickhousedb_projections"] is table.kwargs["clickhousedb_projections"]
    sql = str(CreateTable(table).compile(dialect=ClickHouseDialect()))
    assert "CREATE TABLE `quoted database`.`events`" in sql
    assert "PROJECTION `quoted projection` (SELECT value ORDER BY value)" in sql
    assert "IF NOT EXISTS" not in sql
    assert "FIRST" not in sql
    with pytest.raises(FrozenInstanceError):
        projection.select = "SELECT value + 13"


def test_legacy_import_and_constructor():
    from clickhouse_connect.cc_sqlalchemy.alembic import ClickHouseProjection as AlembicProjection
    from clickhouse_connect.cc_sqlalchemy.alembic.operations import ClickHouseProjection as LegacyProjection

    assert AlembicProjection is LegacyProjection is ClickHouseProjection
    assert LegacyProjection("p", "SELECT value", True, False, "earlier") == ClickHouseProjection(
        "p", "SELECT value", if_not_exists=True, after_projection="earlier"
    )


@pytest.mark.parametrize("projections", [(ClickHouseProjection("same", "SELECT value"),) * 2, ("value",)])
def test_invalid_collection(projections):
    with pytest.raises(ArgumentError):
        table_projections(_table(clickhousedb_projections=projections))


@pytest.mark.parametrize("same_object", [True, False])
def test_duplicate_positional_declaration_rejected(same_object):
    projection = ClickHouseProjection("same", "SELECT value")
    with pytest.raises(ArgumentError, match="Duplicate ClickHouse projection"):
        _table(projection, projection if same_object else ClickHouseProjection("same", "SELECT value"))


def test_equal_redeclaration_and_explicit_replacement():
    first = ClickHouseProjection("first", "SELECT value")
    original = ClickHouseProjection("projection", "SELECT value")
    last = ClickHouseProjection("last", "SELECT value")
    table = _table(first, original, last)
    declarations = table.kwargs["clickhousedb_projections"]
    assert Table("events", table.metadata, ClickHouseProjection("projection", "SELECT value"), extend_existing=True) is table
    assert table.kwargs["clickhousedb_projections"] is declarations
    replacement = ClickHouseProjection("projection", "SELECT value + 13")
    replacement._set_parent(table, allow_replacements=True)
    assert table_projections(table) == (first, replacement, last)
    with pytest.raises(ArgumentError, match="Duplicate ClickHouse projection"):
        original._set_parent(table)


def test_changed_redeclaration():
    original = ClickHouseProjection("p", "SELECT value")
    replacement = ClickHouseProjection("p", "SELECT value + 13")
    table = _table(original)
    if sqlalchemy_version.startswith("1."):
        with pytest.raises(ArgumentError, match="Duplicate ClickHouse projection"):
            Table("events", table.metadata, replacement, extend_existing=True)
        assert table_projections(table) == (original,)
    else:
        assert Table("events", table.metadata, replacement, extend_existing=True) is table
        assert table_projections(table) == (replacement,)


def test_position_validation():
    with pytest.raises(ValueError, match="both first and after"):
        ClickHouseProjection("p", "SELECT value", first=True, after_projection="earlier")


def test_sqlalchemy_only_has_no_alembic_dependency():
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys\n"
            "from clickhouse_connect.cc_sqlalchemy import ClickHouseProjection\n"
            "from sqlalchemy import Table, MetaData, Column\n"
            "from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import UInt32\n"
            "from clickhouse_connect.cc_sqlalchemy.ddl.tableengine import MergeTree\n"
            "Table('events', MetaData(), Column('value', UInt32), "
            "ClickHouseProjection('p', 'SELECT value'), MergeTree(order_by='value'))\n"
            "assert 'alembic' not in sys.modules\n",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("canonical", [None, (), (ClickHouseProjection("canonical", "SELECT value"),)])
def test_legacy_option_and_canonical_precedence(canonical):
    legacy = (ClickHouseProjection("legacy", "SELECT value"),)
    options = {"clickhouse_projections": legacy}
    if canonical is not None:
        options["clickhousedb_projections"] = canonical
    expected = legacy if canonical is None else canonical
    table = _table(**options)
    assert table_projections(table) == table_projections(table.to_metadata(MetaData())) == expected
    sql = str(CreateTable(table).compile(dialect=ClickHouseDialect()))
    assert ("PROJECTION `legacy`" in sql) == (canonical is None)
    assert ("PROJECTION `canonical`" in sql) == bool(canonical)


@pytest.mark.parametrize("placement", ["positional", "canonical", "legacy"])
def test_dictionary_declarations_are_rejected(placement):
    projection = ClickHouseProjection("p", "SELECT value")
    args = (projection,) if placement == "positional" else ()
    options = (
        {} if placement == "positional" else {f"{'clickhousedb' if placement == 'canonical' else 'clickhouse'}_projections": (projection,)}
    )
    dictionary = Dictionary("lookup", MetaData(), Column("value", UInt32), *args, **options)
    with pytest.raises(CompileError, match="Dictionaries do not support projections"):
        str(CreateTable(dictionary).compile(dialect=ClickHouseDialect()))


def test_reflection_preserves_select():
    connection = Mock()
    connection.execute.side_effect = [
        iter([SimpleNamespace(engine="MergeTree")]),
        iter([SimpleNamespace(table="events", name="p", type="Normal", query="SELECT * ORDER BY value", settings={})]),
    ]
    assert get_clickhouse_projections(connection, "events", "warehouse") == [ClickHouseProjection("p", "SELECT * ORDER BY value")]
    statement, parameters = connection.execute.call_args.args
    assert parameters == {"database": "warehouse", "table_name": "events"}
    assert "AND table = :table_name" in str(statement)


@pytest.mark.parametrize(
    ("type_", "query", "settings", "reason"),
    [
        ("Normal", "SELECT value", {"index_granularity": "13"}, "WITH SETTINGS"),
        ("Normal", "value", {}, "index-based"),
        ("Unknown", "SELECT value", {}, "unsupported projection type"),
    ],
)
def test_unsupported_reflection_refuses_incomplete_replay(type_, query, settings, reason):
    connection = Mock()
    connection.execute.side_effect = [
        iter([SimpleNamespace(engine="MergeTree")]),
        iter([SimpleNamespace(table="events", name="p", type=type_, query=query, settings=settings)]),
    ]
    with pytest.raises(InvalidRequestError, match=reason):
        get_clickhouse_projections(connection, "events", "warehouse")


def test_reflection_requires_visible_table():
    connection = Mock()
    connection.execute.return_value = iter([])
    with pytest.raises(NoSuchTableError):
        get_clickhouse_projections(connection, "invisible", "warehouse")
    assert connection.execute.call_count == 1
