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

from clickhouse_connect.cc_sqlalchemy import ClickHouseIndex
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import UInt32
from clickhouse_connect.cc_sqlalchemy.ddl.dictionary import Dictionary
from clickhouse_connect.cc_sqlalchemy.ddl.index import table_indexes
from clickhouse_connect.cc_sqlalchemy.ddl.tableengine import MergeTree
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect
from clickhouse_connect.cc_sqlalchemy.inspector import get_clickhouse_indexes


def _table(*indexes, **kwargs):
    return Table("events", MetaData(), Column("value", UInt32), *indexes, MergeTree(order_by="value"), **kwargs)


def test_index_declaration_create_and_copy():
    index = ClickHouseIndex("value index", "`value` + 13", "minmax", granularity=4, if_not_exists=True, first=True)
    table = _table(index, schema="quoted database")
    assert table_indexes(table) == (index,)
    assert not table.indexes
    copied = table.to_metadata(MetaData(), name="events_copy")
    assert table_indexes(copied) == (index,)
    assert copied.kwargs["clickhousedb_indexes"] is table.kwargs["clickhousedb_indexes"]
    sql = str(CreateTable(table).compile(dialect=ClickHouseDialect()))
    assert "CREATE TABLE `quoted database`.`events`" in sql
    assert "INDEX `value index` `value` + 13 TYPE minmax GRANULARITY 4" in sql
    assert "IF NOT EXISTS" not in sql
    assert "FIRST" not in sql
    with pytest.raises(FrozenInstanceError):
        index.expression = "value"


def test_index_legacy_import_and_constructor():
    from clickhouse_connect.cc_sqlalchemy.alembic.operations import ClickHouseIndex as LegacyIndex

    assert LegacyIndex is ClickHouseIndex
    assert LegacyIndex("value_idx", "value", "minmax", 4, True, False, "earlier_idx") == ClickHouseIndex(
        "value_idx", "value", "minmax", granularity=4, if_not_exists=True, after_index="earlier_idx"
    )


@pytest.mark.parametrize(
    "indexes",
    [
        (ClickHouseIndex("same", "value", "minmax"), ClickHouseIndex("same", "value + 13", "minmax")),
        ("value",),
    ],
)
def test_invalid_index_collection(indexes):
    with pytest.raises(ArgumentError):
        table_indexes(_table(clickhousedb_indexes=indexes))


@pytest.mark.parametrize("same_object", [True, False])
def test_duplicate_positional_index_rejected(same_object):
    index = ClickHouseIndex("same", "value", "minmax")
    with pytest.raises(ArgumentError, match="Duplicate ClickHouse index"):
        _table(index, index if same_object else ClickHouseIndex("same", "value", "minmax"))


def test_equal_positional_index_redeclaration():
    table = _table(ClickHouseIndex("value_idx", "value", "minmax"))
    original_indexes = table.kwargs["clickhousedb_indexes"]
    redeclared = Table(
        "events",
        table.metadata,
        ClickHouseIndex("value_idx", "value", "minmax"),
        extend_existing=True,
    )
    assert redeclared is table
    assert table.kwargs["clickhousedb_indexes"] is original_indexes


def test_changed_positional_index_replaces_in_place():
    first = ClickHouseIndex("first_idx", "value", "minmax")
    original = ClickHouseIndex("value_idx", "value", "minmax")
    last = ClickHouseIndex("last_idx", "value", "minmax")
    replacement = ClickHouseIndex("value_idx", "value + 13", "minmax")
    table = _table(first, original, last)
    replacement._set_parent(table, allow_replacements=True)
    assert table_indexes(table) == (first, replacement, last)
    with pytest.raises(ArgumentError, match="Duplicate ClickHouse index"):
        original._set_parent(table)


def test_changed_positional_index_redeclaration():
    original = ClickHouseIndex("value_idx", "value", "minmax")
    replacement = ClickHouseIndex("value_idx", "value + 13", "minmax")
    first = ClickHouseIndex("first_idx", "value", "minmax")
    last = ClickHouseIndex("last_idx", "value", "minmax")
    table = _table(first, original, last)
    if sqlalchemy_version.startswith("1."):
        # SQLAlchemy 1.4 does not forward allow_replacements during redeclaration.
        with pytest.raises(ArgumentError, match="Duplicate ClickHouse index"):
            Table("events", table.metadata, replacement, extend_existing=True)
        assert table_indexes(table) == (first, original, last)
    else:
        assert Table("events", table.metadata, replacement, extend_existing=True) is table
        assert table_indexes(table) == (first, replacement, last)


@pytest.mark.parametrize("granularity", [True, False, 1.5, "4"])
def test_invalid_index_granularity(granularity):
    with pytest.raises(TypeError, match="granularity must be an integer or None"):
        ClickHouseIndex("value_idx", "value", "minmax", granularity=granularity)


@pytest.mark.parametrize("granularity", [None, 0, 4])
def test_valid_index_granularity(granularity):
    assert ClickHouseIndex("value_idx", "value", "minmax", granularity=granularity).granularity == granularity


def test_sqlalchemy_only_index_has_no_alembic_dependency():
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys\n"
            "from clickhouse_connect.cc_sqlalchemy import ClickHouseIndex\n"
            "from sqlalchemy import Table, MetaData, Column\n"
            "from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import UInt32\n"
            "from clickhouse_connect.cc_sqlalchemy.ddl.tableengine import MergeTree\n"
            "Table('events', MetaData(), Column('value', UInt32), "
            "ClickHouseIndex('idx','value','minmax'), MergeTree(order_by='value'))\n"
            "assert 'alembic' not in sys.modules\n",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_explicit_index_catalog_classification():
    connection = Mock()
    connection.execute.side_effect = [
        iter([SimpleNamespace(engine="MergeTree")]),
        iter(
            [
                SimpleNamespace(
                    table="events",
                    name="auto_minmax_index_user",
                    expr="value",
                    type_full="minmax",
                    granularity=1,
                    creation="Explicit",
                ),
                SimpleNamespace(name="implicit", creation="Implicit"),
            ]
        ),
    ]
    assert get_clickhouse_indexes(connection, "events", "warehouse") == [
        ClickHouseIndex("auto_minmax_index_user", "tuple(value)", "minmax", 1)
    ]


def test_unknown_catalog_classification_is_an_error():
    connection = Mock()
    connection.execute.side_effect = [
        iter([SimpleNamespace(engine="MergeTree")]),
        iter([SimpleNamespace(creation="Unknown")]),
    ]
    with pytest.raises(InvalidRequestError, match="Unrecognized ClickHouse index creation"):
        get_clickhouse_indexes(connection, "events", "warehouse")


def test_index_reflection_requires_visible_table():
    connection = Mock()
    connection.execute.return_value = iter([])
    with pytest.raises(NoSuchTableError):
        get_clickhouse_indexes(connection, "invisible", "warehouse")
    assert connection.execute.call_count == 1


@pytest.mark.parametrize("canonical", [None, (), (ClickHouseIndex("canonical", "value", "minmax"),)])
def test_legacy_index_option_and_canonical_precedence(canonical):
    legacy = (ClickHouseIndex("legacy", "value", "minmax"),)
    options = {"clickhouse_indexes": legacy}
    if canonical is not None:
        options["clickhousedb_indexes"] = canonical
    expected = legacy if canonical is None else canonical
    table = _table(**options)
    assert table_indexes(table) == expected
    assert table_indexes(table.to_metadata(MetaData())) == expected
    sql = str(CreateTable(table).compile(dialect=ClickHouseDialect()))
    assert ("INDEX `legacy`" in sql) == (canonical is None)
    assert ("INDEX `canonical`" in sql) == bool(canonical)


@pytest.mark.parametrize("placement", ["positional", "canonical", "legacy"])
def test_dictionary_index_declarations_are_rejected(placement):
    index = ClickHouseIndex("value_idx", "value", "minmax")
    args = (index,) if placement == "positional" else ()
    options = {} if placement == "positional" else {f"{'clickhousedb' if placement == 'canonical' else 'clickhouse'}_indexes": (index,)}
    dictionary = Dictionary("lookup", MetaData(), Column("value", UInt32), *args, **options)
    with pytest.raises(CompileError, match="Dictionaries do not support data skipping indexes"):
        str(CreateTable(dictionary).compile(dialect=ClickHouseDialect()))
