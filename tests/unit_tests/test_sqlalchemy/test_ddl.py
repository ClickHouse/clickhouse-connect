import ast

import sqlalchemy as db
from sqlalchemy.sql.ddl import CreateTable

from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import Date, DateTime, UInt32, UInt64
from clickhouse_connect.cc_sqlalchemy.ddl.tableengine import (
    GraphiteMergeTree,
    MergeTree,
    ReplacingMergeTree,
    ReplicatedCollapsingMergeTree,
    ReplicatedGraphiteMergeTree,
    ReplicatedMergeTree,
    ReplicatedReplacingMergeTree,
    ReplicatedVersionedCollapsingMergeTree,
)
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect

dialect = ClickHouseDialect()

replicated_mt_ddl = """\
CREATE TABLE `replicated_mt_test` (`key` UInt64) Engine ReplicatedMergeTree('/clickhouse/tables/repl_mt_test',\
 '{replica}') ORDER BY key\
"""

replacing_mt_ddl = """\
CREATE TABLE `replacing_mt_test` (`key` UInt32, `date` DateTime) Engine ReplacingMergeTree(date) ORDER BY key\
"""


def test_table_def():
    metadata = db.MetaData()

    table = db.Table(
        "replicated_mt_test",
        metadata,
        db.Column("key", UInt64),
        ReplicatedMergeTree(order_by="key", zk_path="/clickhouse/tables/repl_mt_test", replica="{replica}"),
    )
    ddl = str(CreateTable(table).compile("", dialect=dialect))
    assert ddl == replicated_mt_ddl

    table = db.Table(
        "replacing_mt_test", metadata, db.Column("key", UInt32), db.Column("date", DateTime), ReplacingMergeTree(ver="date", order_by="key")
    )

    ddl = str(CreateTable(table).compile("", dialect=dialect))
    assert ddl == replacing_mt_ddl


repl_replacing_mt_ddl = """\
CREATE TABLE `repl_replacing_mt` (`key` UInt64, `ver_col` UInt32) Engine \
ReplicatedReplacingMergeTree('/clickhouse/tables/repl_replacing', '{replica}', ver_col) ORDER BY key\
"""

repl_replacing_mt_no_ver_ddl = """\
CREATE TABLE `repl_replacing_mt_no_ver` (`key` UInt64) Engine \
ReplicatedReplacingMergeTree('/clickhouse/tables/repl_replacing_nv', '{replica}') ORDER BY key\
"""

repl_collapsing_mt_ddl = """\
CREATE TABLE `repl_collapsing_mt` (`key` UInt64, `sign_col` UInt32) Engine \
ReplicatedCollapsingMergeTree('/clickhouse/tables/repl_collapsing', '{replica}', sign_col) ORDER BY key\
"""

repl_ver_collapsing_mt_ddl = """\
CREATE TABLE `repl_ver_collapsing_mt` (`key` UInt64, `sign_col` UInt32, `ver_col` UInt32) Engine \
ReplicatedVersionedCollapsingMergeTree('/clickhouse/tables/repl_ver_collapsing', '{replica}', sign_col, ver_col) ORDER BY key\
"""

repl_graphite_mt_ddl = """\
CREATE TABLE `repl_graphite_mt` (`key` UInt64) Engine \
ReplicatedGraphiteMergeTree('/clickhouse/tables/repl_graphite', '{replica}', 'graphite_rollup') ORDER BY key\
"""

graphite_mt_ddl = """\
CREATE TABLE `graphite_mt` (`key` UInt64) Engine GraphiteMergeTree('graphite_rollup') ORDER BY key\
"""


def test_replicated_replacing_merge_tree():
    metadata = db.MetaData()

    table = db.Table(
        "repl_replacing_mt",
        metadata,
        db.Column("key", UInt64),
        db.Column("ver_col", UInt32),
        ReplicatedReplacingMergeTree(ver="ver_col", order_by="key", zk_path="/clickhouse/tables/repl_replacing", replica="{replica}"),
    )
    ddl = str(CreateTable(table).compile("", dialect=dialect))
    assert ddl == repl_replacing_mt_ddl

    table = db.Table(
        "repl_replacing_mt_no_ver",
        metadata,
        db.Column("key", UInt64),
        ReplicatedReplacingMergeTree(order_by="key", zk_path="/clickhouse/tables/repl_replacing_nv", replica="{replica}"),
    )
    ddl = str(CreateTable(table).compile("", dialect=dialect))
    assert ddl == repl_replacing_mt_no_ver_ddl


def test_replicated_collapsing_merge_tree():
    metadata = db.MetaData()

    table = db.Table(
        "repl_collapsing_mt",
        metadata,
        db.Column("key", UInt64),
        db.Column("sign_col", UInt32),
        ReplicatedCollapsingMergeTree(sign="sign_col", order_by="key", zk_path="/clickhouse/tables/repl_collapsing", replica="{replica}"),
    )
    ddl = str(CreateTable(table).compile("", dialect=dialect))
    assert ddl == repl_collapsing_mt_ddl


def test_replicated_versioned_collapsing_merge_tree():
    metadata = db.MetaData()

    table = db.Table(
        "repl_ver_collapsing_mt",
        metadata,
        db.Column("key", UInt64),
        db.Column("sign_col", UInt32),
        db.Column("ver_col", UInt32),
        ReplicatedVersionedCollapsingMergeTree(
            sign="sign_col", version="ver_col", order_by="key", zk_path="/clickhouse/tables/repl_ver_collapsing", replica="{replica}"
        ),
    )
    ddl = str(CreateTable(table).compile("", dialect=dialect))
    assert ddl == repl_ver_collapsing_mt_ddl


def test_replicated_graphite_merge_tree():
    metadata = db.MetaData()

    table = db.Table(
        "repl_graphite_mt",
        metadata,
        db.Column("key", UInt64),
        ReplicatedGraphiteMergeTree(
            config_section="graphite_rollup", order_by="key", zk_path="/clickhouse/tables/repl_graphite", replica="{replica}"
        ),
    )
    ddl = str(CreateTable(table).compile("", dialect=dialect))
    assert ddl == repl_graphite_mt_ddl


def test_graphite_merge_tree_quoting():
    metadata = db.MetaData()

    table = db.Table("graphite_mt", metadata, db.Column("key", UInt64), GraphiteMergeTree(config_section="graphite_rollup", order_by="key"))
    ddl = str(CreateTable(table).compile("", dialect=dialect))
    assert ddl == graphite_mt_ddl


column_partition_by_ddl = """\
CREATE TABLE `events` (`id` UInt64, `partition_date` Date) Engine MergeTree ORDER BY id PARTITION BY `partition_date`\
"""


def test_column_accepted_as_partition_by():
    metadata = db.MetaData()
    col = db.Column("partition_date", Date)
    table = db.Table(
        "events",
        metadata,
        db.Column("id", UInt64),
        col,
        MergeTree(partition_by=col, order_by="id"),
    )
    ddl = str(CreateTable(table).compile("", dialect=dialect))
    assert ddl == column_partition_by_ddl


column_order_by_tuple_ddl = """\
CREATE TABLE `events2` (`id` UInt64, `ts` DateTime) Engine MergeTree  ORDER BY (`ts`,`id`)\
"""


def test_column_tuple_accepted_as_order_by():
    metadata = db.MetaData()
    id_col = db.Column("id", UInt64)
    ts_col = db.Column("ts", DateTime)
    table = db.Table(
        "events2",
        metadata,
        id_col,
        ts_col,
        MergeTree(order_by=(ts_col, id_col)),
    )
    ddl = str(CreateTable(table).compile("", dialect=dialect))
    assert ddl == column_order_by_tuple_ddl


def test_repr_engine_value_column_roundtrip():
    some_column = db.Column("some_column_name", UInt64)
    engine = MergeTree(partition_by=some_column, order_by="id")
    rendered = repr(engine)
    assert "partition_by='some_column_name'" in rendered
    assert "Column(" not in rendered
    # Strip "MergeTree(" prefix and trailing ")" to get the args
    assert rendered.startswith("MergeTree(")
    assert rendered.endswith(")")
    # Assert the full expression parses as valid Python syntax (re-importable)
    ast.parse(rendered)
