import logging
from types import SimpleNamespace

import pytest
from alembic.autogenerate.api import AutogenContext
from alembic.operations import ops
from alembic.runtime.migration import MigrationContext
from sqlalchemy import Column, MetaData, Table
from sqlalchemy.exc import OperationalError

from clickhouse_connect.cc_sqlalchemy.alembic import TableOptionDrift, UnsupportedTableChangeError, include_object
from clickhouse_connect.cc_sqlalchemy.alembic.compare import (
    COMPARE_TABLE_OPTIONS_OPT,
    _compare_table_options,
    _engine_family,
    normalize_expression,
)
from clickhouse_connect.cc_sqlalchemy.alembic.operations import _ModifyClickHouseTableSettingsOp, _ResetClickHouseTableSettingsOp
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import DateTime, String, UInt32
from clickhouse_connect.cc_sqlalchemy.ddl.tableengine import (
    Distributed,
    File,
    GraphiteMergeTree,
    Memory,
    MergeTree,
    Null,
    ReplacingMergeTree,
    ReplicatedMergeTree,
    ReplicatedReplacingMergeTree,
    SummingMergeTree,
    build_engine,
)
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect


@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        ("(a, ts)", "(a,ts)"),
        ("( `a` ,  `ts` )", "(a,ts)"),
        ("(a)", "a"),
        ("((a))", "a"),
        ("(toYYYYMM(ts))", "toYYYYMM(ts)"),
        ("(a)+(b)", "(a)+(b)"),
        ("tuple()", "tuple()"),
        ("()", "tuple()"),
        ("ts + toIntervalDay(30)", "ts+toIntervalDay(30)"),
        ("concat(`name`, ' x ')", "concat(name,' x ')"),
        ("'it''s'", "'it''s'"),
        ("'a\\'b `c`'", "'a\\'b `c`'"),
        ("(`a``b`)", "`a\\`b`"),
        ("(`a\\`b`)", "`a\\`b`"),
        ("(`a b`)", "`a b`"),
        ("(`a.b`)", "`a.b`"),
        ("(`NULL`)", "`NULL`"),
        ("(concat(')', a))", "concat(')',a)"),
        ("(concat('it''s )', a))", "concat('it''s )',a)"),
        (r"(`a\tb`)", r"(`a\tb`)"),
        (r"(`a\x41b`)", r"(`a\x41b`)"),
        (r"(`a\qb`)", r"(`a\qb`)"),
    ],
)
def test_normalize_expression(expression, expected):
    assert normalize_expression(expression) == expected


@pytest.mark.parametrize(
    ("name", "family"),
    [
        ("MergeTree", "MergeTree"),
        ("SharedMergeTree", "MergeTree"),
        ("ReplicatedReplacingMergeTree", "ReplacingMergeTree"),
        ("SharedReplacingMergeTree", "ReplacingMergeTree"),
        ("Memory", "Memory"),
    ],
)
def test_engine_family(name, family):
    assert _engine_family(name) == family


def _autogen_context(**opts) -> AutogenContext:
    context = MigrationContext.configure(dialect=ClickHouseDialect(), opts={"target_metadata": MetaData(), **opts})
    return AutogenContext(context, MetaData())


def _reflected(engine_full: str, name: str = "events", schema: str | None = None) -> Table:
    table = Table(name, MetaData(), Column("a", UInt32), Column("ts", DateTime), schema=schema)
    table.engine = build_engine(engine_full)
    table.kwargs["clickhousedb_engine"] = table.engine
    return table


def _declared(engine, name: str = "events", schema: str | None = None) -> Table:
    return Table(name, MetaData(), Column("a", UInt32), Column("ts", DateTime), engine, schema=schema)


def _compare(reflected: Table | None, declared: Table | None, schema: str | None = None, **opts) -> ops.ModifyTableOps:
    modify_table_ops = ops.ModifyTableOps("events", [], schema=schema)
    _compare_table_options(_autogen_context(**opts), modify_table_ops, schema, "events", reflected, declared)
    return modify_table_ops


_SERVER_ENGINE = (
    "MergeTree PARTITION BY toYYYYMM(ts) PRIMARY KEY a ORDER BY (a, ts) TTL ts + toIntervalDay(30) SETTINGS index_granularity = 8192"
)


@pytest.mark.parametrize(
    "engine",
    [
        MergeTree(order_by=("a", "ts"), partition_by="toYYYYMM(ts)", primary_key="a", ttl="ts + toIntervalDay(30)"),
        MergeTree(order_by=("`a`", "`ts`"), partition_by="toYYYYMM( `ts` )", primary_key="(a)", ttl="ts+toIntervalDay(30)"),
        MergeTree(
            order_by=["a", "ts"],
            partition_by="toYYYYMM(ts)",
            primary_key="a",
            ttl="ts + toIntervalDay(30)",
            settings={"index_granularity": 8192},
        ),
    ],
    ids=["plain", "quoted-and-spaced", "list-and-matching-setting"],
)
def test_matching_table_options_emit_nothing(engine):
    assert _compare(_reflected(_SERVER_ENGINE), _declared(engine)).is_empty()


def test_column_objects_in_keys_match_server_text():
    metadata = MetaData()
    a = Column("a", UInt32)
    ts = Column("ts", DateTime)
    declared = Table(
        "events", metadata, a, ts, MergeTree(order_by=(a, ts), partition_by="toYYYYMM(ts)", primary_key=a, ttl="ts + toIntervalDay(30)")
    )
    assert _compare(_reflected(_SERVER_ENGINE), declared).is_empty()


@pytest.mark.parametrize(
    ("server_engine", "declared_engine"),
    [
        ("MergeTree ORDER BY a SETTINGS index_granularity = 8192", MergeTree(primary_key="a")),
        ("MergeTree ORDER BY a SETTINGS index_granularity = 8192", MergeTree(order_by="a", primary_key="a")),
        ("MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192", MergeTree(order_by=())),
        ("MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192", MergeTree(order_by="tuple()")),
        ("MergeTree PARTITION BY (ts) ORDER BY (a) SETTINGS index_granularity = 8192", MergeTree(order_by="a", partition_by="ts")),
        (
            "SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') ORDER BY a SETTINGS index_granularity = 8192",
            MergeTree(order_by="a"),
        ),
        (
            "ReplicatedMergeTree('/clickhouse/tables/{shard}/events', '{replica}') ORDER BY a SETTINGS index_granularity = 8192",
            ReplicatedMergeTree(order_by="a"),
        ),
        ("ReplacingMergeTree(ts) ORDER BY a SETTINGS index_granularity = 8192", ReplacingMergeTree(version="ts", order_by="a")),
        ("SummingMergeTree((a)) ORDER BY ts SETTINGS index_granularity = 8192", SummingMergeTree(columns=["a"], order_by="ts")),
        ("Memory", Memory()),
        ("`Null`", Null()),
    ],
    ids=[
        "primary-key-only",
        "primary-equals-order",
        "empty-tuple",
        "tuple-call",
        "single-parens",
        "shared-cloud",
        "replicated-paths-ignored",
        "replacing-version",
        "summing-columns",
        "memory",
        "quoted-null",
    ],
)
def test_equivalent_engine_spellings_emit_nothing(server_engine, declared_engine):
    assert _compare(_reflected(server_engine), _declared(declared_engine)).is_empty()


@pytest.mark.parametrize(
    ("server_engine", "declared_engine", "option", "reflected", "declared"),
    [
        (_SERVER_ENGINE, MergeTree(order_by="a", partition_by="toYYYYMM(ts)", ttl="ts + toIntervalDay(30)"), "order_by", "(a, ts)", "a"),
        (
            _SERVER_ENGINE,
            MergeTree(order_by=("a", "ts"), primary_key="a", ttl="ts + toIntervalDay(30)"),
            "partition_by",
            "toYYYYMM(ts)",
            None,
        ),
        (
            _SERVER_ENGINE,
            MergeTree(order_by=("a", "ts"), partition_by="toYYYYMM(ts)", primary_key="a"),
            "ttl",
            "ts + toIntervalDay(30)",
            None,
        ),
        (
            _SERVER_ENGINE,
            MergeTree(order_by=("a", "ts"), partition_by="toYYYYMM(ts)", primary_key="a", ttl="ts + toIntervalDay(30)", sample_by="a"),
            "sample_by",
            None,
            "a",
        ),
        (
            _SERVER_ENGINE,
            MergeTree(order_by=("a", "ts"), partition_by="toYYYYMM(ts)", primary_key="ts", ttl="ts + toIntervalDay(30)"),
            "primary_key",
            "a",
            "ts",
        ),
        (
            _SERVER_ENGINE,
            ReplacingMergeTree(order_by=("a", "ts"), partition_by="toYYYYMM(ts)", primary_key="a", ttl="ts + toIntervalDay(30)"),
            "engine",
            "MergeTree",
            "ReplacingMergeTree",
        ),
        (
            "ReplacingMergeTree(ts) ORDER BY a SETTINGS index_granularity = 8192",
            ReplacingMergeTree(version="a", order_by="a"),
            "version",
            "ts",
            "a",
        ),
        ("ReplacingMergeTree(ts) ORDER BY a SETTINGS index_granularity = 8192", ReplacingMergeTree(order_by="a"), "version", "ts", None),
    ],
    ids=[
        "order-by",
        "partition-removed",
        "ttl-removed",
        "sample-added",
        "primary-key",
        "engine-family",
        "version-arg",
        "version-arg-removed",
    ],
)
def test_table_option_drift_raises(server_engine, declared_engine, option, reflected, declared):
    with pytest.raises(UnsupportedTableChangeError) as excinfo:
        _compare(_reflected(server_engine, schema="olap"), _declared(declared_engine, schema="olap"), schema="olap")
    assert excinfo.value.drifts == (TableOptionDrift("events", "olap", option, reflected, declared),)
    assert excinfo.value.drifts[0].qualified_name == "`olap`.`events`"
    message = str(excinfo.value)
    assert f"{option}: server {reflected!r}, model {declared!r}" in message
    assert COMPARE_TABLE_OPTIONS_OPT in message


def test_multiple_drifts_are_reported_together():
    declared = _declared(MergeTree(order_by="ts", partition_by="toYYYYMM(a)"))
    with pytest.raises(UnsupportedTableChangeError) as excinfo:
        _compare(_reflected(_SERVER_ENGINE), declared)
    assert [drift.option for drift in excinfo.value.drifts] == ["order_by", "partition_by", "primary_key", "ttl"]


def test_unknown_reflected_engine_is_drift():
    reflected = Table("events", MetaData(), Column("a", UInt32))
    with pytest.raises(UnsupportedTableChangeError) as excinfo:
        _compare(reflected, _declared(MergeTree(order_by="a")))
    assert excinfo.value.drifts == (TableOptionDrift("events", None, "engine", None, "MergeTree"),)


def test_opt_out_skips_comparison():
    declared = _declared(MergeTree(order_by="ts"))
    assert _compare(_reflected(_SERVER_ENGINE), declared, **{COMPARE_TABLE_OPTIONS_OPT: False}).is_empty()


def test_added_or_dropped_tables_are_not_compared():
    assert _compare(None, _declared(MergeTree(order_by="ts"))).is_empty()
    assert _compare(_reflected(_SERVER_ENGINE), None).is_empty()


def test_metadata_without_engine_is_not_compared():
    declared = Table("events", MetaData(), Column("a", UInt32))
    assert _compare(_reflected(_SERVER_ENGINE), declared).is_empty()


def test_settings_change_emits_reversible_modify_op():
    declared = _declared(
        MergeTree(
            order_by=("a", "ts"),
            partition_by="toYYYYMM(ts)",
            primary_key="a",
            ttl="ts + toIntervalDay(30)",
            settings={"merge_with_ttl_timeout": 7200, "ttl_only_drop_parts": True, "storage_policy": "default"},
        )
    )
    modify_table_ops = _compare(_reflected(_SERVER_ENGINE + ", merge_with_ttl_timeout = 3600, storage_policy = 'default'"), declared)
    (operation,) = modify_table_ops.ops
    assert isinstance(operation, _ModifyClickHouseTableSettingsOp)
    assert operation.settings == {"merge_with_ttl_timeout": 7200, "ttl_only_drop_parts": True}
    assert operation.existing_settings == {"merge_with_ttl_timeout": 3600, "ttl_only_drop_parts": None}
    assert operation.to_diff_tuple() == (
        "modify_clickhouse_table_settings",
        None,
        "events",
        {"merge_with_ttl_timeout": 7200, "ttl_only_drop_parts": True},
        {"merge_with_ttl_timeout": 3600, "ttl_only_drop_parts": None},
    )

    reverse = operation.reverse()
    assert isinstance(reverse, ops.ModifyTableOps)
    restore, reset = reverse.ops
    assert isinstance(restore, _ModifyClickHouseTableSettingsOp) and restore.settings == {"merge_with_ttl_timeout": 3600}
    assert isinstance(reset, _ResetClickHouseTableSettingsOp) and reset.names == ("ttl_only_drop_parts",)


@pytest.mark.parametrize("setting", ["index_granularity", "index_granularity_bytes"])
def test_readonly_setting_change_is_drift(setting):
    declared = _declared(
        MergeTree(
            order_by=("a", "ts"), partition_by="toYYYYMM(ts)", primary_key="a", ttl="ts + toIntervalDay(30)", settings={setting: 1024}
        )
    )
    with pytest.raises(UnsupportedTableChangeError) as excinfo:
        _compare(_reflected(_SERVER_ENGINE), declared)
    reflected = "8192" if setting == "index_granularity" else None
    assert excinfo.value.drifts == (TableOptionDrift("events", None, f"settings.{setting}", reflected, "1024"),)


def test_settings_reverse_single_op_and_missing_history():
    restore_only = _ModifyClickHouseTableSettingsOp(
        "events", {"merge_with_ttl_timeout": 7200}, existing_settings={"merge_with_ttl_timeout": 3600}
    )
    reverse = restore_only.reverse()
    assert isinstance(reverse, _ModifyClickHouseTableSettingsOp) and reverse.settings == {"merge_with_ttl_timeout": 3600}
    reset_only = _ModifyClickHouseTableSettingsOp("events", {"ttl_only_drop_parts": 1}, existing_settings={"ttl_only_drop_parts": None})
    reverse = reset_only.reverse()
    assert isinstance(reverse, _ResetClickHouseTableSettingsOp) and reverse.names == ("ttl_only_drop_parts",)
    with pytest.raises(NotImplementedError):
        _ModifyClickHouseTableSettingsOp("events", {"merge_with_ttl_timeout": 7200}).reverse()


def test_server_only_settings_are_ignored():
    declared = _declared(MergeTree(order_by=("a", "ts"), partition_by="toYYYYMM(ts)", primary_key="a", ttl="ts + toIntervalDay(30)"))
    assert _compare(_reflected(_SERVER_ENGINE + ", ttl_only_drop_parts = 1"), declared).is_empty()


@pytest.mark.parametrize("kind", ["view", "materialized_view", "window_view", "live_view"])
def test_include_object_skips_reflected_views(kind):
    view = Table("daily", MetaData(), Column("a", String), schema="olap", clickhousedb_table_type=kind)
    assert include_object(view, "daily", "table", True, None) is False
    # Declared metadata is never filtered on kind, and tables stay visible.
    assert include_object(view, "daily", "table", False, None) is True
    table = Table("events", MetaData(), Column("a", String), schema="olap", clickhousedb_engine=MergeTree(order_by="a"))
    assert include_object(table, "events", "table", True, None) is True


class _StubConnection:
    """Answers formatQuerySingleLine and merge_tree_settings lookups from canned rows."""

    def __init__(
        self,
        formatted: dict[str, str] | None = None,
        defaults: dict[str, str] | None = None,
        fail: bool = False,
        types: dict[str, str] | None = None,
    ):
        self.formatted = formatted or {}
        self.defaults = defaults or {}
        self.fail = fail
        self.types = types or {}
        self.statements: list[str] = []

    def execute(self, statement, parameters):
        self.statements.append(str(statement))
        if self.fail:
            raise OperationalError("SELECT", parameters, Exception("server gone"))
        if "merge_tree_settings" in str(statement):
            rows = [
                SimpleNamespace(
                    name=name, value=self.defaults[name], type=self.types.get(name, "String" if name == "storage_policy" else "UInt64")
                )
                for name in parameters["names"]
                if name in self.defaults
            ]
            return SimpleNamespace(fetchall=lambda: rows)
        values = [parameters[key] for key in sorted(parameters)]
        prefix = "ALTER TABLE t MODIFY TTL " if "ALTER TABLE t MODIFY TTL " in str(statement) else "SELECT "
        return SimpleNamespace(fetchone=lambda: tuple(f"{prefix}{self.formatted.get(value, value)}" for value in values))


def _compare_with(connection, reflected: Table, declared: Table) -> ops.ModifyTableOps:
    context = MigrationContext.configure(dialect=ClickHouseDialect(), opts={"target_metadata": MetaData()})
    autogen_context = AutogenContext(context, MetaData())
    autogen_context.connection = connection
    modify_table_ops = ops.ModifyTableOps("events", [])
    _compare_table_options(autogen_context, modify_table_ops, None, "events", reflected, declared)
    return modify_table_ops


@pytest.mark.parametrize(
    ("server_engine", "declared_engine"),
    [
        ("Distributed('c1', 'db', 't')", Distributed(cluster="'c1'", database="'db'", table="'t'")),
        ("Distributed('c1', 'db', 't', rand())", Distributed(cluster="c1", database="db", table="t", sharding_key="rand()")),
        ("File('CSV')", File(fmt="'CSV'")),
        ("ReplacingMergeTree(ts) ORDER BY a SETTINGS index_granularity = 8192", ReplicatedReplacingMergeTree(ver="ts", order_by="a")),
        (
            "SharedReplacingMergeTree('/x/{uuid}', '{replica}', ts) ORDER BY a SETTINGS index_granularity = 8192",
            ReplicatedReplacingMergeTree(ver="ts", order_by="a"),
        ),
        (
            "ReplicatedReplacingMergeTree('/x', '{replica}', ts) ORDER BY a SETTINGS index_granularity = 8192",
            ReplacingMergeTree(version="ts", order_by="a"),
        ),
    ],
    ids=["distributed-quoted", "distributed-unquoted", "file-quoted", "replicated-vs-plain", "replicated-vs-shared", "plain-vs-replicated"],
)
def test_positional_args_pair_across_quoting_and_families(server_engine, declared_engine):
    assert _compare(_reflected(server_engine), _declared(declared_engine)).is_empty()


@pytest.mark.parametrize("engine_name", ["GraphiteMergeTree", "Distributed"])
@pytest.mark.parametrize(
    ("reflected_arg", "declared_arg", "matches"),
    [
        ("'rollup-cold'", "rollup - cold", False),
        ("'events-v2'", "'events - v2'", False),
        ("'rollup.cold'", "rollup . cold", False),
        ("'`rollup`'", "rollup", False),
        ("'(rollup)'", "rollup", False),
        ("' rollup '", "rollup", False),
        ("'rollup-cold'", "rollup-cold", True),
        ("'events-v2'", "'events-v2'", True),
        (r"'rollup\\cold'", r"rollup\cold", True),
        (r"'rollup\ncold'", "rollup\ncold", True),
        (r"'rollup\'cold'", "'rollup''cold'", True),
        ("'rollup''cold'", "rollup'cold", True),
        ("'rollup-cold'", r"'rollup\x2dcold'", True),
        ("'rollupx2dcold'", r"'rollup\x2dcold'", False),
        (r"'rollup\x2dcold'", "rollup-cold", True),
        (r"'rollup\\x2dcold'", r"rollup\x2dcold", True),
        (r"'rollup\qcold'", r"rollup\qcold", True),
        (r"'rollup\qcold'", "rollupqcold", False),
        (r"'rollup\Ncold'", "rollupcold", True),
        (r"'rollup\a\e\vcold'", "rollup\a\x1b\vcold", True),
        (r"'rollup\xc3\xa9'", "rollup\u00e9", True),
        (r"'rollup\'''cold'", "rollup''cold", True),
        (r"'rollup\xff'", r"'rollup\xff'", True),
        ("'rollup'", r"'rollup\xff'", False),
    ],
)
def test_literal_engine_args_preserve_contents(engine_name, reflected_arg, declared_arg, matches):
    if engine_name == "GraphiteMergeTree":
        reflected = _reflected(f"GraphiteMergeTree({reflected_arg}) ORDER BY a")
        declared = _declared(GraphiteMergeTree(config_section=declared_arg, order_by="a"))
        option = "config_section"
    else:
        reflected = _reflected(f"Distributed('cluster', 'database', {reflected_arg})")
        declared = _declared(Distributed(cluster="cluster", database="database", table=declared_arg))
        option = "table"
    connection = _StubConnection()
    if matches:
        assert _compare_with(connection, reflected, declared).is_empty()
    else:
        with pytest.raises(UnsupportedTableChangeError) as excinfo:
            _compare_with(connection, reflected, declared)
        assert [drift.option for drift in excinfo.value.drifts] == [option]
    assert not connection.statements


@pytest.mark.parametrize(
    ("reflected_arg", "declared_arg"),
    [
        ("rand( )", "rand()"),
        ("cityHash64( `a` )", "cityHash64(a)"),
        ("'part_a' || 'part_b'", "'part_a'||'part_b'"),
    ],
)
def test_unquoted_engine_args_keep_expression_comparison(reflected_arg, declared_arg):
    reflected = _reflected(f"Distributed('cluster', 'database', 'events', {reflected_arg})")
    declared = _declared(Distributed(cluster="cluster", database="database", table="events", sharding_key=declared_arg))
    assert _compare(reflected, declared).is_empty()


def test_positional_arg_drift_across_families_uses_declared_name():
    declared = _declared(ReplicatedReplacingMergeTree(ver="a", order_by="a"))
    with pytest.raises(UnsupportedTableChangeError) as excinfo:
        _compare(_reflected("ReplacingMergeTree(ts) ORDER BY a SETTINGS index_granularity = 8192"), declared)
    assert excinfo.value.drifts == (TableOptionDrift("events", None, "ver", "ts", "a"),)


def test_server_formatting_settles_spelling_differences():
    connection = _StubConnection(formatted={"ts + INTERVAL 30 DAY": "ts + toIntervalDay(30)"})
    declared = _declared(MergeTree(order_by=("a", "ts"), partition_by="toYYYYMM(ts)", primary_key="a", ttl="ts + INTERVAL 30 DAY"))
    assert _compare_with(connection, _reflected(_SERVER_ENGINE), declared).is_empty()
    assert any("formatQuerySingleLine" in statement for statement in connection.statements)


def test_server_formatting_failure_falls_back_to_textual_drift(caplog):
    declared = _declared(MergeTree(order_by=("a", "ts"), partition_by="toYYYYMM(ts)", primary_key="a", ttl="ts + INTERVAL 30 DAY"))
    with caplog.at_level(logging.WARNING, logger="clickhouse_connect.cc_sqlalchemy.alembic.compare"):
        with pytest.raises(UnsupportedTableChangeError) as excinfo:
            _compare_with(_StubConnection(fail=True), _reflected(_SERVER_ENGINE), declared)
    assert excinfo.value.drifts[0].option == "ttl"
    assert "could not normalize" in caplog.text


def test_declared_server_defaults_produce_no_change():
    connection = _StubConnection(defaults={"index_granularity_bytes": "10485760", "ttl_only_drop_parts": "0", "storage_policy": "default"})
    declared = _declared(
        MergeTree(
            order_by=("a", "ts"),
            partition_by="toYYYYMM(ts)",
            primary_key="a",
            ttl="ts + toIntervalDay(30)",
            settings={"index_granularity_bytes": 10485760, "ttl_only_drop_parts": False, "storage_policy": "default"},
        )
    )
    assert _compare_with(connection, _reflected(_SERVER_ENGINE), declared).is_empty()


def test_readonly_setting_differing_from_server_default_is_drift():
    connection = _StubConnection(defaults={"index_granularity_bytes": "10485760"})
    declared = _declared(
        MergeTree(
            order_by=("a", "ts"),
            partition_by="toYYYYMM(ts)",
            primary_key="a",
            ttl="ts + toIntervalDay(30)",
            settings={"index_granularity_bytes": 1024},
        )
    )
    with pytest.raises(UnsupportedTableChangeError) as excinfo:
        _compare_with(connection, _reflected(_SERVER_ENGINE), declared)
    assert excinfo.value.drifts == (TableOptionDrift("events", None, "settings.index_granularity_bytes", "10485760", "1024"),)


@pytest.mark.parametrize(
    ("name", "server", "model", "setting_type"),
    [
        ("ttl_only_drop_parts", "true", True, "Bool"),
        ("ttl_only_drop_parts", "false", False, "Bool"),
        ("ttl_only_drop_parts", "1", "true", "Bool"),
        ("ttl_only_drop_parts", "true", "TRUE", "Bool"),
        ("index_granularity", "'8192'", 8192, "UInt64"),
        ("merge_with_ttl_timeout", "'3600'", 3600, "Seconds"),
        ("max_bytes_to_merge_at_max_space_in_pool", "1", 1.0, "UInt64"),
        ("merge_with_ttl_timeout", "3600", "3600.0", "Seconds"),
    ],
)
def test_equivalent_setting_values_emit_nothing(name, server, model, setting_type):
    reflected = _reflected(f"MergeTree ORDER BY a SETTINGS {name} = {server}")
    declared = _declared(MergeTree(order_by="a", settings={name: model}))
    connection = _StubConnection(defaults={name: "0"}, types={name: setting_type})
    assert _compare_with(connection, reflected, declared).is_empty()


@pytest.mark.parametrize(
    ("name", "model", "setting_type"),
    [
        ("ttl_only_drop_parts", "01", "Bool"),
        ("ttl_only_drop_parts", "1.0", "Bool"),
        ("merge_with_ttl_timeout", "true", "Seconds"),
    ],
)
def test_invalid_setting_strings_are_not_equivalent_to_numbers(name, model, setting_type):
    reflected = _reflected(f"MergeTree ORDER BY a SETTINGS {name} = 1")
    declared = _declared(MergeTree(order_by="a", settings={name: model}))
    (operation,) = _compare_with(_StubConnection(defaults={name: "0"}, types={name: setting_type}), reflected, declared).ops
    assert operation.settings == {name: model}


@pytest.mark.parametrize("value", ["true", "01", "1.0", "user_'1\\path"])
def test_string_setting_values_keep_their_meaning_and_downgrade(value):
    connection = _StubConnection(defaults={"storage_policy": "default"}, types={"storage_policy": "String"})
    reflected = _declared(MergeTree(order_by="a", settings={"storage_policy": value}))
    same = _declared(MergeTree(order_by="a", settings={"storage_policy": value}))
    assert _compare_with(connection, reflected, same).is_empty()
    changed = _declared(MergeTree(order_by="a", settings={"storage_policy": "other"}))
    (operation,) = _compare_with(connection, reflected, changed).ops
    assert operation.reverse().settings == {"storage_policy": value}


def test_boolean_setting_downgrade_restores_a_boolean_literal():
    reflected = _reflected("MergeTree ORDER BY a SETTINGS ttl_only_drop_parts = true")
    declared = _declared(MergeTree(order_by="a", settings={"ttl_only_drop_parts": False}))
    (operation,) = _compare_with(
        _StubConnection(defaults={"ttl_only_drop_parts": "0"}, types={"ttl_only_drop_parts": "Bool"}), reflected, declared
    ).ops
    assert operation.existing_settings == {"ttl_only_drop_parts": True}
    assert operation.reverse().settings == {"ttl_only_drop_parts": True}


def test_unsupported_table_change_requires_a_drift():
    with pytest.raises(ValueError, match="at least one drift"):
        UnsupportedTableChangeError([])


@pytest.mark.parametrize(
    ("left", "right"),
    [
        ("`a b`", "`ab`"),
        ("`a.b`", "a.b"),
        ("`a``b`", "ab"),
        ("`a\\`b`", "ab"),
        ("`NULL`", "NULL"),
        ("a AND b", "aANDb"),
        ("NOT a", "NOTa"),
        ("a IS NULL", "aISNULL"),
        ("`a`AND`b`", "aANDb"),
        ("a`b`", "ab"),
        ("`a`b", "ab"),
        (r"`a\tb`", "atb"),
        (r"`a\nb`", "anb"),
        (r"`a\0b`", "a0b"),
        (r"`a\x41b`", "ax41b"),
        (r"`a\qb`", "aqb"),
        ("a - -b", "a--b"),
        ("a / *b", "a/*b"),
        ("a < = b", "a<=b"),
        ("a | | b", "a||b"),
        ("1 . 3", "1.3"),
        ("a--comment\n+b", "a--comment+b"),
        ("a//comment\n+b", "a//comment+b"),
        ("a# comment\n+b", "a# comment+b"),
    ],
)
def test_normalization_keeps_distinct_identifiers(left, right):
    assert normalize_expression(left) != normalize_expression(right)


@pytest.mark.parametrize("fail", [False, True], ids=["missing-type", "failed-lookup"])
@pytest.mark.parametrize("previous", ["01", "true", "1.0"])
def test_unknown_setting_type_preserves_string_values_and_downgrade(fail, previous):
    connection = _StubConnection(fail=fail)
    reflected = _declared(MergeTree(order_by="a", settings={"storage_policy": previous}))
    declared = _declared(MergeTree(order_by="a", settings={"storage_policy": "1"}))
    (operation,) = _compare_with(connection, reflected, declared).ops
    assert operation.existing_settings == {"storage_policy": previous}
    assert operation.reverse().settings == {"storage_policy": previous}


@pytest.mark.parametrize("fail", [False, True], ids=["missing-type", "failed-lookup"])
def test_unknown_readonly_setting_type_diagnostic_keeps_literal_types(fail):
    reflected = _reflected("MergeTree ORDER BY a SETTINGS index_granularity = '4096'")
    declared = _declared(MergeTree(order_by="a", settings={"index_granularity": 4096}))
    with pytest.raises(UnsupportedTableChangeError) as excinfo:
        _compare_with(_StubConnection(fail=fail), reflected, declared)
    (drift,) = excinfo.value.drifts
    assert (drift.reflected, drift.declared) == ("'4096'", "4096")
    assert f"server {drift.reflected!r}, model {drift.declared!r}" in str(excinfo.value)


@pytest.mark.parametrize(
    ("expression", "spelling"),
    [("a AND b", " `a`   AND  `b` "), ("NOT a", "NOT `a`"), ("a IS NULL", "`a` IS  NULL")],
)
def test_equivalent_token_boundaries_match(expression, spelling):
    assert normalize_expression(expression) == normalize_expression(spelling)


@pytest.mark.parametrize(
    "action",
    ["DELETE", "DELETE WHERE a = 13", "TO VOLUME 'cold'", "TO DISK 'cold'", "GROUP BY a SET ts = max(ts)", "RECOMPRESS CODEC(ZSTD(3))"],
)
def test_ttl_actions_use_alter_table_formatter(action):
    declared_ttl = f"ts + INTERVAL 30 DAY {action}"
    reflected_ttl = f"ts + toIntervalDay(30) {action}"
    connection = _StubConnection(formatted={declared_ttl: reflected_ttl})
    reflected = _reflected(f"MergeTree ORDER BY a TTL {reflected_ttl}")
    declared = _declared(MergeTree(order_by="a", ttl=declared_ttl))
    assert _compare_with(connection, reflected, declared).is_empty()
    assert "ALTER TABLE t MODIFY TTL " in connection.statements[-1]
