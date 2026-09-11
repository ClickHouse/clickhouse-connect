from io import StringIO

import pytest
import sqlalchemy as sa
from alembic.autogenerate.api import render_python_code
from alembic.operations import Operations, ops
from alembic.runtime.migration import MigrationContext
from sqlalchemy.exc import ArgumentError
from sqlalchemy.orm import declarative_base

from clickhouse_connect.cc_sqlalchemy import engines, types
from clickhouse_connect.cc_sqlalchemy.alembic import clickhouse_writer
from clickhouse_connect.cc_sqlalchemy.ddl.tableengine import build_engine
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect

simple_engines = (engines.Memory, engines.Log, engines.StripeLog, engines.TinyLog, engines.Null, engines.Set)
summing_engines = (engines.SummingMergeTree, engines.ReplicatedSummingMergeTree, engines.SharedSummingMergeTree)


@pytest.mark.parametrize("engine_cls", simple_engines)
def test_simple_engine_construction(engine_cls):
    engine = engine_cls()
    assert engine.compile() == f"Engine {engine_cls.__name__}"
    assert repr(engine) == f"{engine_cls.__name__}()"
    assert eval(repr(engine), vars(engines)).compile() == engine.compile()


@pytest.mark.parametrize("engine_cls", simple_engines)
@pytest.mark.parametrize("keyword", [False, True])
def test_simple_engine_legacy_mapping(engine_cls, keyword):
    kwargs = {"settings": {"persistent": 0}}
    engine = engine_cls(kwargs=kwargs) if keyword else engine_cls(kwargs)
    assert engine.compile() == f"Engine {engine_cls.__name__} SETTINGS persistent = 0"
    assert eval(repr(engine), vars(engines)).compile() == engine.compile()


@pytest.mark.parametrize("settings", [None, {}, {"max_rows_to_keep": 79}])
def test_memory_settings_keyword(settings):
    kwargs = {"settings": {"max_rows_to_keep": 13}}
    engine = engines.Memory(kwargs, settings=settings)
    expected = {"max_rows_to_keep": 13} if settings is None else settings
    assert engine.settings == expected
    assert eval(repr(engine), vars(engines)).compile() == engine.compile()
    assert engines.Memory(settings=settings).settings == (settings or {})


@pytest.mark.parametrize("engine_cls", summing_engines)
@pytest.mark.parametrize(
    "columns,expected",
    [
        (None, ""),
        ("amount", "(amount)"),
        ("(amount, count)", "((amount, count))"),
        (["amount"], "((`amount`))"),
        (("amount", "count"), "((`amount`, `count`))"),
        (sa.Column("amount", types.UInt64), "(`amount`)"),
        ([sa.Column("amount", types.UInt64), "count"], "((`amount`, `count`))"),
        ((sa.Column("net amount", types.UInt64), "count, total"), "((`net amount`, `count, total`))"),
        (["select", "tick`name", "back\\name"], "((`select`, `tick\\`name`, `back\\\\name`))"),
        (['"amount"', "`count`"], '((`"amount"`, `\\`count\\``))'),
        (sa.Column('"amount"', types.UInt64), '(`"amount"`)'),
    ],
)
def test_summing_columns(engine_cls, columns, expected):
    engine = engine_cls("id", columns=columns)
    assert engine.compile() == f"Engine {engine_cls.__name__}{expected} ORDER BY id"
    assert eval(repr(engine), vars(engines)).compile() == engine.compile()


@pytest.mark.parametrize("engine_cls", summing_engines)
def test_summing_orm_columns(engine_cls):
    base = declarative_base()

    class Metric(base):
        __tablename__ = "metric"
        id = sa.Column(types.UInt64, primary_key=True)
        amount = sa.Column("net amount", types.UInt64)

    scalar = engine_cls("id", columns=Metric.amount)
    assert scalar.compile() == f"Engine {engine_cls.__name__}(`net amount`) ORDER BY id"
    multiple = engine_cls("id", columns=[Metric.amount, "count"])
    assert multiple.compile() == f"Engine {engine_cls.__name__}((`net amount`, `count`)) ORDER BY id"
    assert eval(repr(multiple), vars(engines)).compile() == multiple.compile()


@pytest.mark.parametrize("engine_cls", summing_engines)
@pytest.mark.parametrize(
    "columns",
    [
        [],
        (),
        13,
        ["amount", 79],
        [["amount"]],
        sa.text("amount"),
        sa.column("amount") + 1,
        sa.func.sum(sa.column("amount")),
        sa.Column(types.UInt64),
        [sa.Column(types.UInt64)],
    ],
)
def test_summing_columns_reject_invalid_inputs(engine_cls, columns):
    with pytest.raises(ArgumentError, match="columns"):
        engine_cls("id", columns=columns)


@pytest.mark.parametrize("engine_cls", summing_engines)
def test_summing_preserves_merge_tree_contract(engine_cls):
    base = engines.ReplicatedMergeTree if engine_cls is engines.ReplicatedSummingMergeTree else engines.MergeTree
    assert issubclass(engine_cls, base)
    positional = ["id", "id", "toYYYYMM(ts)", "id"]
    if base is engines.ReplicatedMergeTree:
        positional += ["/tables/metrics", "replica_1"]
    positional += ["ts + INTERVAL 1 DAY", {"index_granularity": 1024}]
    engine = engine_cls(*positional)
    assert engine.compile() == base(*positional).compile().replace(f"Engine {base.__name__}", f"Engine {engine_cls.__name__}", 1)
    assert eval(repr(engine), vars(engines)).compile() == engine.compile()
    with pytest.raises(ArgumentError, match="Either PRIMARY KEY or ORDER BY"):
        engine_cls()


@pytest.mark.parametrize("prefix", ["", "Replicated", "Shared"])
@pytest.mark.parametrize("columns", ["", "amount", "(amount, count)", "(`net amount`, `count, total`)"])
def test_reflected_summing_columns(prefix, columns):
    args = ["'/tables/metrics'", "'replica_1'"] if prefix else []
    if columns:
        args.append(columns)
    argument_sql = f"({', '.join(args)})" if args else ""
    reflected = build_engine(f"{prefix}SummingMergeTree{argument_sql} ORDER BY id SETTINGS index_granularity = 1024")
    assert reflected is not None
    if prefix == "Shared":
        args = [columns] if columns else []
        prefix = ""
    argument_sql = f"({', '.join(args)})" if args else ""
    reconstructed = eval(repr(reflected), vars(engines))
    assert reconstructed.compile() == f"Engine {prefix}SummingMergeTree{argument_sql} ORDER BY id SETTINGS index_granularity = 1024"


@pytest.mark.parametrize(
    "engine_sql",
    [
        *(cls.__name__ for cls in simple_engines),
        "Memory SETTINGS max_rows_to_keep = 13",
        "SummingMergeTree((`net amount`, count)) ORDER BY id",
    ],
)
def test_reflected_engine_alembic_execution(engine_sql):
    engine = build_engine(engine_sql)
    table = sa.Table(
        "metrics",
        sa.MetaData(),
        sa.Column("id", types.UInt64),
        sa.Column("net amount", types.UInt64),
        sa.Column("count", types.UInt64),
        clickhouse_engine=engine,
    )
    context = MigrationContext.configure(dialect=ClickHouseDialect())
    directive = ops.MigrationScript("rev_1", ops.UpgradeOps([ops.CreateTableOp.from_table(table)]), ops.DowngradeOps([]))
    clickhouse_writer(context, (), [directive])
    generated = render_python_code(directive.upgrade_ops, migration_context=context)
    output = StringIO()
    offline = MigrationContext.configure(dialect=ClickHouseDialect(), opts={"as_sql": True, "output_buffer": output})
    namespace = {"sa": sa, "op": Operations(offline), **vars(engines), **vars(types)}
    exec("def upgrade():\n" + generated + "\nupgrade()", namespace)
    assert "Engine " + engine_sql in output.getvalue()
