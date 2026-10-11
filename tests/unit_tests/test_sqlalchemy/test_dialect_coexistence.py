import os
import subprocess
import sys
import textwrap
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

import pytest
import sqlalchemy as sa
from sqlalchemy.schema import CreateTable

from clickhouse_connect.cc_sqlalchemy import inspector as inspector_module
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect


@pytest.fixture
def foreign_dialect_path(tmp_path):
    """Provide an entry point with the legacy driver's accepted schema options."""
    (tmp_path / "foreign_dialect.py").write_text(
        textwrap.dedent(
            """
            import sqlite3
            from sqlalchemy import Column, Table
            from sqlalchemy.engine.default import DefaultDialect

            class ForeignDialect(DefaultDialect):
                name = "clickhouse"
                driver = "foreign"
                construct_arguments = [
                    (Table, {"cluster": None, "data": []}),
                    (Column, {"codec": "LZ4", "materialized": None, "alias": None, "after": None}),
                ]

                @classmethod
                def dbapi(cls):
                    return sqlite3

                @classmethod
                def import_dbapi(cls):
                    return sqlite3
            """
        ),
        encoding="utf-8",
    )
    return tmp_path


def _run_isolated(script, provider_path, *args):
    # Collection imports our dialect, so test process-global registration in a fresh interpreter.
    env = os.environ.copy()
    repo_root = Path(__file__).resolve().parents[3]
    env["PYTHONPATH"] = os.pathsep.join((str(provider_path), str(repo_root), env.get("PYTHONPATH", "")))
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(script), *args],
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
        env=env,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("trigger", ["import", "registered_import", "registered_load"])
def test_aliases_without_foreign_provider(tmp_path, trigger):
    _run_isolated(
        """
        import importlib
        import importlib.metadata as metadata
        import importlib.util
        import sys
        import warnings
        import sqlalchemy as sa
        from sqlalchemy.dialects import registry
        from sqlalchemy.schema import CreateTable

        # Simulate cc-only installation even when a developer also installed the legacy driver.
        dialect_entries = metadata.EntryPoints(
            ep for ep in metadata.entry_points(group="sqlalchemy.dialects")
            if ep.name != "clickhouse"
        )
        metadata.entry_points = lambda **kwargs: dialect_entries.select(**kwargs)
        trigger = sys.argv[1]
        if trigger != "import":
            registry.register("clickhouse", "clickhouse_connect.cc_sqlalchemy.dialect", "ClickHouseDialect")
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            if trigger == "registered_load":
                registry.load("clickhouse")
            import clickhouse_connect.cc_sqlalchemy as cc
        assert not [item for item in caught if "clickhouse://" in str(item.message)]
        from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect
        for name in ("clickhouse", "clickhouse.connect", "clickhousedb", "clickhousedb.connect"):
            assert registry.load(name) is ClickHouseDialect
        if tuple(int(part) for part in sa.__version__.split(".")[:3]) >= (2, 0, 44) and importlib.util.find_spec("greenlet") is not None:
            assert registry.load("clickhouse.async") is registry.load("clickhousedb.async")

        sa.Column.argument_for("clickhousedb", "codec", "ZSTD(3)")
        table = sa.Table(
            "alias_default", sa.MetaData(), sa.Column("id", cc.types.UInt32),
            clickhouse_engine=cc.engines.Memory(),
        )
        sql = str(CreateTable(table).compile(dialect=ClickHouseDialect()))
        assert "CODEC(ZSTD(3))" in sql
        assert "Engine Memory" in sql
        assert str(CreateTable(table.to_metadata(sa.MetaData())).compile(dialect=ClickHouseDialect())) == sql
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            importlib.reload(cc)
        assert not [item for item in caught if "clickhouse://" in str(item.message)]
        assert str(CreateTable(table).compile(dialect=ClickHouseDialect())) == sql
        """,
        tmp_path,
        trigger,
    )


@pytest.mark.parametrize("kind", ["table", "dictionary"])
def test_canonical_schema_and_urls_without_distribution_metadata(tmp_path, kind):
    _run_isolated(
        """
        import importlib.metadata as metadata
        import sys
        import sqlalchemy as sa
        from sqlalchemy.dialects import registry
        from sqlalchemy.schema import CreateTable

        # PYTHONPATH checkouts and frozen applications need runtime registrations.
        metadata.entry_points = lambda **kwargs: metadata.EntryPoints(())
        import clickhouse_connect.cc_sqlalchemy as cc
        from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect

        columns = [sa.Column("id", cc.types.UInt32)]
        if sys.argv[1] == "table":
            table = sa.Table("events", sa.MetaData(), *columns, cc.engines.Memory())
        else:
            table = cc.Dictionary(
                "lookup", sa.MetaData(), *columns,
                source="CLICKHOUSE(TABLE 'source')", layout="FLAT", lifetime="MIN 0 MAX 79",
                primary_key="id",
            )
        assert all(key.startswith("clickhousedb_") for key in dict(table.kwargs))
        sql = str(CreateTable(table).compile(dialect=ClickHouseDialect()))
        assert str(CreateTable(table.to_metadata(sa.MetaData())).compile(dialect=ClickHouseDialect())) == sql
        for scheme in ("clickhousedb", "clickhousedb+connect", "clickhouse+connect"):
            assert sa.engine.make_url(scheme + "://").get_dialect() is ClickHouseDialect
        for name in ("clickhousedb", "clickhousedb.connect"):
            assert registry.load(name) is ClickHouseDialect
        """,
        tmp_path,
        kind,
    )


def test_existing_runtime_registration_is_not_loaded_or_warned(tmp_path):
    _run_isolated(
        """
        import warnings
        import sqlalchemy as sa
        from sqlalchemy.dialects import registry
        from sqlalchemy.schema import CreateTable

        loads = []
        def foreign_loader():
            loads.append(True)
            raise AssertionError("foreign provider must not be loaded")
        registry.impls["clickhouse"] = foreign_loader
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            import clickhouse_connect.cc_sqlalchemy as cc
        assert registry.impls["clickhouse"] is foreign_loader
        assert not [item for item in caught if "clickhouse://" in str(item.message)]
        engine = sa.create_engine("clickhousedb://")
        table = sa.Table("canonical", sa.MetaData(), sa.Column("id", cc.types.UInt32), cc.engines.Memory())
        assert "Engine Memory" in str(CreateTable(table.to_metadata(sa.MetaData())).compile(dialect=engine.dialect))
        engine.dispose()
        assert not loads
        """,
        tmp_path,
    )


@pytest.mark.parametrize("provider", ["entry_point", "runtime"])
@pytest.mark.parametrize("cache", ["cold", "warm"])
def test_foreign_dialect_coexistence(foreign_dialect_path, provider, cache):
    if provider == "entry_point":
        distribution = foreign_dialect_path / "foreign_clickhouse-0.0.dist-info"
        distribution.mkdir()
        (distribution / "METADATA").write_text("Name: foreign-clickhouse\nVersion: 0.0\n", encoding="utf-8")
        (distribution / "entry_points.txt").write_text(
            "[sqlalchemy.dialects]\nclickhouse = foreign_dialect:ForeignDialect\n", encoding="utf-8"
        )

    _run_isolated(
        """
        import importlib
        import sys
        import warnings
        from io import StringIO
        import sqlalchemy as sa
        from sqlalchemy.dialects import registry
        from sqlalchemy.exc import ArgumentError
        from sqlalchemy.schema import CreateTable

        provider, cache = sys.argv[1:]
        if provider == "runtime":
            registry.register("clickhouse", "foreign_dialect", "ForeignDialect")
        before = None
        if cache == "warm":
            before = sa.Table(
                "before", sa.MetaData(),
                sa.Column("id", sa.Integer, clickhouse_codec="ZSTD(3)"),
                clickhouse_cluster="cluster_1",
            )
            table_defaults = dict(before.dialect_options["clickhouse"])
            column_defaults = dict(before.c.id.dialect_options["clickhouse"])
        else:
            assert "foreign_dialect" not in sys.modules

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            import clickhouse_connect.cc_sqlalchemy as cc
        notices = [item for item in caught if "clickhouse://" in str(item.message)]
        assert len(notices) == int(provider == "entry_point" and cache == "cold"), caught
        if notices:
            assert "clickhousedb://" in str(notices[0].message)
        if cache == "cold":
            assert "foreign_dialect" not in sys.modules

        from foreign_dialect import ForeignDialect
        from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect
        from clickhouse_connect.cc_sqlalchemy.sql.ddlcompiler import ClickHouseDDLHelper
        assert sa.engine.make_url("clickhouse://").get_dialect() is ForeignDialect
        after_engine = sa.create_engine("clickhouse://")
        assert type(after_engine.dialect) is ForeignDialect
        after_engine.dispose()
        if before is not None:
            assert dict(before.dialect_options["clickhouse"]) == table_defaults
            assert dict(before.c.id.dialect_options["clickhouse"]) == column_defaults
            assert before.to_metadata(sa.MetaData()).kwargs["clickhouse_cluster"] == "cluster_1"
        assert set(ForeignDialect.construct_arguments[0][1]) == {"cluster", "data"}
        assert set(ForeignDialect.construct_arguments[1][1]) == {"codec", "materialized", "alias", "after"}
        for scheme in ("clickhousedb", "clickhousedb+connect", "clickhouse+connect"):
            assert sa.engine.make_url(scheme + "://").get_dialect() is ClickHouseDialect
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            importlib.reload(cc)
        assert not [item for item in caught if "clickhouse://" in str(item.message)]

        # cc-only legacy keywords must use our namespace in a mixed install.
        for key in ("engine", "table_type", "dictionary_source", "dictionary_layout", "dictionary_lifetime", "dictionary_primary_key"):
            try:
                sa.Table("invalid", sa.MetaData(), **{"clickhouse_" + key: "value"})
            except ArgumentError as exc:
                assert "clickhouse_" + key in str(exc)
            else:
                raise AssertionError(key + " unexpectedly accepted")
        for key in ("ttl", "settings"):
            try:
                sa.Column("invalid", cc.types.UInt32, **{"clickhouse_" + key: "value"})
            except ArgumentError as exc:
                assert "clickhouse_" + key in str(exc)
            else:
                raise AssertionError(key + " unexpectedly accepted")

        dialect = ClickHouseDialect()
        def ddl(table):
            return str(CreateTable(table).compile(dialect=dialect))
        for key, value in {
            "materialized": sa.text("id + 13"), "alias": sa.text("id + 79"),
            "codec": "ZSTD(3)", "after": "id",
        }.items():
            column = sa.Column("extra", cc.types.UInt32, **{"clickhouse_" + key: value})
            assert ClickHouseDDLHelper.get_option(column, key) is value
            table = sa.Table("explicit", sa.MetaData(), sa.Column("id", cc.types.UInt32), column, cc.engines.Log())
            assert dict(table.to_metadata(sa.MetaData()).c.extra.kwargs) == {"clickhouse_" + key: value}
            assert ddl(table.to_metadata(sa.MetaData())) == ddl(table)

        tables = [
            sa.Table("events", sa.MetaData(), sa.Column("id", cc.types.UInt32), cc.engines.Log()),
            cc.Dictionary(
                "lookup", sa.MetaData(), sa.Column("id", cc.types.UInt32),
                source="CLICKHOUSE(TABLE 'source')", layout="FLAT", lifetime="MIN 0 MAX 79", primary_key="id",
            ),
        ]
        from alembic.autogenerate import render
        from alembic.autogenerate.api import AutogenContext
        from alembic.operations import Operations, ops
        from alembic.runtime.migration import MigrationContext
        import clickhouse_connect.cc_sqlalchemy.alembic
        autogen = AutogenContext(
            MigrationContext.configure(dialect=dialect),
            opts={"sqlalchemy_module_prefix": "sa.", "alembic_module_prefix": "op.", "user_module_prefix": None},
        )
        for table in tables:
            assert all(key.startswith("clickhousedb_") for key in dict(table.kwargs))
            sql = ddl(table)
            assert "CODEC" not in sql
            assert ddl(table.to_metadata(sa.MetaData())) == sql
            create_op = ops.CreateTableOp.from_table(table)
            assert ddl(create_op.to_table()) == sql
            assert ddl(create_op.reverse().to_table()) == sql
            for migration_op in (create_op, create_op.reverse()):
                rendered = render.render_op_text(autogen, migration_op)
                assert "clickhouse_" not in rendered, rendered
                buffer = StringIO()
                offline = MigrationContext.configure(dialect=dialect, opts={"as_sql": True, "output_buffer": buffer})
                exec(rendered, {"sa": sa, "op": Operations(offline), "Log": cc.engines.Log, "UInt32": cc.types.UInt32})
                assert buffer.getvalue().strip() == (
                    sql + ";" if migration_op is create_op else "DROP " + ("DICTIONARY" if table is tables[1] else "TABLE") + " `" + table.name + "`;"
                )
        """,
        foreign_dialect_path,
        provider,
        cache,
    )


@pytest.mark.parametrize("kind", ["table", "dictionary"])
def test_reflection_preserves_public_keys_and_copies_canonical_metadata(monkeypatch, kind):
    rows = [
        SimpleNamespace(
            name="value",
            type="UInt32",
            comment="",
            codec_expression="ZSTD(3)",
            ttl_expression="id + INTERVAL 1 DAY",
            default_type="MATERIALIZED",
            default_expression="id + 13",
        )
    ]
    connection = SimpleNamespace(execute=lambda *_args, **_kwargs: rows)
    table_metadata = SimpleNamespace(engine="Dictionary" if kind == "dictionary" else "Memory", engine_full="Memory", comment="")
    monkeypatch.setattr(inspector_module, "get_table_metadata", lambda *_args: table_metadata)
    monkeypatch.setattr(
        inspector_module,
        "get_dictionary_create_sql",
        lambda *_args: (
            "CREATE DICTIONARY default.lookup (\n`value` UInt32 MATERIALIZED id + 13\n)\n"
            "PRIMARY KEY value\nSOURCE(CLICKHOUSE(TABLE 'source'))\nLAYOUT(FLAT())\nLIFETIME(MIN 0 MAX 79)"
        ),
    )
    public_columns = inspector_module.get_columns(connection, "lookup", "default")
    assert "clickhouse_materialized" in public_columns[0]
    assert not any(key.startswith("clickhousedb_") for key in public_columns[0])
    if kind == "table":
        assert public_columns[0]["clickhouse_codec"] == "ZSTD(3)"
        assert str(public_columns[0]["clickhouse_ttl"]) == "id + INTERVAL 1 DAY"
    else:
        public_metadata = inspector_module.get_dictionary_metadata(connection, "lookup", "default")
        assert public_metadata["clickhouse_table_type"] == "dictionary"
        assert public_metadata["clickhouse_dictionary_layout"] == "FLAT()"

    inspector = SimpleNamespace(
        bind=connection,
        get_columns=lambda *_args: inspector_module.get_columns(connection, "lookup", "default"),
    )
    target = SimpleNamespace(_inspection_context=lambda: nullcontext(inspector))
    table = sa.Table("lookup", sa.MetaData(), schema="default")
    inspector_module.ChInspector.reflect_table(target, table)
    for reflected in (table, table.to_metadata(sa.MetaData())):
        assert all(key.startswith("clickhousedb_") for key in dict(reflected.kwargs))
        assert "clickhousedb_materialized" in dict(reflected.c.value.kwargs)
        assert not any(key.startswith("clickhouse_") for key in dict(reflected.c.value.kwargs))
        sql = str(CreateTable(reflected).compile(dialect=ClickHouseDialect()))
        assert "MATERIALIZED id + 13" in sql
        if kind == "dictionary":
            assert "CREATE DICTIONARY" in sql
        else:
            assert sql.endswith(" Memory")
