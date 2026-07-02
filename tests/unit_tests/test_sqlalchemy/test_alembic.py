from io import StringIO
from types import SimpleNamespace

import pytest
from alembic.autogenerate import render
from alembic.autogenerate.api import AutogenContext, render_python_code
from alembic.ddl.impl import DefaultImpl
from alembic.operations import Operations, ops
from alembic.runtime.migration import MigrationContext
from alembic.util import CommandError
from sqlalchemy import Column, Index, Integer, MetaData, String, Table, literal_column, text
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.schema import CreateTable

from clickhouse_connect.cc_sqlalchemy import engines, types
from clickhouse_connect.cc_sqlalchemy.alembic import (
    AddClickHouseIndexesOp,
    AddClickHouseIndexOp,
    AddClickHouseProjectionOp,
    AddClickHouseProjectionsOp,
    ClickHouseImpl,
    ClickHouseIndex,
    ClickHouseProjection,
    CreateClickHouseDictionaryOp,
    CreateClickHouseMaterializedViewOp,
    DropClickHouseDictionaryOp,
    DropClickHouseIndexesOp,
    DropClickHouseIndexOp,
    DropClickHouseMaterializedViewOp,
    DropClickHouseProjectionOp,
    DropClickHouseProjectionsOp,
    MaterializeClickHouseIndexOp,
    MaterializeClickHouseProjectionOp,
    ModifyClickHouseTableSettingsOp,
    ReloadClickHouseDictionaryOp,
    ResetClickHouseTableSettingsOp,
    clickhouse_writer,
    include_object,
    patch_alembic_version,
)
from clickhouse_connect.cc_sqlalchemy.alembic.utils import make_include_object
from clickhouse_connect.cc_sqlalchemy.ddl.dictionary import Dictionary
from clickhouse_connect.cc_sqlalchemy.ddl.tableengine import (
    CollapsingMergeTree,
    MergeTree,
    ReplacingMergeTree,
    ReplicatedMergeTree,
    VersionedCollapsingMergeTree,
    build_engine,
)
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect
from clickhouse_connect.cc_sqlalchemy.sql import full_table


def test_ddl_compiler():
    dialect = ClickHouseDialect()
    metadata = MetaData()
    table = Table("my_table", metadata, Column("id", Integer), MergeTree(order_by=("id",)))

    # Create Table
    create_sql = str(CreateTable(table).compile(dialect=dialect))
    assert "CREATE TABLE `my_table` (`id` INTEGER) Engine MergeTree  ORDER BY (id)" in create_sql

    compiler = dialect.ddl_compiler(dialect, None)

    # Mocking Alembic Op objects
    class MockOp:
        def __init__(self, table, column):
            self.element = table
            self.column = column

    # Add Column
    add_col_op = MockOp(table, Column("name", String))
    add_sql = compiler.visit_add_column(add_col_op)
    assert add_sql == "ALTER TABLE `my_table` ADD COLUMN `name` VARCHAR"

    # Drop Column
    drop_col_op = MockOp(table, Column("old_col", String))
    drop_sql = compiler.visit_drop_column(drop_col_op)
    assert drop_sql == "ALTER TABLE `my_table` DROP COLUMN `old_col`"


def test_dictionary_ddl():
    dialect = ClickHouseDialect()
    metadata = MetaData()
    dictionary = Dictionary(
        "my_dict",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("val", String),
        source="CLICKHOUSE(QUERY 'SELECT id, val FROM source')",
        layout="FLAT()",
        lifetime="MIN 0 MAX 1000",
    )

    # Create Dictionary
    # We use CreateTable construct because Dictionary mimics Table for metadata
    create_sql = str(CreateTable(dictionary).compile(dialect=dialect))
    assert "CREATE DICTIONARY `my_dict`" in create_sql
    assert "`id` INTEGER" in create_sql
    assert "SOURCE(CLICKHOUSE(QUERY 'SELECT id, val FROM source'))" in create_sql
    assert "LAYOUT(FLAT())" in create_sql
    assert "LIFETIME(MIN 0 MAX 1000)" in create_sql


def test_engine_repr():
    # Test that engine repr returns Python code for constructor
    engine = MergeTree(order_by="id", partition_by="date")
    repr_str = repr(engine)
    # The repr should match the constructor args.
    # Note: kwargs order might vary, so check components
    assert "MergeTree(" in repr_str
    assert "order_by='id'" in repr_str
    assert "partition_by='date'" in repr_str

    replacing = ReplacingMergeTree(version="ts", is_deleted="deleted", order_by="id")
    replacing_repr = repr(replacing)
    assert "version='ts'" in replacing_repr
    assert "is_deleted='deleted'" in replacing_repr

    legacy_alias_repr = repr(ReplacingMergeTree(ver="ts", order_by="id"))
    assert "version='ts'" in legacy_alias_repr
    assert "ver=" not in legacy_alias_repr


def test_shared_engine_maps_to_base():
    """ClickHouse Cloud returns SharedMergeTree — build_engine should strip the Shared prefix."""
    engine = build_engine("SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') ORDER BY id")
    assert engine is not None
    assert engine.name == "MergeTree"
    assert repr(engine) == "MergeTree(order_by='id')"

    rmt = build_engine("SharedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', ver) ORDER BY id")
    assert rmt is not None
    assert rmt.name == "ReplacingMergeTree"
    assert "version='ver'" in repr(rmt)


def test_reflected_engine_repr_is_safe():
    engine = build_engine("MergeTree ORDER BY id")
    assert engine is not None
    assert repr(engine) == "MergeTree(order_by='id')"


def test_reflected_engine_repr_round_trips():
    """Reflected engines must produce repr() that evals back into a valid engine."""

    cases = [
        "MergeTree ORDER BY id",
        "MergeTree PARTITION BY toYYYYMM(ts) ORDER BY id SETTINGS index_granularity = 1024",
        "ReplacingMergeTree(ver, deleted) ORDER BY id",
        "ReplicatedMergeTree('/zk/path', 'r1') ORDER BY id",
        "CollapsingMergeTree(sign) ORDER BY id",
        "VersionedCollapsingMergeTree(sign, ver) ORDER BY id",
    ]
    for full_engine in cases:
        engine = build_engine(full_engine)
        assert engine is not None, f"build_engine returned None for {full_engine}"
        r = repr(engine)
        reconstructed = eval(
            r,
            {
                "MergeTree": MergeTree,
                "ReplacingMergeTree": ReplacingMergeTree,
                "ReplicatedMergeTree": ReplicatedMergeTree,
                "CollapsingMergeTree": CollapsingMergeTree,
                "VersionedCollapsingMergeTree": VersionedCollapsingMergeTree,
            },
        )
        assert reconstructed.name == engine.name, f"name mismatch for {full_engine}: {r}"


def test_reflected_replacing_merge_tree_preserves_positional_args():
    engine = build_engine("ReplacingMergeTree(ts, deleted) ORDER BY id")
    assert engine is not None
    assert "version='ts'" in repr(engine)
    assert "is_deleted='deleted'" in repr(engine)
    assert "order_by='id'" in repr(engine)


def test_reflected_replicated_merge_tree_preserves_positional_args():
    engine = build_engine("ReplicatedMergeTree('/clickhouse/tables/{shard}', '{replica}') ORDER BY id")
    assert engine is not None
    assert "zk_path='/clickhouse/tables/{shard}'" in repr(engine)
    assert "replica='{replica}'" in repr(engine)


def test_reflected_replicated_merge_tree_handles_commas_in_quoted_args():
    engine = build_engine("ReplicatedMergeTree('/clickhouse/tables/shard,blue', '{replica}') ORDER BY id")
    assert engine is not None
    assert "zk_path='/clickhouse/tables/shard,blue'" in repr(engine)
    assert "replica='{replica}'" in repr(engine)


def test_engine_repr_supports_text_expressions_and_ttl():
    engine = ReplacingMergeTree(
        version="clickhouse_created_at",
        partition_by=(text("toStartOfDay(timestamp)"), "resource_type"),
        order_by=(text("toStartOfDay(timestamp)"), "resource_type", "workload_identity", "id"),
        ttl=text("toDateTime(timestamp) + INTERVAL 30 DAY"),
    )

    compiled = engine.compile()
    assert "ORDER BY (toStartOfDay(timestamp),resource_type,workload_identity,id)" in compiled
    assert "PARTITION BY (toStartOfDay(timestamp),resource_type)" in compiled
    assert "TTL toDateTime(timestamp) + INTERVAL 30 DAY" in compiled

    repr_str = repr(engine)
    assert "sa.text('toStartOfDay(timestamp)')" in repr_str
    assert "ttl=sa.text('toDateTime(timestamp) + INTERVAL 30 DAY')" in repr_str


def test_reflected_engine_repr_keeps_ttl_separate_from_order_by():
    engine = build_engine(
        "ReplacingMergeTree(version, deleted) "
        "PARTITION BY (toStartOfDay(timestamp), resource_type) "
        "ORDER BY (toStartOfDay(timestamp), resource_type, workload_identity, id) "
        "TTL toDateTime(timestamp) + INTERVAL 30 DAY"
    )

    assert engine is not None
    repr_str = repr(engine)
    assert "ttl='toDateTime(timestamp) + INTERVAL 30 DAY'" in repr_str
    assert "order_by='(toStartOfDay(timestamp), resource_type, workload_identity, id)'" in repr_str


def test_boolean_type_supports_create_table_reverse():
    table = Table("flag_events", MetaData(), Column("flag", types.Boolean(), nullable=False), MergeTree(order_by="flag"))
    op = ops.CreateTableOp.from_table(table)
    reversed_op = op.reverse()
    assert reversed_op.table_name == "flag_events"


def test_clickhouse_impl_registration():
    assert DefaultImpl.get_by_dialect(ClickHouseDialect()) is ClickHouseImpl
    context = MigrationContext.configure(dialect_name="clickhousedb")
    assert isinstance(context.impl, ClickHouseImpl)


def test_render_type_uses_clickhouse_names():
    context = MigrationContext.configure(dialect=ClickHouseDialect(), opts={"target_metadata": MetaData()})
    assert context.impl.render_type(types.Int32(), None) == "Int32"
    assert context.impl.render_type(types.DateTime64(3, "UTC"), None) == "DateTime64(3, 'UTC')"


def test_render_type_enum_emits_valid_python():
    context = MigrationContext.configure(dialect=ClickHouseDialect(), opts={"target_metadata": MetaData()})

    enum8 = types.Enum8(keys=["new", "accepted"], values=[0, 1])
    rendered_enum8 = context.impl.render_type(enum8, None)
    assert rendered_enum8 == "Enum8(keys=['new', 'accepted'], values=[0, 1])"

    enum16 = types.Enum16(keys=["pending", "complete"], values=[300, 600])
    rendered_enum16 = context.impl.render_type(enum16, None)
    assert rendered_enum16 == "Enum16(keys=['pending', 'complete'], values=[300, 600])"

    nullable_enum = types.Nullable(types.Enum8(keys=["new", "accepted"], values=[0, 1]))
    rendered_nullable = context.impl.render_type(nullable_enum, None)
    assert rendered_nullable == "Nullable(Enum8(keys=['new', 'accepted'], values=[0, 1]))"

    namespace = {}
    exec("from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import *", namespace)
    rebuilt = eval(rendered_enum8, namespace)
    assert rebuilt.name == enum8.name
    rebuilt_nullable = eval(rendered_nullable, namespace)
    assert rebuilt_nullable.name == nullable_enum.name


def test_render_type_enum_with_special_chars_in_keys():
    context = MigrationContext.configure(dialect=ClickHouseDialect(), opts={"target_metadata": MetaData()})

    enum_special = types.Enum8(keys=["it's", "a-b", "with space"], values=[0, 1, 2])
    rendered = context.impl.render_type(enum_special, None)

    namespace = {}
    exec("from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import *", namespace)
    rebuilt = eval(rendered, namespace)
    assert rebuilt.name == enum_special.name


def test_render_type_containers_with_enum_emit_valid_python():
    context = MigrationContext.configure(dialect=ClickHouseDialect(), opts={"target_metadata": MetaData()})
    enum8 = types.Enum8(keys=["new", "accepted"], values=[0, 1])
    enum_kwarg = "Enum8(keys=['new', 'accepted'], values=[0, 1])"

    cases = {
        types.Array(enum8): f"Array({enum_kwarg})",
        types.Map(types.String, enum8): f"Map(String, {enum_kwarg})",
        types.Tuple(types.UInt32, enum8): f"Tuple(UInt32, {enum_kwarg})",
        types.Array(types.Nullable(enum8)): f"Array(Nullable({enum_kwarg}))",
        types.Array(types.LowCardinality(enum8)): f"Array(LowCardinality({enum_kwarg}))",
        types.Array(types.Array(enum8)): f"Array(Array({enum_kwarg}))",
        types.Map(enum8, types.Array(enum8)): f"Map({enum_kwarg}, Array({enum_kwarg}))",
    }

    namespace = {}
    exec("from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import *", namespace)
    for type_obj, expected in cases.items():
        rendered = context.impl.render_type(type_obj, None)
        assert rendered == expected, f"{type_obj.name}: {rendered!r} != {expected!r}"
        rebuilt = eval(rendered, namespace)
        assert rebuilt.name == type_obj.name


def test_render_type_non_enum_containers_use_clickhouse_names():
    context = MigrationContext.configure(dialect=ClickHouseDialect(), opts={"target_metadata": MetaData()})

    assert context.impl.render_type(types.Array(types.UInt32), None) == "Array(UInt32)"
    assert context.impl.render_type(types.Map(types.String, types.UInt32), None) == "Map(String, UInt32)"
    assert context.impl.render_type(types.Tuple(types.UInt32, types.String), None) == "Tuple(UInt32, String)"
    assert context.impl.render_type(types.Array(types.Nullable(types.String)), None) == "Array(Nullable(String))"
    assert context.impl.render_type(types.Array(types.DateTime64(3, "UTC")), None) == "Array(DateTime64(3, 'UTC'))"


def test_explicit_nullable_column_renders_nullable_type():
    dialect = ClickHouseDialect()
    metadata = MetaData()
    table = Table("nullable_events", metadata, Column("description", types.String(), nullable=True), MergeTree(order_by="description"))
    create_sql = str(CreateTable(table).compile(dialect=dialect))
    assert "`description` Nullable(String)" in create_sql


def test_unspecified_nullable_column_stays_non_nullable():
    dialect = ClickHouseDialect()
    metadata = MetaData()
    table = Table("default_nullable_events", metadata, Column("description", types.String()), MergeTree(order_by="description"))
    create_sql = str(CreateTable(table).compile(dialect=dialect))
    assert "`description` String" in create_sql
    assert "Nullable(String)" not in create_sql


def test_explicit_nullable_low_cardinality_renders_correctly():
    dialect = ClickHouseDialect()
    metadata = MetaData()
    table = Table(
        "nullable_lc_events",
        metadata,
        Column("group_type", types.LowCardinality(types.String()), nullable=True),
        MergeTree(order_by="group_type"),
    )
    create_sql = str(CreateTable(table).compile(dialect=dialect))
    assert "`group_type` LowCardinality(Nullable(String))" in create_sql


def test_compare_type_uses_column_nullable_flag():
    context = MigrationContext.configure(dialect=ClickHouseDialect(), opts={"target_metadata": MetaData()})
    inspector_column = Column("description", types.Nullable(types.String()))
    metadata_column = Column("description", types.String(), nullable=True)
    assert context.impl.compare_type(inspector_column, metadata_column) is False


def test_compare_type_ignores_unspecified_nullable_flag():
    context = MigrationContext.configure(dialect=ClickHouseDialect(), opts={"target_metadata": MetaData()})
    inspector_column = Column("description", types.Nullable(types.String()))
    metadata_column = Column("description", types.String())
    assert context.impl.compare_type(inspector_column, metadata_column) is False


def test_utils_factories():
    # Test make_include_object
    filter_fn = make_include_object(exclude_tables=frozenset(["excluded"]), include_schemas=frozenset(["public"]))

    t1 = Table("ok", MetaData(), schema="public")
    assert filter_fn(t1, "ok", "table", False, None) is True

    t2 = Table("excluded", MetaData(), schema="public")
    assert filter_fn(t2, "excluded", "table", False, None) is False

    t3 = Table("ok", MetaData(), schema="other")
    assert filter_fn(t3, "ok", "table", False, None) is False

    # Test base include_object integration (system tables)
    t_sys = Table("query_log", MetaData(), schema="system")
    assert filter_fn(t_sys, "query_log", "table", False, None) is False


def test_alembic_impl_version_table_sql():
    buffer = StringIO()
    impl = ClickHouseImpl(ClickHouseDialect(), None, True, False, buffer, {})
    version_table = impl.version_table_impl(
        version_table="alembic_version",
        version_table_schema=None,
        version_table_pk=True,
    )

    impl.create_table(version_table)

    update_stmt = (
        version_table.update().values(version_num=literal_column("'head'")).where(version_table.c.version_num == literal_column("'base'"))
    )
    delete_stmt = version_table.delete().where(version_table.c.version_num == literal_column("'old'"))
    impl._exec(update_stmt)
    impl._exec(delete_stmt)

    sql = buffer.getvalue()
    assert "CREATE TABLE `alembic_version`" in sql
    assert "Engine MergeTree" in sql
    assert "INSERT INTO `alembic_version` (version_num) VALUES ('head')" in sql
    assert "ALTER TABLE `alembic_version` DELETE WHERE `version_num` = 'base' SETTINGS mutations_sync = 2" in sql
    assert "ALTER TABLE `alembic_version` DELETE WHERE `version_num` = 'old' SETTINGS mutations_sync = 2" in sql


def test_version_table_schema_desync():
    """Version-table UPDATE/DELETE is intercepted as INSERT + ALTER...DELETE when the version Table has schema=None and context_opts.version_table_schema is set."""
    buffer = StringIO()
    # Simulate what happens when include_schemas=True auto-sets the schema
    opts = {"version_table": "alembic_version", "version_table_schema": "mydb"}
    impl = ClickHouseImpl(ClickHouseDialect(), None, True, False, buffer, opts)

    # Build version table WITHOUT schema (as Alembic does when version_table_schema
    # was None at the time MigrationContext captured it)
    version_table = impl.version_table_impl(
        version_table="alembic_version",
        version_table_schema=None,
        version_table_pk=True,
    )

    update_stmt = (
        version_table.update().values(version_num=literal_column("'rev2'")).where(version_table.c.version_num == literal_column("'rev1'"))
    )
    delete_stmt = version_table.delete().where(version_table.c.version_num == literal_column("'old'"))

    impl._exec(update_stmt)
    impl._exec(delete_stmt)

    sql = buffer.getvalue()
    # Must be intercepted as insert+delete, not fall through as raw UPDATE
    assert "INSERT INTO" in sql
    assert "ALTER TABLE" in sql
    assert "DELETE WHERE" in sql


def test_alembic_operations_add_column_passes_clickhouse_settings():
    """op.add_column forwards clickhouse_settings to the impl and emits ALTER TABLE ... ADD COLUMN ... SETTINGS ...."""
    buffer = StringIO()
    context = MigrationContext.configure(
        dialect=ClickHouseDialect(),
        opts={"as_sql": True, "output_buffer": buffer},
    )
    op = Operations(context)

    op.add_column(
        "events",
        Column(
            "payload",
            String,
            server_default=text("'{}'"),
            clickhouse_after="id",
        ),
        schema="olap",
        if_not_exists=True,
        clickhouse_settings={"alter_sync": 2},
    )

    sql = buffer.getvalue()
    assert "ALTER TABLE `olap`.`events` ADD COLUMN IF NOT EXISTS `payload` VARCHAR DEFAULT '{}' AFTER `id` SETTINGS alter_sync = 2;" in sql


def test_add_column_autogenerate_render_preserves_op_kw():
    """Programmatic rewrites of AddColumnOp must round-trip clickhouse_settings via op.kw."""
    metadata = MetaData()
    context = MigrationContext.configure(dialect=ClickHouseDialect(), opts={"target_metadata": metadata})
    autogen_context = AutogenContext(
        context,
        opts={"sqlalchemy_module_prefix": "sa.", "alembic_module_prefix": "op.", "user_module_prefix": None},
    )
    add_op = ops.AddColumnOp(
        "events",
        Column("payload", String, server_default=text("'{}'")),
        schema="olap",
        if_not_exists=True,
        clickhouse_settings={"alter_sync": 2},
    )

    rendered = render.render_op_text(autogen_context, add_op)
    assert "clickhouse_settings={'alter_sync': 2}" in rendered


def test_alembic_operations_table_comments():
    """Public table comment operations must use ClickHouse ALTER TABLE syntax."""
    buffer = StringIO()
    context = MigrationContext.configure(
        dialect=ClickHouseDialect(),
        opts={"as_sql": True, "output_buffer": buffer},
    )
    op = Operations(context)

    op.create_table_comment(
        "events",
        "Application events table",
        schema="olap",
        existing_comment="Old comment",
    )
    op.drop_table_comment(
        "events",
        schema="olap",
        existing_comment="Application events table",
    )

    sql = buffer.getvalue()
    assert "ALTER TABLE `olap`.`events` MODIFY COMMENT 'Application events table';" in sql
    assert "ALTER TABLE `olap`.`events` MODIFY COMMENT '';" in sql
    assert "COMMENT ON TABLE" not in sql


def test_get_table_comment_missing_table_raises_no_such_table():
    class EmptyConnection:
        def execute(self, statement, parameters):
            return iter(())

    with pytest.raises(NoSuchTableError):
        ClickHouseDialect().get_table_comment(EmptyConnection(), "missing_events", schema="olap")


def test_alembic_impl_column_operations():
    buffer = StringIO()
    impl = ClickHouseImpl(ClickHouseDialect(), None, True, False, buffer, {})

    impl.add_column(
        "events",
        Column(
            "payload",
            String,
            server_default=text("'{}'"),
            clickhouse_after="id",
        ),
        schema="olap",
        if_not_exists=True,
        clickhouse_settings={"alter_sync": 2},
    )
    impl.alter_column(
        "events",
        "payload",
        schema="olap",
        existing_type=String(),
        server_default=text("'[]'"),
        if_exists=True,
        clickhouse_settings={"alter_sync": 2},
    )
    impl.drop_column(
        "events",
        Column("payload", String),
        schema="olap",
        if_exists=True,
    )

    sql = buffer.getvalue()
    assert "ALTER TABLE `olap`.`events` ADD COLUMN IF NOT EXISTS `payload` VARCHAR DEFAULT '{}' AFTER `id` SETTINGS alter_sync = 2;" in sql
    assert "ALTER TABLE `olap`.`events` MODIFY COLUMN IF EXISTS `payload` VARCHAR DEFAULT '[]' SETTINGS alter_sync = 2;" in sql
    assert "ALTER TABLE `olap`.`events` DROP COLUMN IF EXISTS `payload`;" in sql


def test_public_compat_exports():
    assert hasattr(engines, "MergeTree")
    assert hasattr(types, "String")


def test_positional_engine_autogenerate_render():
    metadata = MetaData()
    table = Table("events", metadata, Column("id", Integer), MergeTree(order_by="id"))
    context = MigrationContext.configure(dialect=ClickHouseDialect(), opts={"target_metadata": metadata})
    autogen_context = AutogenContext(
        context,
        opts={"sqlalchemy_module_prefix": "sa.", "alembic_module_prefix": "op.", "user_module_prefix": None},
    )

    rendered = render.render_op_text(autogen_context, ops.CreateTableOp.from_table(table))
    assert "clickhouse_engine=MergeTree(order_by='id')" in rendered
    assert "nullable=True" not in rendered


def test_dictionary_autogenerate_render():
    metadata = MetaData()
    dictionary = Dictionary(
        "dim_lookup",
        metadata,
        Column("id", Integer),
        source="CLICKHOUSE(TABLE 'system.one')",
        layout="FLAT",
        lifetime="MIN 0 MAX 10",
        primary_key="id",
    )
    context = MigrationContext.configure(dialect=ClickHouseDialect(), opts={"target_metadata": metadata})
    autogen_context = AutogenContext(
        context,
        opts={"sqlalchemy_module_prefix": "sa.", "alembic_module_prefix": "op.", "user_module_prefix": None},
    )

    rendered = render.render_op_text(autogen_context, ops.CreateTableOp.from_table(dictionary))
    assert "clickhouse_table_type='dictionary'" in rendered
    assert "clickhouse_dictionary_source=" in rendered
    assert "clickhouse_dictionary_layout='FLAT'" in rendered
    assert "clickhouse_dictionary_lifetime='MIN 0 MAX 10'" in rendered
    assert "clickhouse_dictionary_primary_key='id'" in rendered
    assert "nullable=True" not in rendered


def test_explicit_nullable_column_still_renders_nullable_argument():
    metadata = MetaData()
    table = Table("events", metadata, Column("description", types.String(), nullable=True), MergeTree(order_by="description"))
    context = MigrationContext.configure(dialect=ClickHouseDialect(), opts={"target_metadata": metadata})
    autogen_context = AutogenContext(
        context,
        opts={"sqlalchemy_module_prefix": "sa.", "alembic_module_prefix": "op.", "user_module_prefix": None},
    )

    rendered = render.render_op_text(autogen_context, ops.CreateTableOp.from_table(table))
    assert "nullable=True" in rendered


def test_dictionary_drop_autogenerate_render():
    metadata = MetaData()
    dictionary = Dictionary(
        "dim_lookup",
        metadata,
        Column("id", Integer),
        source="CLICKHOUSE(TABLE 'system.one')",
        layout="FLAT",
        lifetime="MIN 0 MAX 10",
        primary_key="id",
    )
    context = MigrationContext.configure(dialect=ClickHouseDialect(), opts={"target_metadata": metadata})
    autogen_context = AutogenContext(
        context,
        opts={"sqlalchemy_module_prefix": "sa.", "alembic_module_prefix": "op.", "user_module_prefix": None},
    )

    rendered = render.render_op_text(autogen_context, ops.CreateTableOp.from_table(dictionary).reverse())
    assert "op.drop_table('dim_lookup'" in rendered
    assert "clickhouse_table_type='dictionary'" in rendered
    assert "clickhouse_dictionary_source=" in rendered
    assert "clickhouse_dictionary_layout='FLAT'" in rendered
    assert "clickhouse_dictionary_lifetime='MIN 0 MAX 10'" in rendered
    assert "clickhouse_dictionary_primary_key='id'" in rendered


def test_include_object():
    t = Table("some_table", MetaData(), schema="default")
    assert include_object(t, "some_table", "table", False, None) is True

    t_version = Table("alembic_version", MetaData(), schema="default")
    assert include_object(t_version, "alembic_version", "table", False, None) is False

    t_sys = Table("query_log", MetaData(), schema="system")
    assert include_object(t_sys, "query_log", "table", False, None) is False

    t_inner = Table(".inner.some_mv", MetaData(), schema="default")
    assert include_object(t_inner, ".inner.some_mv", "table", False, None) is False

    indexed = Table("indexed_table", MetaData(), Column("id", Integer), schema="default")
    index = Index("idx_some_table", indexed.c.id)
    assert include_object(index, "idx_some_table", "index", True, None) is False
    with pytest.raises(CommandError, match="op.add_clickhouse_index"):
        include_object(index, "idx_some_table", "index", False, None)


def test_full_table_honors_schema_for_dotted_table_names():
    assert full_table("events.latest", None) == "`events.latest`"
    assert full_table("events.latest", "olap") == "`olap`.`events.latest`"

    op, buffer = _offline_op()
    op.rename_table("events.old", "events.new", schema="olap")
    assert "RENAME TABLE `olap`.`events.old` TO `olap`.`events.new`;" in buffer.getvalue()


def test_patch_alembic_version_is_noop():
    context = object()
    assert patch_alembic_version(context) is context


def test_clickhouse_writer_adds_template_imports():
    class _Ops:
        @staticmethod
        def is_empty() -> bool:
            return False

    class _Directive:
        def __init__(self, upgrade_ops=None, downgrade_ops=None):
            self.upgrade_ops = upgrade_ops
            self.downgrade_ops = downgrade_ops
            self.imports: set[str] = set()

    directive = _Directive(upgrade_ops=_Ops())
    clickhouse_writer(None, None, [directive])

    assert "from clickhouse_connect import cc_sqlalchemy" in directive.imports
    assert any("ddl.tableengine import *" in value for value in directive.imports)
    assert any("datatypes.sqltypes import *" in value for value in directive.imports)

    downgrade_only = _Directive(downgrade_ops=_Ops())
    clickhouse_writer(None, None, [downgrade_only])
    assert any("ddl.tableengine import *" in value for value in downgrade_only.imports)
    assert any("datatypes.sqltypes import *" in value for value in downgrade_only.imports)


def test_set_type_nullable_accepts_type_classes():
    assert str(ClickHouseImpl._set_type_nullable(types.UInt32, True).name) == "Nullable(UInt32)"


def test_clickhouse_settings_string_value_quoted():
    from clickhouse_connect.cc_sqlalchemy.sql.ddlcompiler import ClickHouseDDLHelper

    rendered = ClickHouseDDLHelper.render_settings({"mutations_sync": "2", "alter_sync": 2})
    assert "mutations_sync = '2'" in rendered
    assert "alter_sync = 2" in rendered


def _non_clickhouse_autogen_context():
    """AutogenContext for a non-ClickHouse dialect, with the cc_sqlalchemy.alembic import side-effect active."""
    context = MigrationContext.configure(dialect_name="sqlite", opts={"target_metadata": MetaData()})
    return AutogenContext(
        context,
        opts={"sqlalchemy_module_prefix": "sa.", "alembic_module_prefix": "op.", "user_module_prefix": None},
    )


def test_non_clickhouse_add_column_render_keeps_nullable():
    """Importing the ClickHouse Alembic adapter must not corrupt autogenerate for other dialects (#832)."""
    autogen_context = _non_clickhouse_autogen_context()
    rendered = render.render_op_text(autogen_context, ops.AddColumnOp("widgets", Column("name", String(32))))
    assert rendered == "op.add_column('widgets', sa.Column('name', sa.String(length=32), nullable=True))"
    assert "cc_sqlalchemy" not in rendered


def test_non_clickhouse_create_table_render_keeps_nullable():
    autogen_context = _non_clickhouse_autogen_context()
    table = Table("widgets", MetaData(), Column("id", Integer, primary_key=True), Column("name", String(32)))
    rendered = render.render_op_text(autogen_context, ops.CreateTableOp.from_table(table))
    assert "sa.Column('id', sa.Integer(), nullable=False)" in rendered
    assert "sa.Column('name', sa.String(length=32), nullable=True)" in rendered
    assert "clickhouse_engine" not in rendered


def test_non_clickhouse_drop_table_render_is_unmodified():
    autogen_context = _non_clickhouse_autogen_context()
    table = Table("widgets", MetaData(), Column("id", Integer, primary_key=True))
    rendered = render.render_op_text(autogen_context, ops.CreateTableOp.from_table(table).reverse())
    assert rendered == "op.drop_table('widgets')"


def test_captured_default_renderers_are_alembic_builtins():
    from clickhouse_connect.cc_sqlalchemy.alembic import adapter

    for op_type, ours in (
        (ops.CreateTableOp, adapter.render_create_table),
        (ops.AddColumnOp, adapter.render_add_column),
        (ops.DropTableOp, adapter.render_drop_table),
    ):
        default = adapter._DEFAULT_RENDERERS[op_type]
        assert default is not ours
        assert default.__module__.startswith("alembic.")


def _offline_op():
    buffer = StringIO()
    context = MigrationContext.configure(
        dialect=ClickHouseDialect(),
        opts={"as_sql": True, "output_buffer": buffer},
    )
    return Operations(context), buffer


def _statements(buffer: StringIO) -> list[str]:
    # Offline statements are terminated with ';' and separated by a blank line.
    return [s for s in buffer.getvalue().split("\n\n") if s.strip()]


@pytest.mark.parametrize(
    "call, expected",
    [
        pytest.param(
            lambda op: op.add_clickhouse_index(
                "events",
                "idx_1",
                "user_id",
                "bloom_filter(0.01)",
                granularity=13,
                if_not_exists=True,
                after_index="idx_prev",
                schema="olap",
                clickhouse_settings={"alter_sync": 2},
            ),
            "ALTER TABLE `olap`.`events` ADD INDEX IF NOT EXISTS `idx_1` user_id TYPE bloom_filter(0.01) "
            "GRANULARITY 13 AFTER `idx_prev` SETTINGS alter_sync = 2;",
            id="add_index_full",
        ),
        pytest.param(
            lambda op: op.add_clickhouse_index("events", "idx_1", "user_id", "minmax", first=True),
            "ALTER TABLE `events` ADD INDEX `idx_1` user_id TYPE minmax FIRST;",
            id="add_index_first",
        ),
        pytest.param(
            lambda op: op.add_clickhouse_index("events", "idx_1", "lower(name)", "minmax"),
            "ALTER TABLE `events` ADD INDEX `idx_1` lower(name) TYPE minmax;",
            id="add_index_minimal",
        ),
        pytest.param(
            lambda op: op.drop_clickhouse_index("events", "idx_1", if_exists=True, schema="olap"),
            "ALTER TABLE `olap`.`events` DROP INDEX IF EXISTS `idx_1`;",
            id="drop_index",
        ),
        pytest.param(
            lambda op: op.drop_clickhouse_index("events", "idx_1", clickhouse_settings={"alter_sync": 1}),
            "ALTER TABLE `events` DROP INDEX `idx_1` SETTINGS alter_sync = 1;",
            id="drop_index_settings",
        ),
        pytest.param(
            lambda op: op.materialize_clickhouse_index(
                "events",
                "idx_1",
                if_exists=True,
                partition="202401",
                clickhouse_settings={"mutations_sync": 1},
            ),
            "ALTER TABLE `events` MATERIALIZE INDEX IF EXISTS `idx_1` IN PARTITION 202401 SETTINGS mutations_sync = 1;",
            id="materialize_index",
        ),
        pytest.param(
            lambda op: op.materialize_clickhouse_index("events", "idx_1", schema="olap"),
            "ALTER TABLE `olap`.`events` MATERIALIZE INDEX `idx_1`;",
            id="materialize_index_no_partition",
        ),
        pytest.param(
            lambda op: op.add_clickhouse_projection(
                "events",
                "proj_1",
                "SELECT user_1, count() GROUP BY user_1",
                if_not_exists=True,
                after_projection="proj_prev",
                schema="olap",
            ),
            "ALTER TABLE `olap`.`events` ADD PROJECTION IF NOT EXISTS `proj_1` (SELECT user_1, count() GROUP BY user_1) AFTER `proj_prev`;",
            id="add_projection",
        ),
        pytest.param(
            lambda op: op.drop_clickhouse_projection("events", "proj_1", if_exists=True),
            "ALTER TABLE `events` DROP PROJECTION IF EXISTS `proj_1`;",
            id="drop_projection",
        ),
        pytest.param(
            lambda op: op.materialize_clickhouse_projection("events", "proj_1", if_exists=True, partition="202401"),
            "ALTER TABLE `events` MATERIALIZE PROJECTION IF EXISTS `proj_1` IN PARTITION 202401;",
            id="materialize_projection",
        ),
        pytest.param(
            lambda op: op.modify_clickhouse_table_settings(
                "events", {"merge_with_ttl_timeout": 79}, schema="olap", clickhouse_settings={"alter_sync": 2}
            ),
            "ALTER TABLE `olap`.`events` MODIFY SETTING merge_with_ttl_timeout = 79 SETTINGS alter_sync = 2;",
            id="modify_settings",
        ),
        pytest.param(
            lambda op: op.reset_clickhouse_table_settings("events", ["merge_with_ttl_timeout", "ttl_only_drop_parts"], schema="olap"),
            "ALTER TABLE `olap`.`events` RESET SETTING merge_with_ttl_timeout, ttl_only_drop_parts;",
            id="reset_settings",
        ),
        pytest.param(
            lambda op: op.reload_clickhouse_dictionary("dim_lookup", schema="olap"),
            "SYSTEM RELOAD DICTIONARY `olap`.`dim_lookup`;",
            id="reload_dictionary",
        ),
        pytest.param(
            lambda op: op.create_clickhouse_materialized_view(
                "events_mv",
                "events_sink",
                "SELECT id, name FROM events",
                if_not_exists=True,
                schema="olap",
                to_schema="olap",
            ),
            "CREATE MATERIALIZED VIEW IF NOT EXISTS `olap`.`events_mv` TO `olap`.`events_sink` AS SELECT id, name FROM events;",
            id="create_materialized_view",
        ),
        pytest.param(
            lambda op: op.drop_clickhouse_materialized_view("events_mv", if_exists=True, schema="olap"),
            "DROP VIEW IF EXISTS `olap`.`events_mv`;",
            id="drop_materialized_view",
        ),
        pytest.param(
            lambda op: op.create_clickhouse_dictionary(
                "dim_lookup",
                [Column("id", types.UInt32()), Column("value", types.String())],
                primary_key="id",
                source="CLICKHOUSE(TABLE 'dim_source')",
                layout="FLAT",
                lifetime="MIN 0 MAX 10",
                if_not_exists=True,
                schema="olap",
                comment="Lookup values",
            ),
            "CREATE DICTIONARY IF NOT EXISTS `olap`.`dim_lookup` (`id` UInt32, `value` String) "
            "PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'dim_source')) LAYOUT(FLAT()) "
            "LIFETIME(MIN 0 MAX 10) COMMENT 'Lookup values';",
            id="create_dictionary",
        ),
        pytest.param(
            lambda op: op.drop_clickhouse_dictionary("dim_lookup", if_exists=True, schema="olap"),
            "DROP DICTIONARY IF EXISTS `olap`.`dim_lookup`;",
            id="drop_dictionary",
        ),
    ],
)
def test_clickhouse_ddl_ops_render_offline(call, expected):
    op, buffer = _offline_op()
    call(op)
    statements = _statements(buffer)
    assert len(statements) == 1
    assert statements[0] == expected


@pytest.mark.parametrize(
    "call",
    [
        lambda op: op.add_clickhouse_index("events", "idx_token", ":tenant", "minmax"),
        lambda op: op.add_clickhouse_projection("events", "proj_token", "SELECT :tenant AS tenant_id"),
        lambda op: op.materialize_clickhouse_index("events", "idx_token", partition=":tenant"),
        lambda op: op.create_clickhouse_materialized_view("events_mv", "events_sink", "SELECT ':tenant' AS tenant_id"),
        lambda op: op.create_clickhouse_dictionary(
            "dim_lookup",
            [Column("id", types.UInt32())],
            primary_key="id",
            source="CLICKHOUSE(QUERY 'SELECT :tenant AS id')",
            layout="FLAT",
            lifetime="MIN 0 MAX 10",
        ),
    ],
)
def test_raw_sql_passthrough_preserves_colon_tokens(call):
    op, buffer = _offline_op()
    call(op)
    sql = buffer.getvalue()
    assert ":tenant" in sql
    assert "NULL" not in sql


def test_dictionary_comment_escapes_backslashes_and_quotes():
    op, buffer = _offline_op()
    op.create_clickhouse_dictionary(
        "dim_lookup",
        [Column("id", types.UInt32())],
        primary_key="id",
        source="CLICKHOUSE(TABLE 'dim_source')",
        layout="FLAT",
        lifetime="MIN 0 MAX 10",
        comment="path\\:name it's",
    )
    assert "COMMENT 'path\\\\:name it\\'s';" in buffer.getvalue()


def test_dictionary_settings_without_comment_renders_valid_comment_slot():
    op, buffer = _offline_op()
    op.create_clickhouse_dictionary(
        "dim_lookup",
        [Column("id", types.UInt32())],
        primary_key="id",
        source="CLICKHOUSE(TABLE 'dim_source')",
        layout="FLAT",
        lifetime="MIN 0 MAX 10",
        clickhouse_settings={"log_queries": 0},
    )
    assert (
        "CREATE DICTIONARY `dim_lookup` (`id` UInt32) PRIMARY KEY id "
        "SOURCE(CLICKHOUSE(TABLE 'dim_source')) LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 10) "
        "COMMENT '' SETTINGS log_queries = 0;"
    ) in buffer.getvalue()


def test_add_clickhouse_indexes_single_statement():
    op, buffer = _offline_op()
    op.add_clickhouse_indexes(
        "events",
        [
            ClickHouseIndex("idx_1", "user_id", "minmax"),
            ClickHouseIndex("idx_2", "lower(name)", "bloom_filter(0.01)", granularity=13),
        ],
        schema="olap",
        clickhouse_settings={"alter_sync": 1},
    )
    statements = _statements(buffer)
    assert len(statements) == 1
    assert statements[0] == (
        "ALTER TABLE `olap`.`events` ADD INDEX `idx_1` user_id TYPE minmax, "
        "ADD INDEX `idx_2` lower(name) TYPE bloom_filter(0.01) GRANULARITY 13 SETTINGS alter_sync = 1;"
    )


def test_drop_clickhouse_indexes_single_statement():
    op, buffer = _offline_op()
    op.drop_clickhouse_indexes(
        "events",
        ["idx_1", "idx_2"],
        if_exists=True,
        schema="olap",
        clickhouse_settings={"alter_sync": 2},
    )
    statements = _statements(buffer)
    assert len(statements) == 1
    assert statements[0] == (
        "ALTER TABLE `olap`.`events` DROP INDEX IF EXISTS `idx_1`, DROP INDEX IF EXISTS `idx_2` SETTINGS alter_sync = 2;"
    )


def test_add_clickhouse_projections_single_statement():
    op, buffer = _offline_op()
    op.add_clickhouse_projections(
        "events",
        [
            ClickHouseProjection("proj_1", "SELECT user_1"),
            ClickHouseProjection("proj_2", "SELECT user_2", if_not_exists=True),
        ],
    )
    statements = _statements(buffer)
    assert len(statements) == 1
    assert statements[0] == (
        "ALTER TABLE `events` ADD PROJECTION `proj_1` (SELECT user_1), ADD PROJECTION IF NOT EXISTS `proj_2` (SELECT user_2);"
    )


def test_drop_clickhouse_projections_single_statement():
    op, buffer = _offline_op()
    op.drop_clickhouse_projections(
        "events",
        ["proj_1", "proj_2"],
        if_exists=True,
        clickhouse_settings={"alter_sync": 2},
    )
    statements = _statements(buffer)
    assert len(statements) == 1
    assert statements[0] == (
        "ALTER TABLE `events` DROP PROJECTION IF EXISTS `proj_1`, DROP PROJECTION IF EXISTS `proj_2` SETTINGS alter_sync = 2;"
    )


@pytest.mark.parametrize("schema", [None, "olap"])
def test_rename_table_renders_rename_table(schema):
    op, buffer = _offline_op()
    op.rename_table("old_events", "new_events", schema=schema)
    sql = buffer.getvalue()
    if schema:
        assert "RENAME TABLE `olap`.`old_events` TO `olap`.`new_events`;" in sql
    else:
        assert "RENAME TABLE `old_events` TO `new_events`;" in sql
    # Unpatched DefaultImpl emits the ClickHouse-rejected ALTER TABLE ... RENAME TO form.
    assert "ALTER TABLE" not in sql
    assert "RENAME TO" not in sql


def test_create_index_raises_pointing_at_helper():
    op, _ = _offline_op()
    with pytest.raises(CommandError, match="op.add_clickhouse_index"):
        op.create_index("idx_1", "events", ["user_id"])


def test_drop_index_raises_pointing_at_helper():
    op, _ = _offline_op()
    with pytest.raises(CommandError, match="op.drop_clickhouse_index"):
        op.drop_index("idx_1", table_name="events")


def test_standard_index_paths_raise_before_sql():
    buffer = StringIO()
    impl = ClickHouseImpl(ClickHouseDialect(), None, True, False, buffer, {})
    metadata = MetaData()

    with pytest.raises(CommandError, match="Column\\(index=True\\)"):
        impl.add_column("events", Column("name", String, index=True))
    assert buffer.getvalue() == ""

    indexed_table = Table("events", metadata, Column("id", Integer), Column("name", String, index=True), MergeTree(order_by="id"))
    with pytest.raises(CommandError, match="op.add_clickhouse_index"):
        impl.create_table(indexed_table)
    assert buffer.getvalue() == ""

    batch_table = Table("batch_events", MetaData(), Column("id", Integer), MergeTree(order_by="id"))
    batch_impl = SimpleNamespace(new_indexes={"idx_name": object()}, indexes={})
    with pytest.raises(CommandError, match="op.add_clickhouse_index"):
        impl.prep_table_for_batch(batch_impl, batch_table)
    assert buffer.getvalue() == ""


@pytest.mark.parametrize(
    "call",
    [
        lambda op: op.modify_clickhouse_table_settings("events", {}),
        lambda op: op.reset_clickhouse_table_settings("events", []),
        lambda op: op.add_clickhouse_indexes("events", []),
        lambda op: op.drop_clickhouse_indexes("events", []),
        lambda op: op.add_clickhouse_projections("events", []),
        lambda op: op.drop_clickhouse_projections("events", []),
        lambda op: op.create_clickhouse_dictionary(
            "dim_lookup",
            [],
            primary_key="id",
            source="CLICKHOUSE(TABLE 'dim_source')",
            layout="FLAT",
            lifetime="MIN 0 MAX 10",
        ),
    ],
)
def test_list_ops_require_non_empty(call):
    op, _ = _offline_op()
    with pytest.raises(ValueError):
        call(op)


def test_add_index_reverse_round_trips():
    settings = {"alter_sync": 2}
    add_op = AddClickHouseIndexOp("events", "idx_1", "user_id", "minmax", schema="olap", clickhouse_settings=settings)
    reversed_op = add_op.reverse()
    assert isinstance(reversed_op, DropClickHouseIndexOp)
    assert reversed_op.table_name == "events"
    assert reversed_op.name == "idx_1"
    assert reversed_op.if_exists is True
    assert reversed_op.schema == "olap"
    assert reversed_op.clickhouse_settings == settings


def test_add_indexes_reverse_round_trips():
    settings = {"alter_sync": 2}
    add_op = AddClickHouseIndexesOp(
        "events",
        [ClickHouseIndex("idx_1", "user_id", "minmax"), ClickHouseIndex("idx_2", "event_name", "set(100)")],
        schema="olap",
        clickhouse_settings=settings,
    )
    reversed_op = add_op.reverse()
    assert isinstance(reversed_op, DropClickHouseIndexesOp)
    assert reversed_op.table_name == "events"
    assert reversed_op.names == ("idx_1", "idx_2")
    assert reversed_op.if_exists is True
    assert reversed_op.schema == "olap"
    assert reversed_op.clickhouse_settings == settings


def test_add_projection_reverse_round_trips():
    settings = {"alter_sync": 2}
    add_op = AddClickHouseProjectionOp("events", "proj_1", "SELECT user_1", schema="olap", clickhouse_settings=settings)
    reversed_op = add_op.reverse()
    assert isinstance(reversed_op, DropClickHouseProjectionOp)
    assert reversed_op.table_name == "events"
    assert reversed_op.name == "proj_1"
    assert reversed_op.if_exists is True
    assert reversed_op.schema == "olap"
    assert reversed_op.clickhouse_settings == settings


def test_add_projections_reverse_round_trips():
    settings = {"alter_sync": 2}
    add_op = AddClickHouseProjectionsOp(
        "events",
        [ClickHouseProjection("proj_1", "SELECT user_1"), ClickHouseProjection("proj_2", "SELECT user_2")],
        schema="olap",
        clickhouse_settings=settings,
    )
    reversed_op = add_op.reverse()
    assert isinstance(reversed_op, DropClickHouseProjectionsOp)
    assert reversed_op.table_name == "events"
    assert reversed_op.names == ("proj_1", "proj_2")
    assert reversed_op.if_exists is True
    assert reversed_op.schema == "olap"
    assert reversed_op.clickhouse_settings == settings


def test_create_materialized_view_reverse_round_trips():
    create_op = CreateClickHouseMaterializedViewOp(
        "events_mv",
        "events_sink",
        "SELECT id FROM events",
        schema="olap",
    )
    reversed_op = create_op.reverse()
    assert isinstance(reversed_op, DropClickHouseMaterializedViewOp)
    assert reversed_op.name == "events_mv"
    assert reversed_op.if_exists is True
    assert reversed_op.schema == "olap"
    assert reversed_op.clickhouse_settings is None


def test_create_dictionary_reverse_round_trips():
    settings = {"distributed_ddl_task_timeout": 120}
    create_op = CreateClickHouseDictionaryOp(
        "dim_lookup",
        [Column("id", types.UInt32())],
        primary_key="id",
        source="CLICKHOUSE(TABLE 'dim_source')",
        layout="FLAT",
        lifetime="MIN 0 MAX 10",
        schema="olap",
        clickhouse_settings=settings,
    )
    reversed_op = create_op.reverse()
    assert isinstance(reversed_op, DropClickHouseDictionaryOp)
    assert reversed_op.name == "dim_lookup"
    assert reversed_op.if_exists is True
    assert reversed_op.schema == "olap"
    assert reversed_op.clickhouse_settings == settings


@pytest.mark.parametrize(
    "factory",
    [
        lambda: ClickHouseIndex("idx_1", "user_id", "minmax", first=True, after_index="idx_0"),
        lambda: ClickHouseProjection("proj_1", "SELECT user_1", first=True, after_projection="proj_0"),
        lambda: AddClickHouseIndexOp("events", "idx_1", "user_id", "minmax", first=True, after_index="idx_0"),
        lambda: AddClickHouseProjectionOp("events", "proj_1", "SELECT user_1", first=True, after_projection="proj_0"),
    ],
)
def test_first_and_after_are_mutually_exclusive(factory):
    with pytest.raises(ValueError):
        factory()


def test_clickhouse_ddl_ops_registered_on_operations():
    op, _ = _offline_op()
    for name in (
        "add_clickhouse_index",
        "add_clickhouse_indexes",
        "drop_clickhouse_index",
        "drop_clickhouse_indexes",
        "materialize_clickhouse_index",
        "add_clickhouse_projection",
        "add_clickhouse_projections",
        "drop_clickhouse_projection",
        "drop_clickhouse_projections",
        "materialize_clickhouse_projection",
        "modify_clickhouse_table_settings",
        "reset_clickhouse_table_settings",
        "create_clickhouse_materialized_view",
        "drop_clickhouse_materialized_view",
        "create_clickhouse_dictionary",
        "drop_clickhouse_dictionary",
        "reload_clickhouse_dictionary",
    ):
        assert callable(getattr(op, name))


def test_clickhouse_custom_ops_autogenerate_render():
    context = MigrationContext.configure(dialect=ClickHouseDialect(), opts={"target_metadata": MetaData()})
    generated = render_python_code(
        ops.UpgradeOps(
            ops=[
                AddClickHouseIndexOp("events", "idx_1", "user_id", "minmax", schema="olap"),
                AddClickHouseIndexesOp("events", [ClickHouseIndex("idx_1", "user_id", "minmax")]),
                DropClickHouseIndexOp("events", "idx_1", if_exists=True),
                DropClickHouseIndexesOp("events", ["idx_1", "idx_2"]),
                MaterializeClickHouseIndexOp("events", "idx_1", partition="202401"),
                AddClickHouseProjectionOp("events", "proj_1", "SELECT user_1"),
                AddClickHouseProjectionsOp("events", [ClickHouseProjection("proj_1", "SELECT user_1")]),
                DropClickHouseProjectionOp("events", "proj_1"),
                DropClickHouseProjectionsOp("events", ["proj_1", "proj_2"]),
                MaterializeClickHouseProjectionOp("events", "proj_1"),
                ModifyClickHouseTableSettingsOp("events", {"merge_with_ttl_timeout": 79}),
                ResetClickHouseTableSettingsOp("events", ["merge_with_ttl_timeout"]),
                CreateClickHouseMaterializedViewOp("events_mv", "events_sink", "SELECT id FROM events"),
                DropClickHouseMaterializedViewOp("events_mv"),
                CreateClickHouseDictionaryOp(
                    "dim_lookup",
                    [Column("id", types.UInt32())],
                    primary_key="id",
                    source="CLICKHOUSE(TABLE 'dim_source')",
                    layout="FLAT",
                    lifetime="MIN 0 MAX 10",
                ),
                DropClickHouseDictionaryOp("dim_lookup"),
                ReloadClickHouseDictionaryOp("dim_lookup"),
            ]
        ),
        migration_context=context,
    )

    for expected in (
        "op.add_clickhouse_index(",
        "op.add_clickhouse_indexes(",
        "op.drop_clickhouse_index(",
        "op.drop_clickhouse_indexes(",
        "op.materialize_clickhouse_index(",
        "op.add_clickhouse_projection(",
        "op.add_clickhouse_projections(",
        "op.drop_clickhouse_projection(",
        "op.drop_clickhouse_projections(",
        "op.materialize_clickhouse_projection(",
        "op.modify_clickhouse_table_settings(",
        "op.reset_clickhouse_table_settings(",
        "op.create_clickhouse_materialized_view(",
        "op.drop_clickhouse_materialized_view(",
        "op.create_clickhouse_dictionary(",
        "op.drop_clickhouse_dictionary(",
        "op.reload_clickhouse_dictionary(",
    ):
        assert expected in generated


def test_clickhouse_custom_op_renderers_add_value_object_imports():
    context = MigrationContext.configure(dialect=ClickHouseDialect(), opts={"target_metadata": MetaData()})
    autogen_context = AutogenContext(
        context,
        opts={"sqlalchemy_module_prefix": "sa.", "alembic_module_prefix": "op.", "user_module_prefix": None},
    )

    render.render_op_text(autogen_context, AddClickHouseIndexesOp("events", [ClickHouseIndex("idx_1", "user_id", "minmax")]))
    render.render_op_text(
        autogen_context,
        AddClickHouseProjectionsOp("events", [ClickHouseProjection("proj_1", "SELECT user_1")]),
    )

    assert "from clickhouse_connect.cc_sqlalchemy.alembic import ClickHouseIndex" in autogen_context.imports
    assert "from clickhouse_connect.cc_sqlalchemy.alembic import ClickHouseProjection" in autogen_context.imports


def test_render_settings_module_level_matches_helper():
    from clickhouse_connect.cc_sqlalchemy.sql.ddlcompiler import ClickHouseDDLHelper, render_settings

    settings = {"flag": True, "off": False, "count": 13, "ratio": 0.5, "label": "high"}
    rendered = render_settings(settings)
    assert rendered == "flag = 1, off = 0, count = 13, ratio = 0.5, label = 'high'"
    assert ClickHouseDDLHelper.render_settings(settings) == rendered
    assert render_settings(None) == ""
    assert render_settings({}) == ""
