from io import StringIO

from alembic.autogenerate import render
from alembic.autogenerate.api import AutogenContext
from alembic.ddl.impl import DefaultImpl
from alembic.operations import ops
from alembic.runtime.migration import MigrationContext
from sqlalchemy import Column, Integer, MetaData, String, Table, literal_column, text
from sqlalchemy.schema import CreateTable

from clickhouse_connect.cc_sqlalchemy import engines, types
from clickhouse_connect.cc_sqlalchemy.alembic import (
    ClickHouseImpl,
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

# pylint: disable=protected-access


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


# pylint: disable=eval-used
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
    """Regression: when include_schemas causes ClickHouseImpl to auto-detect
    version_table_schema, the _version Table built by Alembic has schema=None
    because the opts mutation happens after Alembic captures the value.

    The version-table UPDATE/DELETE interception must still work despite the
    schema mismatch between the Table object and the context_opts.
    """
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
