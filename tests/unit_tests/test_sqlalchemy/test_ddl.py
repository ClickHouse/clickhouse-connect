import ast

import pytest
import sqlalchemy as db
from sqlalchemy.exc import ArgumentError
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql.ddl import CreateTable

from clickhouse_connect import dbapi
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import Date, DateTime, DateTime64, String, UInt32, UInt64
from clickhouse_connect.cc_sqlalchemy.ddl.dictionary import Dictionary
from clickhouse_connect.cc_sqlalchemy.ddl.tableengine import (
    GraphiteMergeTree,
    MergeTree,
    ReplacingMergeTree,
    ReplicatedCollapsingMergeTree,
    ReplicatedGraphiteMergeTree,
    ReplicatedMergeTree,
    ReplicatedReplacingMergeTree,
    ReplicatedVersionedCollapsingMergeTree,
    build_engine,
)
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect
from clickhouse_connect.cc_sqlalchemy.sql.ddlcompiler import column_specification
from clickhouse_connect.driver.binding import format_str

dialect = ClickHouseDialect()


class DialectDateTime(db.TypeDecorator):
    impl = db.DateTime
    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == "clickhousedb":
            return dialect.type_descriptor(DateTime64(6, "UTC"))
        return self.impl


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


def test_engine_settings_string_value_quoted():
    engine = MergeTree(order_by="id", settings={"storage_policy": "hot_cold"})
    compiled = engine.compile()
    assert "SETTINGS storage_policy = 'hot_cold'" in compiled


def test_engine_settings_string_value_escapes_inner_quote():
    engine = MergeTree(order_by="id", settings={"comment_like": "it's fine"})
    compiled = engine.compile()
    assert r"comment_like = 'it\'s fine'" in compiled


def test_engine_settings_mixed_types():
    engine = MergeTree(
        order_by="id",
        settings={"index_granularity": 1024, "storage_policy": "default", "allow_nullable_key": True},
    )
    compiled = engine.compile()
    assert "index_granularity = 1024" in compiled
    assert "storage_policy = 'default'" in compiled
    assert "allow_nullable_key = 1" in compiled


def test_reflected_engine_preserves_settings():
    engine = build_engine("MergeTree ORDER BY id SETTINGS index_granularity = 1024, storage_policy = 'hot_cold'")
    assert engine is not None
    assert engine.settings == {"index_granularity": 1024, "storage_policy": "hot_cold"}


def test_reflected_settings_decode_clickhouse_backslash_escapes():
    engine = build_engine(r"MergeTree ORDER BY id SETTINGS comment_like = 'it\'s \"fine\"\nok'")
    assert engine is not None
    assert engine.settings == {"comment_like": 'it\'s "fine"\nok'}


def test_reflected_settings_float_value_preserved_as_float():
    engine = build_engine("MergeTree ORDER BY id SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9")
    assert engine is not None
    assert engine.settings == {"ratio_of_defaults_for_sparse_serialization": 0.9}
    # And re-rendering must keep it numeric, not quoted.
    assert "= 0.9" in engine.full_engine
    assert "= '0.9'" not in engine.full_engine


def test_engine_settings_string_value_roundtrip():
    original = MergeTree(order_by="id", settings={"comment_like": "it's fine"})
    # full_engine carries an "Engine " prefix; system.tables.engine_full does not.
    engine_full = original.full_engine.removeprefix("Engine ")
    reflected = build_engine(engine_full)
    assert reflected is not None
    assert reflected.settings == original.settings


def _engine_ddl(name, columns, engine):
    metadata = db.MetaData()
    table = db.Table(name, metadata, *columns, engine)
    return str(CreateTable(table).compile("", dialect=dialect))


@pytest.mark.parametrize(
    "type_factory",
    [
        DialectDateTime,
        lambda: db.DateTime().with_variant(DateTime64(6, "UTC"), "clickhousedb"),
    ],
    ids=["type-decorator", "variant"],
)
def test_column_type_uses_clickhouse_dialect(type_factory):
    ddl = _engine_ddl(
        "events",
        [db.Column("created_at", type_factory())],
        MergeTree(order_by="created_at"),
    )

    assert ddl == "CREATE TABLE `events` (`created_at` DateTime64(6, 'UTC')) Engine MergeTree ORDER BY created_at"


@pytest.mark.parametrize(
    "order_fn,expected_tail",
    [
        (lambda c: c.desc(), "Engine MergeTree ORDER BY `score` DESC"),
        (lambda c: c.asc(), "Engine MergeTree ORDER BY `score` ASC"),
    ],
)
def test_order_by_expression_scalar_direction(order_fn, expected_tail):
    score = db.Column("score", UInt32)
    ddl = _engine_ddl("books_test", [db.Column("book_id", UInt64), score], MergeTree(order_by=order_fn(score)))
    assert ddl == f"CREATE TABLE `books_test` (`book_id` UInt64, `score` UInt32) {expected_tail}"


def test_order_by_expression_list_direction():
    score = db.Column("score", UInt32)
    book_id = db.Column("book_id", UInt64)
    ddl = _engine_ddl("books_test", [book_id, score], MergeTree(order_by=[score.desc(), book_id.asc()]))
    assert ddl == ("CREATE TABLE `books_test` (`book_id` UInt64, `score` UInt32) Engine MergeTree  ORDER BY (`score` DESC,`book_id` ASC)")


def test_order_by_function_expression():
    book_id = db.Column("book_id", UInt64)
    author_id = db.Column("author_id", UInt64)
    ddl = _engine_ddl("books_test", [book_id, author_id], MergeTree(order_by=db.func.cityHash64(book_id, author_id)))
    assert ddl == (
        "CREATE TABLE `books_test` (`book_id` UInt64, `author_id` UInt64) Engine MergeTree ORDER BY cityHash64(`book_id`, `author_id`)"
    )


def test_order_by_tuple_expression():
    genre = db.Column("genre", String)
    score = db.Column("score", UInt32)
    ddl = _engine_ddl("books_test", [genre, score], MergeTree(order_by=db.tuple_(genre, score.desc())))
    assert ddl == ("CREATE TABLE `books_test` (`genre` String, `score` UInt32) Engine MergeTree ORDER BY (`genre`, `score` DESC)")


def test_order_by_mixed_list_full_issue_example():
    genre = db.Column("genre", String)
    score = db.Column("score", UInt32)
    review_count = db.Column("review_count", UInt32)
    book_id = db.Column("book_id", UInt64)
    author_id = db.Column("author_id", UInt64)
    ddl = _engine_ddl(
        "books_test",
        [genre, score, review_count, book_id, author_id],
        MergeTree(order_by=[genre, score.desc(), review_count.desc(), db.func.cityHash64(book_id, author_id)]),
    )
    assert ddl == (
        "CREATE TABLE `books_test` (`genre` String, `score` UInt32, `review_count` UInt32, "
        "`book_id` UInt64, `author_id` UInt64) Engine MergeTree  "
        "ORDER BY (`genre`,`score` DESC,`review_count` DESC,cityHash64(`book_id`, `author_id`))"
    )


def test_order_by_orm_mapped_attributes_issue_example():
    base = declarative_base()

    class BookAuthorScore(base):
        __tablename__ = "book_author_score"
        book_id = db.Column(UInt64, primary_key=True)
        author_id = db.Column(UInt64)
        genre = db.Column(String)
        score = db.Column(UInt32)
        review_count = db.Column(UInt32)

    BookAuthorScore.__table__.append_constraint(
        MergeTree(
            order_by=[
                BookAuthorScore.genre,
                BookAuthorScore.score.desc(),
                BookAuthorScore.review_count.desc(),
                db.func.cityHash64(BookAuthorScore.book_id, BookAuthorScore.author_id),
            ]
        )
    )
    ddl = str(CreateTable(BookAuthorScore.__table__).compile("", dialect=dialect))
    assert ddl.endswith("ORDER BY (`genre`,`score` DESC,`review_count` DESC,cityHash64(`book_id`, `author_id`))")


def test_partition_by_expression():
    ts = db.Column("ts", DateTime)
    ddl = _engine_ddl("books_test", [db.Column("book_id", UInt64), ts], MergeTree(order_by="book_id", partition_by=db.func.toYYYYMM(ts)))
    assert ddl.endswith("Engine MergeTree ORDER BY book_id PARTITION BY toYYYYMM(`ts`)")


def test_primary_key_expression():
    user_id = db.Column("user_id", UInt64)
    ddl = _engine_ddl("books_test", [user_id], MergeTree(order_by=user_id, primary_key=db.tuple_(user_id)))
    assert ddl.endswith("Engine MergeTree ORDER BY `user_id` PRIMARY KEY (`user_id`)")


def test_sample_by_expression():
    user_id = db.Column("user_id", UInt64)
    ddl = _engine_ddl("books_test", [user_id], MergeTree(order_by=user_id, sample_by=db.func.cityHash64(user_id)))
    assert ddl.endswith("Engine MergeTree ORDER BY `user_id` SAMPLE BY cityHash64(`user_id`)")


def test_ttl_binary_expression():
    created_at = db.Column("created_at", DateTime)
    ddl = _engine_ddl(
        "books_test",
        [db.Column("book_id", UInt64), created_at],
        MergeTree(order_by="book_id", ttl=created_at + db.text("INTERVAL 30 DAY")),
    )
    assert ddl.endswith("Engine MergeTree ORDER BY book_id TTL `created_at` + INTERVAL 30 DAY")


def test_order_by_plain_string_unchanged():
    ddl = _engine_ddl("books_test", [db.Column("book_id", UInt64)], MergeTree(order_by="book_id"))
    assert ddl == "CREATE TABLE `books_test` (`book_id` UInt64) Engine MergeTree ORDER BY book_id"


def test_order_by_bare_column_unchanged():
    book_id = db.Column("book_id", UInt64)
    ddl = _engine_ddl("books_test", [book_id], MergeTree(order_by=book_id))
    assert ddl == "CREATE TABLE `books_test` (`book_id` UInt64) Engine MergeTree ORDER BY `book_id`"


def test_repr_engine_value_expression_roundtrip():
    score = db.Column("score", UInt32)
    engine = MergeTree(order_by=score.desc())
    rendered = repr(engine)
    assert "sa.text(" in rendered
    assert "UnaryExpression" not in rendered
    ast.parse(rendered)

    original_ddl = _engine_ddl("books_test", [db.Column("book_id", UInt64), db.Column("score", UInt32)], engine)
    reconstructed = eval(rendered, {"sa": db, "MergeTree": MergeTree})  # noqa: S307
    roundtrip_ddl = _engine_ddl("books_test", [db.Column("book_id", UInt64), db.Column("score", UInt32)], reconstructed)
    assert roundtrip_ddl == original_ddl


def test_order_by_non_column_expression_rejected():
    throwaway = db.Table("throwaway", db.MetaData(), db.Column("c", UInt64))
    with pytest.raises(ArgumentError, match="column or scalar expression, got Table"):
        _engine_ddl("books_test", [db.Column("book_id", UInt64)], MergeTree(order_by=throwaway))


def test_engine_clause_string_literal_uses_clickhouse_escaping():
    quote_value = "O'Reilly"
    backslash_value = "a\\'; DROP"
    col = db.column("c")

    ddl_quote = _engine_ddl("books_test", [db.Column("c", String)], MergeTree(order_by=(col == quote_value)))
    assert ddl_quote == f"CREATE TABLE `books_test` (`c` String) Engine MergeTree ORDER BY `c` = {format_str(quote_value)}"
    assert "'O\\'Reilly'" in ddl_quote

    ddl_backslash = _engine_ddl("books_test", [db.Column("c", String)], MergeTree(order_by=(col == backslash_value)))
    assert ddl_backslash.endswith(f"ORDER BY `c` = {format_str(backslash_value)}")
    assert "'a\\\\\\'; DROP'" in ddl_backslash


def test_order_by_unbound_bindparam_rejected():
    with pytest.raises(ArgumentError, match="unbound parameter 'x'"):
        _engine_ddl("books_test", [db.Column("book_id", UInt64)], MergeTree(order_by=db.bindparam("x")))


def test_engine_clause_literal_value_allowed():
    ddl_literal = _engine_ddl("books_test", [db.Column("book_id", UInt64)], MergeTree(order_by=db.literal(79)))
    assert ddl_literal == "CREATE TABLE `books_test` (`book_id` UInt64) Engine MergeTree ORDER BY 79"

    created_at = db.column("created_at")
    ddl_add = _engine_ddl("books_test", [db.Column("created_at", UInt64)], MergeTree(order_by=created_at + 79))
    assert ddl_add == "CREATE TABLE `books_test` (`created_at` UInt64) Engine MergeTree ORDER BY `created_at` + 79"


def _column_spec(column):
    return column_specification(dialect, column)


@pytest.mark.parametrize(
    "build_column,expected",
    [
        (
            lambda: db.Column("derived", UInt64, clickhouse_materialized=db.text("id * 2"), comment="derived value"),
            "`derived` UInt64 MATERIALIZED id * 2 COMMENT 'derived value'",
        ),
        (
            lambda: db.Column("derived", UInt64, clickhouse_alias=db.text("id * 2"), comment="derived value"),
            "`derived` UInt64 ALIAS id * 2 COMMENT 'derived value'",
        ),
    ],
)
def test_materialized_alias_columns_keep_comment(build_column, expected):
    assert _column_spec(build_column()) == expected


@pytest.mark.parametrize(
    "build_column,expected",
    [
        (
            lambda: db.Column(
                "val",
                UInt64,
                server_default=db.text("5"),
                comment="a value",
                clickhouse_codec="ZSTD(1)",
                clickhouse_ttl=db.text("created_at + INTERVAL 1 DAY"),
            ),
            "`val` UInt64 DEFAULT 5 COMMENT 'a value' CODEC(ZSTD(1)) TTL created_at + INTERVAL 1 DAY",
        ),
        (
            lambda: db.Column(
                "val",
                UInt64,
                clickhouse_materialized=db.text("id * 2"),
                comment="a value",
                clickhouse_codec="ZSTD(1)",
                clickhouse_ttl=db.text("created_at + INTERVAL 1 DAY"),
            ),
            "`val` UInt64 MATERIALIZED id * 2 COMMENT 'a value' CODEC(ZSTD(1)) TTL created_at + INTERVAL 1 DAY",
        ),
    ],
)
def test_column_clause_order_comment_before_codec_before_ttl(build_column, expected):
    # ClickHouse's column grammar requires COMMENT, then CODEC, then TTL.
    assert _column_spec(build_column()) == expected


@pytest.mark.parametrize(
    "build_column,expected",
    [
        (
            lambda: db.Column(
                "c", UInt64, clickhouse_materialized=db.text("1"), clickhouse_alias=db.text("2"), server_default=db.text("3")
            ),
            "`c` UInt64 MATERIALIZED 1",
        ),
        (
            lambda: db.Column("c", UInt64, clickhouse_alias=db.text("2"), server_default=db.text("3")),
            "`c` UInt64 ALIAS 2",
        ),
    ],
)
def test_default_kinds_are_mutually_exclusive(build_column, expected):
    # DEFAULT, MATERIALIZED, and ALIAS are mutually exclusive; only the highest-precedence one is emitted.
    assert _column_spec(build_column()) == expected


def _generic_literal(value, server_side):
    rendered = value.replace("'", "''").replace("\\", "\\\\")
    if not server_side:
        rendered = rendered.replace("%", "%%")
    return f"'{rendered}'"


@pytest.mark.parametrize(
    ("option", "clause"),
    [
        ("server_default", "DEFAULT"),
        ("clickhouse_materialized", "MATERIALIZED"),
        ("clickhouse_alias", "ALIAS"),
        ("clickhouse_ttl", "TTL"),
    ],
)
@pytest.mark.parametrize("expression", [False, True], ids=["plain", "expression"])
@pytest.mark.parametrize("server_side", [False, True], ids=["client-side", "server-side"])
def test_column_string_clauses_use_clickhouse_escaping(option, clause, expression, server_side):
    value = "middle\\'value% adjacent%% trailing\\"
    default = db.literal(value) if expression else value
    column = db.Column("value", String, **{option: default})
    ch_dialect = ClickHouseDialect(dbapi=dbapi, server_side_params=server_side)

    assert column_specification(ch_dialect, column) == f"`value` String {clause} {_generic_literal(value, server_side)}"


def _comment_ddl(kind, comment, ch_dialect):
    if kind == "table":
        table = db.Table(
            "comment_test",
            db.MetaData(),
            db.Column("key", UInt64),
            MergeTree(order_by="key"),
            comment=comment,
        )
        return str(CreateTable(table).compile(dialect=ch_dialect))
    if kind == "dictionary":
        dictionary = Dictionary(
            "dict_comment_test",
            db.MetaData(),
            db.Column("id", UInt32),
            primary_key="id",
            source="CLICKHOUSE(TABLE 'src')",
            layout="FLAT",
            lifetime="MIN 0 MAX 10",
            comment=comment,
        )
        return str(CreateTable(dictionary).compile(dialect=ch_dialect))
    column = db.Column("key", UInt64, comment=comment)
    return column_specification(ch_dialect, column)


@pytest.mark.parametrize("kind", ["table", "dictionary", "column"])
@pytest.mark.parametrize("server_side", [False, True], ids=["client-side", "server-side"])
@pytest.mark.parametrize(
    "comment",
    ["middle\\path", "trailing\\", "backslash\\'quote% adjacent%%"],
    ids=["middle-backslash", "trailing-backslash", "backslash-quote-percent"],
)
def test_create_comments_use_statement_literal_compiler(kind, server_side, comment):
    ch_dialect = ClickHouseDialect(dbapi=dbapi, server_side_params=server_side)

    assert f"COMMENT {_generic_literal(comment, server_side)}" in _comment_ddl(kind, comment, ch_dialect)
