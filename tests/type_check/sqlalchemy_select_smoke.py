from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.orm import InstrumentedAttribute
from sqlalchemy.sql import ColumnElement
from typing_extensions import assert_type

import clickhouse_connect.cc_sqlalchemy as cc_sa
from clickhouse_connect.cc_sqlalchemy.datatypes.base import ChSqlaType
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import JSON, Array, DateTime64, LowCardinality, Nullable, String, UInt32
from clickhouse_connect.cc_sqlalchemy.ddl.tableengine import (
    Memory,
    MergeTree,
    ReplicatedMergeTree,
    ReplicatedSummingMergeTree,
    SummingMergeTree,
)

book = sa.table(
    "book",
    sa.column("id"),
    sa.column("author_id"),
    sa.column("title"),
)
author = sa.table("author", sa.column("id"))

base = cc_sa.select(book.c.id).select_from(book)
assert_type(base, cc_sa.ClickHouseSelect)

after_add_columns = base.add_columns(book.c.title)
assert_type(after_add_columns, cc_sa.ClickHouseSelect)
after_add_columns.ch_join(author, author.c.id == book.c.author_id, isouter=True, strictness="ANY")

after_with_only_columns = base.with_only_columns(book.c.title)
assert_type(after_with_only_columns, cc_sa.ClickHouseSelect)
after_with_only_columns.prewhere(book.c.id == 13)

after_reduce_columns = cc_sa.select(book.c.id, book.c.id).select_from(book).reduce_columns()
assert_type(after_reduce_columns, cc_sa.ClickHouseSelect)
after_reduce_columns.limit_by([book.c.id], 3)

after_column = base.column(book.c.title)
assert_type(after_column, cc_sa.ClickHouseSelect)
after_column.ch_join(author, author.c.id == book.c.author_id, strictness="ALL")

after_common_generatives = (
    base.where(book.c.id == 13).order_by(book.c.title).group_by(book.c.author_id).having(book.c.id > 13).limit(10).offset(1)
)
assert_type(after_common_generatives, cc_sa.ClickHouseSelect)
after_common_generatives.prewhere(book.c.title != "done")

materialized = base.cte("ranked", materialized=True)
assert_type(materialized, sa.CTE)
assert_type(cc_sa.cte(sa.select(book.c.id), "ranked", materialized=True), sa.CTE)

assert_type(Nullable(String), String)
assert_type(Nullable(String()), String)
assert_type(LowCardinality(String), String)
assert_type(LowCardinality(String()), String)
assert_type(Nullable(UInt32), UInt32)
assert_type(LowCardinality(UInt32()), UInt32)
assert_type(Nullable(DateTime64(6)), DateTime64)
assert_type(LowCardinality(Nullable(String)), String)
assert_type(Array(LowCardinality(Nullable(String))), Array)

sa.Column("hostname", LowCardinality(String))
sa.Column("description", Nullable(String()))
sa.Column("count", Nullable(UInt32))
sa.Column("category", LowCardinality(UInt32()))
sa.Column("optional_hostname", LowCardinality(Nullable(String)))

column_types: list[ChSqlaType] = [Nullable(String)]
column_types.append(UInt32())


def invalid_wrapper_inputs() -> None:
    Nullable(sa.String)  # type: ignore[type-var]
    LowCardinality(sa.String())  # type: ignore[arg-type]
    Nullable("String")  # type: ignore[arg-type]
    LowCardinality(None)  # type: ignore[arg-type]


json_payload = sa.column("payload", JSON())
configured_json = JSON(
    typed_paths={"request.id": UInt32, "attributes": "Variant(String, Array(String))"},
    max_dynamic_paths=256,
    max_dynamic_types=16,
    skip_paths=["internal.debug"],
    skip_regexps=[r"^private\."],
)
assert_type(configured_json, JSON)
assert_type(JSON(user_id=UInt32), JSON)
untyped_json_path = cc_sa.json_subcolumn(json_payload, "severity")
assert_type(untyped_json_path, ColumnElement[object])
typed_json_path = cc_sa.json_subcolumn(json_payload, "request_id", type_=UInt32())
assert_type(typed_json_path, ColumnElement[int])
typed_json_path_from_class = cc_sa.json_subcolumn(json_payload, "request_id", type_=UInt32)
assert_type(typed_json_path_from_class, ColumnElement[int])
cc_sa.json_subcolumn(json_payload, 13)  # type: ignore[call-overload]

assert_type(Memory(), Memory)
assert_type(Memory({"settings": {"max_rows_to_keep": 13}}), Memory)
assert_type(Memory(kwargs={}, settings={"max_rows_to_keep": 13}), Memory)


def summing_engine_columns(attribute: InstrumentedAttribute[int]) -> None:
    column = sa.Column("amount", UInt32)
    names: list[str] = ["amount", "count"]
    columns: list[sa.Column[int]] = [column]
    assert_type(SummingMergeTree("id", columns="amount"), SummingMergeTree)
    assert_type(SummingMergeTree("id", columns=names), SummingMergeTree)
    assert_type(SummingMergeTree("id", columns=columns), SummingMergeTree)
    assert_type(SummingMergeTree("id", columns=attribute), SummingMergeTree)
    assert_type(SummingMergeTree("id", columns=[column, attribute, "count"]), SummingMergeTree)
    merge_tree: MergeTree = SummingMergeTree("id", columns=(column, "count"))
    replicated: ReplicatedMergeTree = ReplicatedSummingMergeTree("id", columns=[column])
    assert_type(merge_tree, MergeTree)
    assert_type(replicated, ReplicatedMergeTree)
    SummingMergeTree("id", columns=13)  # type: ignore[arg-type]
    SummingMergeTree("id", columns=sa.text("amount"))  # type: ignore[arg-type]
