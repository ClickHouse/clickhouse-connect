from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.sql import ColumnElement
from typing_extensions import assert_type

import clickhouse_connect.cc_sqlalchemy as cc_sa
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import JSON, UInt32

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
