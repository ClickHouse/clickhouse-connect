# Migrating from `clickhouse-sqlalchemy` to `clickhouse-connect`'s `cc_sqlalchemy`

For projects using [`clickhouse-sqlalchemy`](https://github.com/xzkostyan/clickhouse-sqlalchemy) (TCP via `clickhouse-driver`) that want to move to `clickhouse-connect`'s `cc_sqlalchemy` dialect (HTTP, Alembic support).

## Install

```sh
pip install clickhouse-connect[sqlalchemy]
pip install clickhouse-connect[alembic]  # if you use Alembic for migrations
```

## Engine URL

```python
# Before
create_engine("clickhouse+native://user:pass@host:9000/db")
create_engine("clickhouse+http://user:pass@host:8123/db")

# After
create_engine("clickhousedb+connect://user:pass@host:8123/db")
create_engine("clickhousedb://user:pass@host:8123/db")  # short form alias

# If you import clickhouse_connect.cc_sqlalchemy first, it also registers
# runtime aliases for clickhouse+connect:// and clickhouse://
```

## Import rewrite table

| `clickhouse-sqlalchemy` import                                          | Action  | Replacement                                                                                                                                                                                                                                                                                                                                                                                                         |
|-------------------------------------------------------------------------|---------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `from clickhouse_sqlalchemy import Table`                               | Alias   | `from clickhouse_connect.cc_sqlalchemy import Table`. Pure alias for `sqlalchemy.Table`. The dialect picks up the engine via `construct_arguments`.                                                                                                                                                                                                                                                                 |
| `from clickhouse_sqlalchemy import select as ch_select`                 | Rewrite | `from sqlalchemy import select`. Once `clickhouse_connect.cc_sqlalchemy` has been imported anywhere in the process, plain `select` has our chainables (`.final()`, `.sample()`, `.array_join()`, `.left_array_join()`, `.prewhere()`, `.limit_by()`) attached.                                                                                                                                                      |
| `from clickhouse_sqlalchemy import get_declarative_base`                | Rewrite | Use standard SQLAlchemy declarative setup. For a version-generic form across SQLAlchemy 1.4 and 2.x: `from sqlalchemy.orm import declarative_base; Base = declarative_base()`.                                                                                                                                                                                                                                      |
| `from clickhouse_sqlalchemy.orm.query import Query` (`ClickHouseQuery`) | Drop    | Remove `query_cls=ClickHouseQuery` from your `sessionmaker(...)` call. `clickhouse-sqlalchemy`'s Query subclass adds ClickHouse-specific behavior to `session.query(...)`. In `clickhouse-connect`, the ClickHouse select helpers are attached to `sqlalchemy.select` instead. Your own `query_cls=YourQuery` still works for legacy `session.query(...)` usage, but it is separate from the `select(...)` helpers. |
| `from clickhouse_sqlalchemy import types`                               | Alias   | `from clickhouse_connect.cc_sqlalchemy import types`. Provides `types.DateTime`, `types.UInt32`, `types.LowCardinality`, etc.                                                                                                                                                                                                                                                                                       |
| `from clickhouse_sqlalchemy import engines`                             | Alias   | `from clickhouse_connect.cc_sqlalchemy import engines`. Provides `engines.MergeTree`, `engines.ReplacingMergeTree`, etc.                                                                                                                                                                                                                                                                                            |
| `from clickhouse_sqlalchemy.drivers.http.escaper import Escaper`        | Skip    | See [Escaper](#escaper).                                                                                                                                                                                                                                                                                                                                                                                            |
| `from clickhouse_sqlalchemy.orm.session import make_session`            | Rewrite | Use standard SQLAlchemy session setup directly, for example `Session = sessionmaker(bind=engine); session = Session()`.                                                                                                                                                                                                                                                                                             |

## Type construction

### `DateTime` / `DateTime64`

`timezone=` and `tz=` are aliases. Both work. Passing both raises. `timezone=True` raises. ClickHouse requires a concrete IANA zone string. `timezone=False` is accepted silently because SQLAlchemy passes it during internal type adaptation.

```python
from clickhouse_connect.cc_sqlalchemy.types import DateTime, DateTime64

DateTime(timezone="UTC")
DateTime(tz="UTC")
DateTime64(3, timezone="America/New_York")
```

### `Tuple`

Variadic positional args and the `elements=[...]` list form both work.

```python
from clickhouse_connect.cc_sqlalchemy.types import Tuple, UInt32, UInt64, UUID

Tuple(UInt32, UUID, UInt64)
Tuple(elements=[UInt32, UUID, UInt64])
```

## Select-level chainables

Importing `clickhouse_connect.cc_sqlalchemy` attaches the chainables to plain `sqlalchemy.select`.

```python
import clickhouse_connect.cc_sqlalchemy  # side-effect import: registers chainables on Select
from sqlalchemy import column, func, select
from clickhouse_connect.cc_sqlalchemy import Lambda

select(tbl).array_join(tbl.c.tags)
select(tbl).left_array_join(tbl.c.tags, tbl.c.metric_values)
select(tbl).left_array_join(tbl.c.tags, alias="tag")

select(tbl).final()
select(tbl).sample(0.1)

select(tbl).prewhere(tbl.c.active == 1)
select(tbl).limit_by([tbl.c.user_id], 5)

func.arrayMap(Lambda("x", column("x") * 2), tbl.c.nums)

select(tbl).final().prewhere(tbl.c.active == 1).left_array_join(tbl.c.tags).limit_by([tbl.c.user_id], 3)
```

`final()`, `sample()`, and `limit_by()` use the same calling convention as `clickhouse-sqlalchemy`. `prewhere()` and explicit `Lambda(...)` are available on `sqlalchemy.select` in `clickhouse-connect`.

The dialect compiles these modifiers on SQLAlchemy Select statements, but the ClickHouse server still enforces its own rules. For example, the server rejects `FINAL` on a plain `MergeTree` and `PREWHERE` against `FROM (SELECT ...)`.

## Engine expressions accept `Column` objects

MergeTree-family engines (`partition_by`, `order_by`, `primary_key`, `sample_by`, `ttl`) accept `Column` objects in addition to strings.

```python
partition_date = Column("partition_date", Date)
Table("events", metadata,
    Column("id", UInt64),
    partition_date,
    MergeTree(partition_by=partition_date, order_by="id"),
)
```

Covers the same use case as `clickhouse-sqlalchemy`'s `KeysExpressionOrColumn`. Alembic autogen renders the `Column` as its string name so generated migrations re-import cleanly.

## Cursor identity check

```python
from clickhouse_connect.cc_sqlalchemy import Cursor

if isinstance(cursor, Cursor):
    ...
```

Re-exported from `clickhouse_connect.dbapi.cursor`.

## Patterns to stop using

### Session unit-of-work for writes

ClickHouse does not behave like a row-store OLTP database with meaningful row-level transaction semantics. Even if simple ORM write flows can appear to work, `session.add(obj); session.commit()` is not the pattern to depend on for OLAP workloads. Prefer one of:

- Bulk INSERT: `engine.connect().execute(t.insert(), list_of_row_dicts)`
- Pandas / Arrow: `client.insert_df("t", df)` or `client.insert_arrow("t", arrow_table)`
- Schema changes: Alembic `op.execute(...)`

Session for reads (`session.execute(select(...))`) is fine. Custom `Session` or `Query` subclasses for event hooks, telemetry, or filter helpers (not `clickhouse-sqlalchemy`'s `ClickHouseQuery`, which you drop per the rewrite table) still compose: `sessionmaker(bind=engine, class_=YourSession, query_cls=YourQuery)`.

### Escaper

`clickhouse-sqlalchemy` ships `drivers/http/escaper.py` because its HTTP connector performs client-side DBAPI-style parameter substitution into SQL text, so it needs explicit value-escaping helpers. `clickhouse-connect` has its own query binding and formatting helpers, including support for ClickHouse HTTP external parameters (`{name:Type}`) when you use that syntax directly, so a separate `Escaper` compatibility class is not needed.

| Use case                                 | Replacement                                                                                                                                                                                                                    |
|------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Parameterized SELECT                     | `session.execute(text("... WHERE id = :id"), {"id": value})` for normal SQLAlchemy binds. If you specifically want ClickHouse HTTP external parameters, use `text("... WHERE id = {id:UInt64}")` with the same parameter dict. |
| Identifier quoting                       | `from clickhouse_connect.driver.binding import quote_identifier`                                                                                                                                                               |
| DDL literal (`DEFAULT`, array constants) | `from clickhouse_connect.driver.binding import str_query_value, format_str`                                                                                                                                                    |
| Alembic literal rendering                | Handled by the dialect's ClickHouse type literal rendering. `compile(..., compile_kwargs={"literal_binds": True})` works for supported ClickHouse types.                                                                       |

### `make_session`

`clickhouse-sqlalchemy`'s `make_session(engine)` creates a session and wires in a custom `Query` class. We don't have a custom `Query` class, so use standard SQLAlchemy session setup directly.

```python
# Before
from clickhouse_sqlalchemy import make_session
session = make_session(engine)

# After
from sqlalchemy.orm import sessionmaker
Session = sessionmaker(bind=engine)
session = Session()

# With your own Session/Query subclasses for event hooks or telemetry
# (not clickhouse-sqlalchemy's ClickHouseQuery, which you drop):
Session = sessionmaker(bind=engine, class_=MyOwnSession, query_cls=MyOwnQuery)
session = Session()
```

## Not provided (by design)

- `Escaper` class. See [Escaper](#escaper).
- `KeysExpressionOrColumn`. Covered by native `Column` support in engine params.
- `Lambda(lambda x: 2*x)` AST-introspection form. Use the explicit `Lambda('x', column('x') * 2)` form instead. AST introspection of Python lambdas is brittle across closures and default args.
