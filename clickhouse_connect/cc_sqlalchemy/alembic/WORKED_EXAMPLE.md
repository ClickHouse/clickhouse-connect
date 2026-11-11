# Alembic worked example

This is a full end-to-end Alembic walkthrough for `clickhouse_connect.cc_sqlalchemy`.

## What is Alembic?

[Alembic](https://alembic.sqlalchemy.org/en/latest/) is a database migration tool for SQLAlchemy that tracks changes in your SQLAlchemy table definitions or ORM models and generates versioned migration scripts to keep your actual database schema in sync with those definitions. This gives you a repeatable, reviewable history of schema changes. In practice, that means:

- your models describe the schema you want
- Alembic compares those models to the live database
- Alembic generates revision files
- `alembic upgrade` and `alembic downgrade` apply them to your database

Alembic does have a Python API, but most users interact with Alembic through the CLI:

```bash
alembic revision --autogenerate -m "message"
alembic upgrade head
alembic downgrade -1
alembic current
alembic history
```

## What we are going to build

We are going to work through a very simple but typical Alembic workflow. Specifically, we will work through the following:

1. Create a ClickHouse database
2. Define an `events` table in a SQLAlchemy model
3. Run Alembic autogenerate against an empty database
4. Apply the initial migration
5. Insert a couple of rows
6. Change the model in a bad way
7. Generate and apply that migration anyway
8. Realize it was wrong and roll it back
9. Fix the model
10. Generate and apply the corrected migration
11. Insert more data

## Prerequisites

You need:

- a running ClickHouse server on `localhost:8123`
- Python
- `clickhouse-connect` installed with the `alembic` extra, which pulls in compatible `sqlalchemy` and `alembic` versions

Install however you normally manage dependencies, for example:

```bash
pip install "clickhouse-connect[alembic]"
```

This guide uses:

- `alembic` for migrations
- `curl` for quick ClickHouse SQL examples

## Step 1: Create a project directory

```bash
mkdir alembic_demo
cd alembic_demo
```

## Step 2: Create the ClickHouse database

When using Alembic, it's typically expected for the database itself to already exist. However, if the tables you've defined in your models don't exist in the database, Alembic will create those.

So the normal from-scratch setup is:

- database exists first
- tables do not

Create the database once:

```bash
curl -sS "http://localhost:8123/" \
  --data-binary "CREATE DATABASE IF NOT EXISTS alembic_demo"
```

At this point:

- `alembic_demo` exists
- it has no application tables yet

It's worth noting that you can you also start using Alembic against tables that already exist and have data, but that's beyond the scope of this simple example.

## Step 3: Create your initial model

Create `models.py`:

```python
from sqlalchemy import Column, MetaData, Table, text

from clickhouse_connect.cc_sqlalchemy import engines, types

metadata = MetaData()

events = Table(
    "events",
    metadata,
    Column("id", types.UInt32(), nullable=False),
    Column("event_name", types.String(), nullable=False),
    Column("created_at", types.DateTime64(3, "UTC"), server_default=text("now64(3)")),
    engines.MergeTree(order_by="id"),
)
```

This is the schema you want. Nothing has been created in ClickHouse yet.

## Step 4: Initialize Alembic

```bash
alembic init alembic
```

That creates:

- `alembic.ini`
- `alembic/env.py`
- `alembic/versions/`

## Step 5: Point Alembic at ClickHouse

Edit `alembic.ini` and set:

```
sqlalchemy.url = clickhousedb://default:@localhost:8123/alembic_demo
```

## Step 6: Replace `alembic/env.py`

The `env.py` file is where Alembic defines how to connect to your database and compare your models to it. The default version is generic, so we have to replace it with a ClickHouse-aware version that teaches Alembic how to correctly autogenerate and run migrations for this dialect.

Therefore, replace the generated `alembic/env.py` with this:

```python
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from clickhouse_connect.cc_sqlalchemy import alembic as ch_alembic
from models import metadata

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = metadata


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        compare_server_default=True,
        include_object=ch_alembic.include_object,
        version_table="alembic_version",
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        default_schema = connection.dialect.default_schema_name
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_schemas=True,
            include_name=ch_alembic.make_include_name(
                include_schemas=frozenset({default_schema}),
                default_schema=default_schema,
            ),
            compare_server_default=True,
            include_object=ch_alembic.include_object,
            process_revision_directives=ch_alembic.clickhouse_writer,
            version_table="alembic_version",
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
```

For ClickHouse specifically, this version of `env.py`:

- `clickhouse_connect.cc_sqlalchemy.alembic` registers the ClickHouse Alembic integration 
- `make_include_name(...)` limits autogenerate to the active database
- `clickhouse_writer(...)` ensures generated revisions include required ClickHouse imports
- `version_table="alembic_version"` configures a compatible version table

On ClickHouse Cloud, add `alter_sync=2` to the migration engine URL so schema
changes finish across replicas before reflection. This matters if a connection is
replaced or autogenerate follows an upgrade. `select_sequential_consistency=1`
covers data reads, not schema changes.

Version-table inserts bypass insert deduplication so downgrades can restore earlier
revision rows. Offline SQL uses `SETTINGS deduplicate_insert = 'disable'` and needs
that setting on the target server. Online migrations fall back to
`insert_deduplicate = 0` when the newer setting is unavailable. This does not change
deduplication for your data tables.

## Step 7: Generate the initial migration

Now we are at the key point where:

- the database exists
- your model exists
- the database is empty

Run:

```bash
alembic revision --autogenerate -m "create events"
```

This tells Alembic to compare the live database which is empty to the target metadata, which has a new table `events` with defined columns and types. This causes it to generate a revision that creates the table according to the defined model.

Typical output from this command would look something like:

```text
Detected added table 'events'
Generating .../alembic/versions/<revision>_create_events.py ...  done
```

## Step 8: Review the generated revision

Open the new file in `alembic/versions/`. You should see an `op.create_table(...)` operation for `events`. This review step is important as a sanity check. Even with autogenerate, it's _highly_ recommended users still manually inspect the migration file before applying it.

## Step 9: Apply the initial migration

```bash
alembic upgrade head
```

In ClickHouse this creates two new tables:

- `events`
- `alembic_version`

## Step 10: Verify the live schema

```bash
curl -sS "http://localhost:8123/?database=alembic_demo" \
  --data-binary "SHOW CREATE TABLE events"
```

You should see something like:

```sql
CREATE TABLE alembic_demo.events
(
    `id` UInt32,
    `event_name` String,
    `created_at` DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY id
```

The exact formatting may differ, and ClickHouse may include default table settings such as `SETTINGS index_granularity = 8192`.

## Step 11: Insert a few rows

Now that the table exists, let's insert some data:

```bash
curl -sS "http://localhost:8123/?database=alembic_demo" \
  --data-binary "INSERT INTO events (id, event_name) FORMAT Values (101, 'signup'), (102, 'purchase')"
```

Verify it with:

```bash
curl -sS "http://localhost:8123/?database=alembic_demo" \
  --data-binary "SELECT id, event_name, created_at FROM events ORDER BY id FORMAT Vertical"
```

At this point you have a normal application state. Schema exists, data exists, and Alembic knows the current revision.

## Step 12: Make a bad model change

Now let's pretend you changed the model, but picked the wrong column name.

Replace `models.py` with:

```python
from sqlalchemy import Column, MetaData, Table, text

from clickhouse_connect.cc_sqlalchemy import engines, types

metadata = MetaData()

events = Table(
    "events",
    metadata,
    Column("id", types.UInt32(), nullable=False),
    Column("event_name", types.String(), nullable=False),
    Column("details", types.String(), nullable=True, server_default=text("'{}'")),
    Column("created_at", types.DateTime64(3, "UTC"), server_default=text("now64(3)")),
    engines.MergeTree(order_by="id"),
)
```

This is a perfectly valid schema change, but in our story it is the wrong one. We really wanted the new column to be called `payload`, not `details`.

## Step 13: Generate the migration for the bad change

```bash
alembic revision --autogenerate -m "add details column"
```

Typical output:

```text
Detected added column 'events.details'
```

## Step 14: Apply it anyway

```bash
alembic upgrade head
```

Verify the schema:

```bash
curl -sS "http://localhost:8123/?database=alembic_demo" \
  --data-binary "SHOW CREATE TABLE events"
```

You should now see `details` in the table definition.

## Step 15: Realize it was wrong and roll it back

This is where Alembic becomes useful in normal development. If the last schema change was wrong, just roll back one revision:

```bash
alembic downgrade -1
```

Now check the table again:

```bash
curl -sS "http://localhost:8123/?database=alembic_demo" \
  --data-binary "SHOW CREATE TABLE events"
```

The `details` column should be gone.

## Step 16: Remove the bad local revision file

For a local mistake that has not been shared yet, the normal next step is to remove the bad revision file you just created.

List the revision files:

```bash
ls alembic/versions
```

Then remove the bad one:

```bash
rm alembic/versions/<bad_revision>_add_details_column.py
```

After `downgrade -1`, your database is back on the previous revision but Alembic's script history still has the bad revision file as the current head. If you leave that file in place, `alembic revision --autogenerate` will tell you the target database is not up to date. Generally, if the bad revision was only local, removing it is fine. However, if the bad revision has already been shared or applied elsewhere, do not delete history. Instead, create a new corrective revision.

## Step 17: Fix the model

Now correct `models.py` so it reflects the schema you actually want:

```python
from sqlalchemy import Column, MetaData, Table, text

from clickhouse_connect.cc_sqlalchemy import engines, types

metadata = MetaData()

events = Table(
    "events",
    metadata,
    Column("id", types.UInt32(), nullable=False),
    Column("event_name", types.String(), nullable=False, comment="Human-readable event name"),
    Column("payload", types.String(), nullable=True, server_default=text("'{}'")),
    Column("created_at", types.DateTime64(3, "UTC"), server_default=text("now64(3)")),
    engines.MergeTree(order_by="id"),
)
```

This corrected change does two things. First, it adds the `payload` column and second, it adds a comment to `event_name`. The point of the second change is strictly to show that Alembic compares the current state of the model, your "ground truth" against what's currently in the live database. So _all_ the changes you make will be reflected.

## Step 18: Generate the corrected migration

```bash
alembic revision --autogenerate -m "add payload and event name comment"
```

Typical output:

```text
Detected added column 'events.payload'
Detected column comment 'events.event_name'
```

## Step 19: Apply the corrected migration

```bash
alembic upgrade head
```

You may also find other inspection commands useful as well.

```bash
alembic current
alembic history
```

## Step 20: Verify the corrected schema

```bash
curl -sS "http://localhost:8123/?database=alembic_demo" \
  --data-binary "SHOW CREATE TABLE events"
```

You should see something like:

```sql
CREATE TABLE alembic_demo.events
(
    `id` UInt32,
    `event_name` String COMMENT 'Human-readable event name',
    `created_at` DateTime64(3, 'UTC') DEFAULT now64(3),
    `payload` Nullable(String) DEFAULT '{}'
)
ENGINE = MergeTree
ORDER BY id
```

The exact formatting may differ, and ClickHouse may include default table settings such as `SETTINGS index_granularity = 8192`.

## Step 21: Insert data using the new schema

Now insert rows that use the new column:

```bash
curl -sS "http://localhost:8123/?database=alembic_demo" \
  --data-binary "INSERT INTO events (id, event_name, payload) FORMAT Values (103, 'refund', '{\"amount\": 12}'), (104, 'renewal', '{\"plan\": \"pro\"}')"
```

Query the table:

```bash
curl -sS "http://localhost:8123/?database=alembic_demo" \
  --data-binary "SELECT id, event_name, payload FROM events ORDER BY id FORMAT Vertical"
```

You should see the original rows still exist and the new rows use the new `payload` column.

## What A Typical Alembic Loop Looks Like

After initial setup, the day-to-day loop is usually relatively simple:

1. Edit your models
2. Run `alembic revision --autogenerate -m "describe change"`
3. Review the generated migration file
4. Run `alembic upgrade head`
5. If needed, run `alembic downgrade -1` and try again

That is the expected Alembic workflow, and it is how this ClickHouse integration is intended to be used.

## Optional: Manual migration operations

You do not need this section for the main walkthrough. Autogenerate handles the common cases shown above, but you can also write migration operations by hand when you want more control over a specific change.

The examples below use the same `alembic_demo.events` table from the walkthrough.

Add a column with placement and operation-level settings:

```python
from alembic import op
from sqlalchemy import Column, text
from clickhouse_connect.cc_sqlalchemy import types

op.add_column(
    "events",
    Column(
        "payload",
        types.String(),
        server_default=text("'{}'"),
        clickhousedb_after="id",
    ),
    schema="alembic_demo",
    if_not_exists=True,
    clickhouse_settings={"alter_sync": 2},
)
```

Alter a column default:

```python
op.alter_column(
    "events",
    "payload",
    schema="alembic_demo",
    existing_type=types.Nullable(types.String()),
    server_default=text("'[]'"),
    clickhouse_settings={"alter_sync": 2},
)
```

Remove a column default or another column property. ClickHouse keeps an existing `DEFAULT` when `MODIFY COLUMN` only restates the type, so removal is an explicit `REMOVE` statement. Passing `None` for `server_default`, `clickhousedb_codec`, `clickhousedb_ttl`, `clickhousedb_alias`, or `clickhousedb_materialized` removes that property. Leaving the argument out keeps it. Setting or removing codec, TTL, alias, or materialized properties alone does not require `existing_type`. Pass SQL expressions with `text(...)`, for example `clickhousedb_ttl=text("ts + INTERVAL 7 DAY")`. If both namespaces are passed to `alter_column`, an explicit `clickhousedb_` value takes precedence over the legacy `clickhouse_` value, including `None`:

```python
op.alter_column(
    "events",
    "payload",
    schema="alembic_demo",
    server_default=None,
    clickhousedb_codec=None,
    clickhouse_settings={"alter_sync": 2},
)
```

Drop a column safely:

```python
op.drop_column(
    "events",
    "payload",
    schema="alembic_demo",
    if_exists=True,
)
```

For DDL that Alembic does not model, use `op.execute(...)` directly with a raw SQL string. That includes `ON CLUSTER` DDL, which is deliberately outside the helper API for now. When you need per-statement ClickHouse settings on the raw path, pass them with `op.execute(sql, execution_options={"settings": {...}})`. Those settings apply in online mode only and are dropped in offline `alembic upgrade --sql` output, so prefer the helpers below when you want the setting to survive as a rendered `SETTINGS` clause.

## Optional: ClickHouse DDL helpers

The integration also registers runtime `op.*` helpers for ClickHouse-specific DDL that standard Alembic does not model: skip indexes, projections, table settings, materialized views, and dictionaries. They render valid ClickHouse SQL in both online and offline `--sql` mode, quote identifiers for you, and helpers with a `clickhouse_settings={...}` parameter render it as an inline `SETTINGS` clause where ClickHouse accepts one. Because Alembic attaches these methods dynamically, static type checkers may not recognize them in migration scripts.

Import the value objects from the integration package:

```python
from sqlalchemy import Column

from clickhouse_connect.cc_sqlalchemy import types
from clickhouse_connect.cc_sqlalchemy.alembic import ClickHouseIndex, ClickHouseProjection
```

### Skip indexes

Add one skip index. The `expression` and `type_` are raw ClickHouse SQL passthrough, so index types keep their full parameter form:

```python
op.add_clickhouse_index(
    "events",
    "idx_event_name",
    "event_name",
    "bloom_filter(0.01)",
    granularity=4,
    schema="alembic_demo",
)
```

Use `first=True` or `after_index="existing_index_name"` only to place an index relative to other indexes. `after_index` is not a column placement option.

Add several skip indexes in a single statement. This is the recommended shape on replicated deployments. See the batching note below:

```python
op.add_clickhouse_indexes(
    "events",
    [
        ClickHouseIndex("idx_event_name", "event_name", "bloom_filter(0.01)", granularity=4),
        ClickHouseIndex("idx_payload", "payload", "minmax", granularity=1),
    ],
    schema="alembic_demo",
)
```

`ADD INDEX` is metadata only. It does not backfill existing parts. Backfill with a separate `MATERIALIZE INDEX`, which runs as a mutation:

```python
op.materialize_clickhouse_index(
    "events",
    "idx_event_name",
    if_exists=True,
    schema="alembic_demo",
    clickhouse_settings={"mutations_sync": 1},
)
```

Drop a skip index. `DROP INDEX` schedules a mutation, so `alter_sync` applies:

```python
op.drop_clickhouse_index(
    "events",
    "idx_event_name",
    if_exists=True,
    schema="alembic_demo",
    clickhouse_settings={"alter_sync": 2},
)
```

Use `op.drop_clickhouse_indexes(table_name, ["idx_a", "idx_b"], if_exists=True, ...)` to drop several skip indexes in one statement.

### Projections

Projections follow the same add, materialize, drop shape. The `select` body is raw SQL passthrough:

```python
op.add_clickhouse_projection(
    "events",
    "proj_by_name",
    "SELECT event_name, count() GROUP BY event_name",
    schema="alembic_demo",
)

op.materialize_clickhouse_projection(
    "events",
    "proj_by_name",
    if_exists=True,
    schema="alembic_demo",
    clickhouse_settings={"mutations_sync": 1},
)

op.drop_clickhouse_projection(
    "events",
    "proj_by_name",
    if_exists=True,
    schema="alembic_demo",
)
```

Use `op.add_clickhouse_projections(table_name, [ClickHouseProjection(...), ...])` to add several in one statement, the same way as `add_clickhouse_indexes`.
Use `first=True` or `after_projection="existing_projection_name"` only to place a projection relative to other projections.
Use `op.drop_clickhouse_projections(table_name, ["proj_a", "proj_b"], if_exists=True, ...)` to drop several in one statement.

### Table settings

Modify or reset MergeTree table settings after creation:

```python
op.modify_clickhouse_table_settings(
    "events",
    {"ttl_only_drop_parts": 1, "merge_with_ttl_timeout": 3600},
    schema="alembic_demo",
)

op.reset_clickhouse_table_settings(
    "events",
    ["merge_with_ttl_timeout"],
    schema="alembic_demo",
)
```

`MODIFY SETTING` is its own statement and is never combined with schema alters, because replicated databases reject a statement that mixes settings changes with schema changes. Settings fixed at creation, such as `index_granularity` and `index_granularity_bytes`, are read-only on the server and cannot be modified. Autogenerate emits these helpers when the `settings` declared on the table engine differ from the server, and refuses read-only settings as unsupported changes. Settings omitted from the model, including settings removed after an earlier migration, stay unchanged because reflection cannot distinguish them from server-injected settings. Use an explicit reset operation to remove them.

### Ordinary views and SQL functions

Ordinary view helpers take a raw SQL SELECT body. SQL function helpers take a raw SQL lambda:

```python
op.create_clickhouse_view("events_view", "SELECT id FROM alembic_demo.events", schema="alembic_demo")
op.create_clickhouse_view("events_view", "SELECT id FROM alembic_demo.events WHERE id > 13", schema="alembic_demo", or_replace=True)
op.drop_clickhouse_view("events_view", schema="alembic_demo", if_exists=True)

op.create_clickhouse_function("demo_increment_v1", "value -> value + 13")
op.create_clickhouse_function("demo_increment_v1", "value -> value + 79", or_replace=True)
op.drop_clickhouse_function("demo_increment_v1", if_exists=True)
```

Create helpers accept `if_not_exists` or `or_replace`, never both. Both default to `False`. Names are quoted; function names are single identifiers and have no database qualifier or `schema` argument. SQL UDFs are server-global, so give them unique or versioned names and coordinate changes across databases. Replacement requires both create and drop function privileges.

`CREATE OR REPLACE VIEW` can replace a table or materialized view with the same name and discard its data. `DROP VIEW` also accepts materialized views. Review collisions before opting into replacement. `IF NOT EXISTS` preserves an existing definition without validating it. Guarded creates and replacements refuse automatic reversal because the old state is unknown. Write explicit downgrade operations with the old definitions for replacements and drops. They cannot recover discarded data.

`inspect(connection).get_view_definition(name, schema=...)` returns a view's stored SELECT. `get_clickhouse_function_names()` lists SQL UDFs only, and `get_clickhouse_function_definition(name)` returns the full stored `CREATE FUNCTION` statement unchanged. Missing functions raise `sqlalchemy.exc.NoResultFound`. SQL functions have no schema parameter. The stored CREATE statement is for inspection or explicit replay; the create helper still expects only the lambda body.

These helpers work online and offline, with sync connections and async `run_sync`. They do not add autogenerate comparison or dependency ordering. Render them with `render_as_batch=False` and order explicit revisions yourself.

### Materialized views

Create and drop materialized views with an explicit target table. The `select` argument is raw SQL passthrough:

```python
op.create_clickhouse_materialized_view(
    "events_mv",
    "events_rollup",
    "SELECT id, event_name FROM alembic_demo.events",
    if_not_exists=True,
    schema="alembic_demo",
    to_schema="alembic_demo",
)

op.drop_clickhouse_materialized_view(
    "events_mv",
    if_exists=True,
    schema="alembic_demo",
)
```

This helper intentionally models the `TO ... AS SELECT ...` form. ClickHouse rejects combining `TO` with `ENGINE` or `POPULATE`, so use `op.execute(...)` for exotic materialized view shapes. `create_clickhouse_materialized_view` does not accept `clickhouse_settings`, because ClickHouse parses `SETTINGS` after `AS SELECT` as part of the stored SELECT definition rather than as one-shot DDL settings.

### Dictionaries

Create and drop dictionaries directly:

```python
op.create_clickhouse_dictionary(
    "my_dictionary",
    [
        Column("id", types.UInt32()),
        Column("value", types.String()),
    ],
    primary_key="id",
    source="CLICKHOUSE(TABLE 'dictionary_source')",
    layout="FLAT",
    lifetime="MIN 0 MAX 10",
    if_not_exists=True,
    schema="alembic_demo",
)

op.drop_clickhouse_dictionary(
    "my_dictionary",
    if_exists=True,
    schema="alembic_demo",
)
```

Reload a dictionary created through the `Dictionary` construct. This runs on the node that receives it and is not distributed:

```python
op.reload_clickhouse_dictionary("my_dictionary", schema="alembic_demo")
```

### Why the plural helpers emit one statement

On a `ReplicatedMergeTree` table, emitting several separate `ALTER TABLE` statements against the same table can fail with `Code: 517 CANNOT_ASSIGN_ALTER`. Each ALTER commits table metadata through a ZooKeeper compare-and-set on the metadata version, and concurrent alters can lose that race. Combining compatible subcommands into one comma-joined `ALTER TABLE` removes the race for the common case.

`add_clickhouse_indexes` and `add_clickhouse_projections` do exactly that. They combine only homogeneous pure-metadata subcommands, which is the one shape valid on both plain and replicated databases. `MATERIALIZE` operations always run as separate statements, and `MODIFY SETTING` is always its own statement, because a replicated database allows only one command bucket per statement and rejects mixing a materialize or a settings change with schema alters.

### Which sync setting applies

The two ClickHouse sync settings apply to different operations, and using the wrong one has no effect:

- `MATERIALIZE INDEX` and `MATERIALIZE PROJECTION` run as mutations. They wait according to `mutations_sync`.
- `DROP INDEX` and `DROP PROJECTION` schedule a mutation. They wait according to `alter_sync`.
- `ADD INDEX`, `ADD PROJECTION`, `MODIFY SETTING`, and `RESET SETTING` are metadata only. There is no mutation to wait for, so neither sync setting changes their behavior.

Recommend values 0, 1, or 2 only. The value 3 is documented but not implemented for OSS `ReplicatedMergeTree` and falls through to no wait at all.

On a replicated database, all DDL goes through a distributed queue governed by `distributed_ddl_task_timeout` and `distributed_ddl_output_mode`. The defaults are migration safe.

## What This Example Covered

This walkthrough focused on the parts of the integration most users need first:

- creating a ClickHouse table from SQLAlchemy metadata
- preserving the ClickHouse table engine in generated migrations
- adding and dropping columns with autogenerate
- changing a column default and comment
- applying and rolling back revisions with the Alembic CLI
- inserting and querying data before and after schema changes
- using optional manual column operations when autogenerate is not enough
- using the ClickHouse DDL helpers for skip indexes, projections, table settings, materialized views, and dictionaries

## Migrating from `clickhouse_sqlalchemy`

If you are coming from [`clickhouse_sqlalchemy`](https://github.com/xzkostyan/clickhouse-sqlalchemy), see [MIGRATING_FROM_CLICKHOUSE_SQLALCHEMY.md](../MIGRATING_FROM_CLICKHOUSE_SQLALCHEMY.md) for the import and URL changes you need.

## Summary

The intended user experience is:

- create the database once
- write models first
- autogenerate the initial migration from an empty database
- apply migrations with the Alembic CLI
- evolve the models over time
- roll back bad revisions when needed

If you find yourself needing features or hitting bugs, find us on [GitHub](https://github.com/ClickHouse/clickhouse-connect).

## Upgrade existing index and projection migrations

Alembic compares explicit skip indexes and SELECT projections by default. Objects
created only by earlier imperative migrations generate drops when absent from
managed table metadata. Before upgrading, declare them with `ClickHouseIndex` or
`ClickHouseProjection`, filter unmanaged objects with `include_name` or
`include_object`, or pass `clickhousedb_compare_indexes=False` or
`clickhousedb_compare_projections=False` to `context.configure()` for the relevant
object type. Each opt-out skips its catalog reads and table-drop snapshots.

Edits generate separate drop and add operations. Downgrade restores definitions,
but neither direction restores prior materialization. Replacing a projection
discards its existing projection parts. Review that rebuild cost and call
`op.materialize_clickhouse_projection` explicitly when existing parts need the
replacement immediately. Index materialization also remains explicit.

## Declarative skip indexes

Use `ClickHouseIndex` table arguments to include indexes in generated `CREATE TABLE`
statements and to compare them on existing tables:

```python
from clickhouse_connect.cc_sqlalchemy import ClickHouseIndex

# Inside Table(...):
ClickHouseIndex("value_idx", "value", "minmax", granularity=4)
```

Explicit indexes absent from managed table metadata generate drops by default.
Catalog classification and server normalization
must be available when comparison is enabled. Unavailable catalog access,
expression resolution, or expected normalization output raises
`IndexComparisonError` with structured definitions and a reason. Comparison uses
the server's structural normal form. It does not prove arbitrary mathematical
equivalence. When normalization succeeds but the resulting forms differ,
autogenerate generates replacement operations.

Same-name edits generate separate drop and add operations. Policy hooks should
pair both index and projection edits by schema, table, and object name. Do not
rely on adjacency. Their downgrades
restore definitions, including indexes on a recreated dropped table, but do not
restore old part materialization. Review the generated operations before running
them and call `op.materialize_clickhouse_index` explicitly when required.

An edited index can reference a physical column added in the same revision.
When analysis names that scheduled new column as an unknown identifier,
autogenerate validates the full expression with the added columns available,
then emits `add_column` followed by the index drop and add. Downgrade restores
the old index before dropping the column. Other unknown identifiers, including
typos and columns excluded by filters, still raise `IndexComparisonError`.
Changed `ALIAS` definitions that affect an index still require an explicit column
migration before index comparison.

Generate index add, drop, and edit operations with `render_as_batch=False`.
ClickHouse index helpers do not participate in Alembic's deferred batch queue,
so batch rendering of those operations raises an actionable error. Inline table
creation and drop-table downgrade snapshots remain supported.

Other custom ClickHouse operations inside a batch block, including table settings,
projections, and materialization, are also refused during rendering.

For physical batch recreation, pass a complete `copy_from` Table containing its
`ClickHouseIndex` definitions. Automatic recreation reads the explicit-index
catalog at execution time and refuses tables with explicit indexes before
creating or dropping tables. Ordinary table autoload still omits skip indexes.
The autogenerate comparison opt-out does not bypass this execution guard.

## Declarative SELECT projections

Use `ClickHouseProjection` table arguments to include projections in generated CREATE statements and compare existing tables:

```python
from clickhouse_connect.cc_sqlalchemy import ClickHouseProjection

# Inside Table(...):
ClickHouseProjection("by_label", "SELECT value, label ORDER BY label, value")
```

Declarations own the projection set on managed tables by default. An undeclared projection generates a drop. A changed SELECT generates a separate drop followed by an add. On each table, autogenerate orders projection drops, column additions and alterations, index operations, projection additions, then column drops. Alembic appends column drops after the table comparators finish. Other operations can appear between an edited projection's drop and add. Downgrade restores the previous definition. Table-drop downgrades also restore projection declarations. No `MATERIALIZE PROJECTION` is generated, so existing parts need explicit materialization if the projection must serve them immediately.

For `AggregatingMergeTree` and other engines with nonstandard merge behavior, ClickHouse rejects `ADD PROJECTION` when `deduplicate_merge_projection_mode='throw'`. Set it explicitly to `drop` or `rebuild` before applying projection additions or replacements, including replacements of projections originally declared inline.

Projection filters receive `type_="projection"`. Object filters expose the definition and `.table`, with the usual `reflected` and `compare_to` values. Inline CREATE declarations are filtered before policy hooks run.

Comparison follows the server's projection grammar formatting and case-insensitive function-name normalization, preserving wildcards, aliases, GROUP BY, and ORDER BY. Case-sensitive names such as `sumIf` and `SumIf` remain distinct. When the formatted definitions differ, comparison also checks the parsed syntax tree, which ignores grouping parentheses. Redundant parentheses and operator and function spellings such as `value + 13` and `plus(value, 13)` compare equally, because servers store redundant parentheses inconsistently. Other structural differences generate a replacement. Comparison does not prove arbitrary SQL equivalence, analyze an ordinary SELECT, or validate column dependencies. Review changes involving columns and ALIAS definitions. Invalid DDL dependencies fail at execution. A failed add leaves the preceding drop applied. Catalog, formatter, or function-metadata failures raise `ProjectionComparisonError` with structured definitions and a reason instead of generating a replacement. Projection-level `WITH SETTINGS` and index-based forms also raise rather than lose information. Filter them or use explicit migrations. If `query_masking_rules` changes the SELECT text returned by the catalog, use explicit migrations, filters, or the comparison opt-out for affected projections.

`inspect(connection).get_clickhouse_projections("events")` returns replayable SELECT definitions. Ordinary table autoload omits projections. For physical batch recreation, pass a complete `copy_from` Table with `clickhousedb_projections`, optionally populated from that inspection method. Automatic online recreation refuses tables with projections before DDL. Generate custom projection operations with `render_as_batch=False`. Inline CREATE and table-drop snapshots are supported.

### Classify generated operations before rendering

All 21 ClickHouse migration operation classes are public from
`clickhouse_connect.cc_sqlalchemy.alembic` and its `operations` module. Use
`isinstance` to inspect them in `process_revision_directives` or register them
with Alembic's `Rewriter.rewrites`. The old underscore names remain aliases of
the same classes, so existing registrations and imports keep working.

The public inspection fields match constructor arguments. Table operations expose
`table_name`, `schema`, and `clickhouse_settings`. Single index operations expose
`name`. Additions also expose `expression`, `type_`, `granularity`, `if_not_exists`,
`first`, and `after_index`. Projection additions expose `select` and
`after_projection` instead of index expression, type, and granularity fields. Drops
expose `if_exists` and, for single indexes or projections, `existing_index` or
`existing_projection` when a prior definition is available. Materializations
expose `if_exists` and `partition`. Grouped additions store `indexes` or
`projections` as tuples of `ClickHouseIndex` or `ClickHouseProjection`. Grouped
drops store `names` as a tuple. Inspect every member of a grouped operation.

`ModifyClickHouseTableSettingsOp` exposes `settings` and optional
`existing_settings`. `ResetClickHouseTableSettingsOp` exposes a tuple of `names`.
These table settings differ from the query-level `clickhouse_settings` mapping.
Create view and function operations expose `name`, `select` or `expression`,
`if_not_exists`, and `or_replace`. Views also expose `schema`. Materialized-view
creation exposes `name`, `schema`, `to_table`, `to_schema`, `select`, and
`if_not_exists`. Dictionary creation exposes `name`, `schema`, a `columns` tuple,
`primary_key`, `source`, `layout`, `lifetime`, `comment`, `if_not_exists`, and
`clickhouse_settings`. View, function, materialized-view, and dictionary drops expose
`name` and `if_exists`, plus `schema` and `clickhouse_settings` where accepted by
the constructor. Dictionary reload exposes `name` and `schema`. SQL fragments
remain strings. Mappings retain their supplied values. Reverse operations can
require prior definitions or raise `NotImplementedError`. A reversible definition
does not restore materialized data.

This example is an application policy, not a driver safety guarantee. It permits
only an explicitly chosen index shape or table setting, sends replacements and
unknown operations for review, and rejects standalone index drops, standalone
projection drops, and destructive built-in operations. Change these rules for your deployment. Even
creation needs field inspection: `or_replace=True` on a view can discard a
colliding table's data, and raw SQL bodies and dictionary sources need review.

```python
import logging

from alembic.operations import ops
from clickhouse_connect.cc_sqlalchemy import alembic as ch_alembic


def leaves(operation):
    if isinstance(operation, ops.OpContainer):
        for child in operation.ops:
            yield from leaves(child)
    else:
        yield operation


def object_key(operation):
    if isinstance(operation, (ch_alembic.AddClickHouseIndexOp,
                              ch_alembic.DropClickHouseIndexOp)):
        kind = "index"
    elif isinstance(operation, (ch_alembic.AddClickHouseProjectionOp,
                                ch_alembic.DropClickHouseProjectionOp)):
        kind = "projection"
    else:
        return None
    return kind, operation.schema, operation.table_name, operation.name


def classify_tree(tree, direction):
    operations = list(leaves(tree))
    adds = {object_key(op) for op in operations if isinstance(
        op, (ch_alembic.AddClickHouseIndexOp, ch_alembic.AddClickHouseProjectionOp))}
    drops = {object_key(op) for op in operations if isinstance(
        op, (ch_alembic.DropClickHouseIndexOp, ch_alembic.DropClickHouseProjectionOp))}
    replacements = adds & drops
    for operation in operations:
        verdict, reason = "review", "operation needs explicit review"
        if isinstance(operation, (ops.DropTableOp, ops.DropColumnOp, ops.ExecuteSQLOp)):
            verdict, reason = "rejected", "destructive change or opaque SQL"
        elif object_key(operation) in replacements:
            reason = "replacement can discard existing materialization"
        elif isinstance(operation, (ch_alembic.DropClickHouseIndexOp,
                                    ch_alembic.DropClickHouseProjectionOp,
                                    ch_alembic.DropClickHouseIndexesOp,
                                    ch_alembic.DropClickHouseProjectionsOp)):
            verdict, reason = "rejected", "removal requires a separate migration"
        elif direction == "upgrade":
            if isinstance(operation, ch_alembic.AddClickHouseIndexOp):
                if (operation.type_ == "minmax" and operation.expression == "value"
                        and operation.granularity == 1 and not operation.first
                        and operation.after_index is None and not operation.clickhouse_settings):
                    verdict, reason = "allowed", "approved index definition"
            elif isinstance(operation, ch_alembic.ModifyClickHouseTableSettingsOp):
                if (dict(operation.settings) == {"max_bytes_to_merge_at_max_space_in_pool": 107374182400}
                        and not operation.clickhouse_settings):
                    verdict, reason = "allowed", "approved table setting value"
        yield direction, operation, verdict, reason


def review_revision(context, revision, directives):
    # Scope: a single-engine env.py. clickhouse_writer uses singular roots.
    for script in directives:
        if len(script.upgrade_ops_list) != 1 or len(script.downgrade_ops_list) != 1:
            raise ValueError("This policy hook supports one operation stream only")
        findings = [
            *classify_tree(script.upgrade_ops_list[0], "upgrade"),
            *classify_tree(script.downgrade_ops_list[0], "downgrade"),
        ]
        for direction, op, verdict, reason in findings:
            logging.getLogger("alembic.policy").info(
                "%s: %s: %s: %s", direction, type(op).__name__, verdict, reason)
        blocked = [f"{direction}: {type(op).__name__}: {verdict}: {reason}"
                   for direction, op, verdict, reason in findings
                   if direction == "upgrade" and verdict != "allowed"]
        if blocked:
            raise ValueError("Migration requires review:\n" + "\n".join(blocked))
    ch_alembic.clickhouse_writer(context, revision, directives)
```

Pass `process_revision_directives=review_revision` to `context.configure()` in
place of `clickhouse_writer`. The hook logs findings for both directions and gates revision generation on
upgrade findings. Downgrade findings require a separate review before executing
a downgrade. They do not block writing an approved upgrade. The hook traverses nested containers and pairs replacements by
kind, schema, table, and name, regardless of adjacency. Grouped additions and all
unrecognized operations default to review. This recipe supports one engine and
one operation stream per direction. Using list roots in the classifier does not
make `clickhouse_writer` a multi-engine hook. An empty generated tree proves only
that enabled, supported comparisons found no changes. Views, functions,
dictionaries, and other unsupported comparisons still need explicit migrations.
