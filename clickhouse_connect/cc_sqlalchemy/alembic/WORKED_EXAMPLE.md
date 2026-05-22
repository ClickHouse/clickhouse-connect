# Alembic worked example

This is a full end-to-end Alembic walkthrough for `clickhouse_connect.cc_sqlalchemy`.

## What is Alembic?

[Alembic](https://alembic.sqlalchemy.org/en/latest/) is a database migration tool for SQLAlchemy that tracks changes in your Python ORM models and generates versioned migration scripts to keep your actual database schema in sync with those models. This gives you a repeatable, reviewable history of schema changes. In practice, that means:

- your models describe the schema you want
- Alembic compares those models to the live database
- Alembic generates revision files
- `alembic upgrade` and `alembic downgrade` apply them to your database

## How people normally use it

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
- `sqlalchemy`
- `alembic`
- `clickhouse-connect`

Install them however you normally manage dependencies. for example:

```bash
pip install sqlalchemy alembic clickhouse-connect
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
        dialect_name="clickhousedb",
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
        default_schema = connection.exec_driver_sql("SELECT currentDatabase()").scalar()
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

## Manual migration operations

Autogenerate handles the common cases, but you can also write migration operations by hand. Here are some examples of ClickHouse-specific operations you can use in your migration scripts.

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
        clickhouse_after="id",
    ),
    schema="analytics",
    if_not_exists=True,
    clickhouse_settings={"alter_sync": 2},
)
```

Alter a column default:

```python
op.alter_column(
    "events",
    "payload",
    schema="analytics",
    existing_type=types.String(),
    server_default=text("'[]'"),
    clickhouse_settings={"alter_sync": 2},
)
```

Drop a column safely:

```python
op.drop_column(
    "events",
    "payload",
    schema="analytics",
    if_exists=True,
)
```

For DDL that Alembic does not model, use `op.execute(...)` directly:

```python
op.execute("CREATE MATERIALIZED VIEW ...")
```

This is common for materialized views, advanced engine rewrites, data-skipping indexes, and codec/TTL changes on existing columns.

## Supported operations

The following operations work with `revision --autogenerate` and `upgrade`:

- create / drop table (including ClickHouse engine preservation)
- create / drop dictionary
- add / alter / drop / rename column
- alter column type, nullability, default, and comment
- generated downgrade support for dropped tables and dictionaries

ClickHouse-specific features supported on that path:

- positional table engines (`MergeTree(order_by=...)`, `ReplacingMergeTree(version=...)`, etc.)
- engine settings (`settings={"index_granularity": 1024}`)
- dictionary `SOURCE`, `LAYOUT`, `LIFETIME`, and `PRIMARY KEY`
- `TextClause` expressions in `partition_by`, `order_by`, and `ttl`
- column `DEFAULT`, `COMMENT`
- `ADD COLUMN ... AFTER ...` placement
- `IF EXISTS` / `IF NOT EXISTS` guards
- operation-level `clickhouse_settings={"alter_sync": 2}`

DDL that is best handled with `op.execute(...)`:

- engine rewrites of existing tables
- codec / TTL / materialized / alias diffs on existing columns
- advanced dictionary rewrites
- materialized views
- data-skipping and secondary index DDL

## Dictionary support

Dictionary metadata autogenerates cleanly:

```python
from sqlalchemy import Column
from clickhouse_connect.cc_sqlalchemy.ddl.dictionary import Dictionary
from clickhouse_connect.cc_sqlalchemy import types

dim_lookup = Dictionary(
    "dim_lookup",
    metadata,
    Column("id", types.UInt64()),
    Column("value", types.String()),
    source="CLICKHOUSE(TABLE 'system.one')",
    layout="FLAT",
    lifetime="MIN 0 MAX 10",
    primary_key="id",
)
```

## Compatibility with clickhouse_sqlalchemy

If you are migrating from `clickhouse_sqlalchemy`, the following compatibility shims are available:

- `from clickhouse_connect.cc_sqlalchemy import engines` (replaces `from clickhouse_sqlalchemy import engines`)
- `from clickhouse_connect.cc_sqlalchemy import types` (replaces `from clickhouse_sqlalchemy import types`)
- `ReplacingMergeTree(version=..., is_deleted=...)` accepts both the new keyword names and the legacy `ver=` alias

The migration work usually consists of updating imports and the SQLAlchemy URL (`clickhousedb://...`) rather than redesigning the migration flow.

## Summary

The intended user experience is:

- create the database once
- write models first
- autogenerate the initial migration from an empty database
- apply migrations with the Alembic CLI
- evolve the models over time
- roll back bad revisions when needed

If you find yourself needing features or hitting bugs, find us on [GitHub](https://github.com/ClickHouse/clickhouse-connect).
