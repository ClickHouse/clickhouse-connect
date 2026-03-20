## ClickHouse Connect

A high performance core database driver for connecting ClickHouse to Python, Pandas, and Superset

* Pandas DataFrames (numpy and arrow-backed)
* Numpy Arrays
* PyArrow Tables
* Polars DataFrames
* Superset Connector
* SQLAlchemy Core (select, joins, lightweight deletes; limited feature set)

ClickHouse Connect currently uses the ClickHouse HTTP interface for maximum compatibility.

### Installation

```
pip install clickhouse-connect
```

ClickHouse Connect requires Python 3.9 or higher. We officially test against Python 3.9 through 3.13.

### Superset Connectivity

ClickHouse Connect is fully integrated with Apache Superset. Previous versions of ClickHouse Connect utilized a
dynamically loaded Superset Engine Spec, but as of Superset v2.1.0 the engine spec was incorporated into the main
Apache Superset project and removed from clickhouse-connect in v0.6.0. If you have issues connecting to earlier
versions of Superset, please use clickhouse-connect v0.5.25.

When creating a Superset Data Source, either use the provided connection dialog, or a SqlAlchemy DSN in the form
`clickhousedb://{username}:{password}@{host}:{port}`.

### SQLAlchemy Implementation

ClickHouse Connect includes a lightweight SQLAlchemy dialect implementation focused on compatibility with **Superset**
and **SQLAlchemy Core**.

Supported features include:
- Basic query execution via SQLAlchemy Core
- `SELECT` queries with `JOIN`s, `ARRAY JOIN`, and `FINAL` modifier
- Lightweight `DELETE` statements
- **Alembic** schema migrations (autogenerate, upgrade/downgrade, ClickHouse engine support)

The implementation does not include ORM support and is not intended as a full SQLAlchemy dialect. While it can support
a range of Core-based applications beyond Superset, it may not be suitable for more complex SQLAlchemy applications
that rely on full ORM or advanced dialect functionality.

#### Alembic Migrations

ClickHouse Connect supports [Alembic](https://alembic.sqlalchemy.org/) for schema migrations, including
autogeneration of migration scripts from SQLAlchemy metadata. ClickHouse table engines (`MergeTree`,
`ReplacingMergeTree`, etc.) are preserved through the migration lifecycle.

See the [Alembic README](clickhouse_connect/cc_sqlalchemy/alembic/README.md) for full setup and configuration details.

### Asyncio Support

ClickHouse Connect provides an async wrapper, so that it is possible to use the client in an `asyncio` environment.
See the [run_async example](./examples/run_async.py) for more details.

### Complete Documentation

The documentation for ClickHouse Connect has moved to
[ClickHouse Docs](https://clickhouse.com/docs/integrations/python)
