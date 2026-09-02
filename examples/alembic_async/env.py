import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from clickhouse_connect.cc_sqlalchemy import alembic as ch_alembic

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Import your application's metadata and assign it here for autogenerate.
target_metadata = None


def run_migrations_offline() -> None:
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_server_default=True,
        include_object=ch_alembic.include_object,
        dialect_name="clickhousedb",
        version_table="alembic_version",
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
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


async def run_migrations_online() -> None:
    configuration = config.get_section(config.config_ini_section, {})
    connectable = async_engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    try:
        async with connectable.connect() as connection:
            await connection.run_sync(do_run_migrations)
    finally:
        await connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
