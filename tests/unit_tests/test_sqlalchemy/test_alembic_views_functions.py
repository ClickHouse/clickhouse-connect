from io import StringIO
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from alembic.autogenerate.api import render_python_code
from alembic.operations import Operations, ops
from alembic.runtime.migration import MigrationContext
from alembic.util import CommandError
from sqlalchemy.exc import NoResultFound, SQLAlchemyError

from clickhouse_connect.cc_sqlalchemy.alembic.operations import (
    _CreateClickHouseFunctionOp,
    _CreateClickHouseViewOp,
    _DropClickHouseFunctionOp,
    _DropClickHouseViewOp,
)
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect
from clickhouse_connect.cc_sqlalchemy.inspector import get_clickhouse_function_definition, get_clickhouse_function_names


@pytest.mark.parametrize("kind", ["view", "function"])
@pytest.mark.parametrize("guard", [{}, {"if_not_exists": True}, {"or_replace": True}])
def test_create_offline_and_renderer_round_trip(kind, guard):
    buffer = StringIO()
    context = MigrationContext.configure(dialect=ClickHouseDialect(), opts={"as_sql": True, "output_buffer": buffer})
    cls = _CreateClickHouseViewOp if kind == "view" else _CreateClickHouseFunctionOp
    body = "SELECT 'user_1\\n100%%' AS label" if kind == "view" else "(value) -> concat(value, '100%%')"
    kwargs = {**guard, **({"schema": "db`quoted"} if kind == "view" else {})}
    operation = cls("quoted`name.100%%", body, **kwargs)
    code = render_python_code(ops.UpgradeOps([operation]), migration_context=MigrationContext.configure(dialect=ClickHouseDialect()))
    assert f"op.create_clickhouse_{kind}(" in code
    # Execute rendered code through Alembic's proxy-shaped namespace.
    exec("\n".join(line[4:] for line in code.splitlines() if line.startswith("    ")), {"op": Operations(context)})
    prefix = "CREATE OR REPLACE" if guard.get("or_replace") else "CREATE"
    exists = " IF NOT EXISTS" if guard.get("if_not_exists") else ""
    name = "`db\\`quoted`.`quoted\\`name.100%%`" if kind == "view" else "`quoted\\`name.100%%`"
    assert buffer.getvalue().strip() == f"{prefix} {kind.upper()}{exists} {name} AS {body};"


@pytest.mark.parametrize("cls", [_CreateClickHouseViewOp, _CreateClickHouseFunctionOp])
def test_creation_validation_and_reverse_safety(cls):
    with pytest.raises(ValueError, match="mutually exclusive"):
        cls("managed", "body", if_not_exists=True, or_replace=True)
    for guard in ({"if_not_exists": True}, {"or_replace": True}):
        with pytest.raises(NotImplementedError, match="explicit downgrade"):
            cls("managed", "body", **guard).reverse()
    reverse = cls("managed", "body").reverse()
    assert isinstance(reverse, _DropClickHouseViewOp if cls is _CreateClickHouseViewOp else _DropClickHouseFunctionOp)
    assert reverse.name == "managed" and reverse.if_exists


@pytest.mark.parametrize("cls", [_DropClickHouseViewOp, _DropClickHouseFunctionOp])
@pytest.mark.parametrize("if_exists", [False, True])
def test_drop_sql_renderer_and_irreversibility(cls, if_exists):
    buffer = StringIO()
    context = MigrationContext.configure(dialect=ClickHouseDialect(), opts={"as_sql": True, "output_buffer": buffer})
    kwargs = {"schema": "warehouse"} if cls is _DropClickHouseViewOp else {}
    operation = cls("managed", if_exists=if_exists, **kwargs)
    code = render_python_code(ops.UpgradeOps([operation]), migration_context=MigrationContext.configure(dialect=ClickHouseDialect()))
    exec("\n".join(line[4:] for line in code.splitlines() if line.startswith("    ")), {"op": Operations(context)})
    kind = "VIEW" if kwargs else "FUNCTION"
    name = "`warehouse`.`managed`" if kwargs else "`managed`"
    assert buffer.getvalue().strip() == f"DROP {kind}{' IF EXISTS' if if_exists else ''} {name};"
    with pytest.raises(NotImplementedError):
        operation.reverse()


@pytest.mark.parametrize(
    "operation",
    [
        _CreateClickHouseViewOp("v", "SELECT 13"),
        _CreateClickHouseFunctionOp("f", "x -> x + 13"),
        _DropClickHouseViewOp("v"),
        _DropClickHouseFunctionOp("f"),
    ],
)
def test_batch_rendering_refused(operation):
    with pytest.raises(CommandError, match="render_as_batch=False"):
        render_python_code(
            ops.UpgradeOps([ops.ModifyTableOps("table", [operation])]),
            render_as_batch=True,
            migration_context=MigrationContext.configure(dialect=ClickHouseDialect()),
        )


def test_function_catalog_bound_names_and_raw_definition():
    connection = Mock()
    connection.execute.return_value = [SimpleNamespace(name="user_1"), SimpleNamespace(name="user_2")]
    assert get_clickhouse_function_names(connection) == ["user_1", "user_2"]
    statement = connection.execute.call_args.args[0]
    assert "origin = 'SQLUserDefined'" in str(statement) and "ORDER BY name" in str(statement)
    definition = "CREATE FUNCTION user_1 AS x -> concat(x, '100%')\n"
    connection.execute.return_value = Mock(first=lambda: SimpleNamespace(create_query=definition))
    assert get_clickhouse_function_definition(connection, "name'quoted") == definition
    statement, parameters = connection.execute.call_args.args
    assert "origin = 'SQLUserDefined'" in str(statement)
    assert parameters == {"function_name": "name'quoted"}
    assert "name'quoted" not in str(statement)
    connection.execute.return_value.first = lambda: None
    with pytest.raises(NoResultFound, match="SQL function"):
        get_clickhouse_function_definition(connection, "missing")
    connection.execute.side_effect = SQLAlchemyError("catalog denied")
    with pytest.raises(SQLAlchemyError, match="catalog denied"):
        get_clickhouse_function_names(connection)


def test_parenthesized_view_name_preserves_schema():
    buffer = StringIO()
    context = MigrationContext.configure(dialect=ClickHouseDialect(), opts={"as_sql": True, "output_buffer": buffer})
    migration = Operations(context)
    migration.create_clickhouse_view("(managed)", "SELECT 13", schema="warehouse")
    migration.drop_clickhouse_view("(managed)", schema="warehouse")
    assert buffer.getvalue().splitlines() == [
        "CREATE VIEW `warehouse`.`(managed)` AS SELECT 13;",
        "",
        "DROP VIEW `warehouse`.`(managed)`;",
        "",
    ]
