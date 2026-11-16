from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from tests.integration_tests import conftest


@pytest.mark.parametrize("age_ms,drop", [(6 * 60 * 60 * 1000 - 1, False), (6 * 60 * 60 * 1000, False), (6 * 60 * 60 * 1000 + 1, True)])
def test_stale_function_cutoff(monkeypatch, age_ms, drop):
    now = 1794268800
    monkeypatch.setattr(conftest.time, "time", lambda: now)
    name = f"{conftest.TEST_DATABASE_PREFIX}udf__{now * 1000 - age_ms}__{'a' * 32}__quoted`100%%"
    client = Mock()
    client.query.return_value = SimpleNamespace(result_rows=[(name,)])
    conftest.drop_stale_test_functions(client)
    statement = client.query.call_args.args[0]
    assert "origin = 'SQLUserDefined'" in statement
    assert "startsWith(name, {prefix:String})" in statement
    assert client.query.call_args.kwargs["parameters"] == {"prefix": conftest.TEST_DATABASE_PREFIX}
    if drop:
        quoted_name = name.replace("`", "\\`")
        client.command.assert_called_once_with(f"DROP FUNCTION IF EXISTS `{quoted_name}`", use_database=False)
    else:
        client.command.assert_not_called()


def test_stale_function_sweep_preserves_unowned_names(monkeypatch):
    monkeypatch.setattr(conftest.time, "time", lambda: 1794268800)
    old = "1794200000000"
    unicode_timestamp = "".join(chr(0x660 + int(digit)) for digit in old)
    prefix = conftest.TEST_DATABASE_PREFIX
    names = [
        "plus",
        f"unrelated_udf__{old}__{'a' * 32}",
        f"{prefix}udf__{old}__not_a_uuid",
        f"{prefix}udf__invalid__{'a' * 32}",
        f"{prefix}udf__{unicode_timestamp}__{'a' * 32}",
        f"{prefix}udf__{old}__{'a' * 31}",
        f"{prefix}udf__{old[:-1]}__{'a' * 32}",
        f"{prefix}udf__1794300000000__{'a' * 32}",
    ]
    client = Mock()
    client.query.return_value = SimpleNamespace(result_rows=[(name,) for name in names])
    conftest.drop_stale_test_functions(client)
    client.command.assert_not_called()


def test_stale_function_catalog_error_is_logged(capsys):
    client = Mock()
    client.query.side_effect = RuntimeError("catalog denied")
    conftest.drop_stale_test_functions(client)
    client.command.assert_not_called()
    assert "failed to list stale test functions: catalog denied" in capsys.readouterr().err


def test_stale_function_drop_error_does_not_stop_sweep(monkeypatch, capsys):
    monkeypatch.setattr(conftest.time, "time", lambda: 1794268800)
    names = [f"{conftest.TEST_DATABASE_PREFIX}udf__1794200000000__{letter * 32}" for letter in ("a", "b")]
    client = Mock()
    client.query.return_value = SimpleNamespace(result_rows=[(name,) for name in names])
    client.command.side_effect = [RuntimeError("drop denied"), None]
    conftest.drop_stale_test_functions(client)
    assert client.command.call_count == 2
    assert f"failed to drop stale test function {names[0]}: drop denied" in capsys.readouterr().err


@pytest.mark.parametrize("worker,sweep", [(None, True), ("gw0", True), ("gw1", False)])
def test_only_first_worker_sweeps_stale_functions(monkeypatch, worker, sweep):
    if worker is None:
        monkeypatch.delenv("PYTEST_XDIST_WORKER", raising=False)
    else:
        monkeypatch.setenv("PYTEST_XDIST_WORKER", worker)
    monkeypatch.setenv("CLICKHOUSE_CONNECT_TEST_DATABASE", "user_database")
    sweep_databases = Mock()
    sweep_functions = Mock()
    monkeypatch.setattr(conftest, "drop_stale_test_databases", sweep_databases)
    monkeypatch.setattr(conftest, "drop_stale_test_functions", sweep_functions)
    client = Mock()
    client.query.return_value = SimpleNamespace(result_rows=[("user_database",)])
    config = SimpleNamespace(docker=False, test_database="user_database")
    fixture = conftest.test_client_fixture.__wrapped__(config, lambda: client)
    assert next(fixture) is client
    fixture.close()
    assert sweep_databases.call_count == int(sweep)
    assert sweep_functions.call_count == int(sweep)
