from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect


class _Row:
    def __init__(self, name):
        self.name = name


class _FakeConnection:
    """Minimal stand-in that records executed SQL and returns canned rows."""

    def __init__(self, database_names):
        self._database_names = database_names
        self.executed = []

    def execute(self, clause):
        self.executed.append(str(clause))
        return [_Row(n) for n in self._database_names]


def test_has_database_uses_show_databases():
    # SHOW DATABASES lists DataLakeCatalog databases; system.databases does not
    # (unless show_data_lake_catalogs_in_system_tables=1). Regression test for #849.
    conn = _FakeConnection(["default", "my_lake"])

    assert ClickHouseDialect.has_database(conn, "my_lake") is True
    assert ClickHouseDialect.has_database(conn, "missing") is False

    assert any("SHOW DATABASES" in sql for sql in conn.executed)
    assert all("system.databases" not in sql for sql in conn.executed)
