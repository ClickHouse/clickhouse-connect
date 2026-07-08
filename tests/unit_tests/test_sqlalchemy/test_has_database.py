from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect
from clickhouse_connect.driver.binding import quote_identifier


class _FakeResult:
    def __init__(self, exists):
        self._exists = exists

    def fetchone(self):
        return (1 if self._exists else 0,)


class _FakeConnection:
    """Minimal stand-in that records executed SQL and answers EXISTS DATABASE."""

    def __init__(self, database_names):
        self._existing = {quote_identifier(n) for n in database_names}
        self.executed = []

    def execute(self, clause):
        sql = str(clause)
        self.executed.append(sql)
        quoted = sql.split("EXISTS DATABASE ", 1)[-1].strip()
        return _FakeResult(quoted in self._existing)


def test_has_database_uses_exists_database():
    # EXISTS DATABASE sees DataLakeCatalog databases; system.databases omitted them
    # by default before server 26.5. Regression test for #849.
    conn = _FakeConnection(["default", "my_lake"])

    assert ClickHouseDialect.has_database(conn, "my_lake") is True
    assert ClickHouseDialect.has_database(conn, "missing") is False

    assert all(sql.startswith("EXISTS DATABASE") for sql in conn.executed)
    assert all("system.databases" not in sql for sql in conn.executed)
    assert any(quote_identifier("my_lake") in sql for sql in conn.executed)
