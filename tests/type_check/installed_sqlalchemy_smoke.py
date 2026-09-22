import sys
from importlib.metadata import entry_points
from pathlib import Path

from sqlalchemy.engine import make_url


def main() -> None:
    assert not (Path.cwd() / "clickhouse_connect").exists()
    assert not any(name.startswith("clickhouse_connect.cc_sqlalchemy") for name in sys.modules)

    expected = {
        "clickhousedb": "clickhouse_connect.cc_sqlalchemy.dialect:ClickHouseDialect",
        "clickhousedb.connect": "clickhouse_connect.cc_sqlalchemy.dialect:ClickHouseDialect",
        "clickhousedb.async": "clickhouse_connect.cc_sqlalchemy.asyncio:ClickHouseAsyncDialect",
    }
    advertised = {
        entry_point.name: entry_point.value
        for entry_point in entry_points(group="sqlalchemy.dialects")
        if entry_point.dist is not None and entry_point.dist.name == "clickhouse-connect"
    }
    assert advertised == expected

    sync_dialect = make_url("clickhousedb://").get_dialect()
    assert sync_dialect.__module__ == "clickhouse_connect.cc_sqlalchemy.dialect"
    assert sync_dialect.__name__ == "ClickHouseDialect"
    assert "clickhouse_connect.cc_sqlalchemy.asyncio" not in sys.modules

    async_dialect = make_url("clickhousedb+async://").get_dialect()
    assert async_dialect.__module__ == "clickhouse_connect.cc_sqlalchemy.asyncio"
    assert async_dialect.__name__ == "ClickHouseAsyncDialect"
    assert "clickhouse_connect.cc_sqlalchemy.asyncio" in sys.modules


if __name__ == "__main__":
    main()
