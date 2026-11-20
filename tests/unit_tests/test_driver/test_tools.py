from unittest.mock import AsyncMock, Mock

import pytest

from clickhouse_connect.driver.tools import insert_file, insert_file_async


@pytest.mark.parametrize("async_mode", [False, True])
@pytest.mark.parametrize(
    "table, database, expected",
    [
        ("db.t", None, "`db`.`t`"),
        ("`db`.`t`", None, "`db`.`t`"),
        ("db.t", "other", "`other`.`db.t`"),
        (
            "db.other SELECT id FROM db.secret WHERE id NOT IN (SELECT id FROM input('id UInt64')) OR id IN",
            None,
            "`db`.`other SELECT id FROM db`.`secret WHERE id NOT IN (SELECT id FROM input(\\'id UInt64\\')) OR id IN`",
        ),
    ],
)
@pytest.mark.asyncio
async def test_insert_file_quotes_table(tmp_path, async_mode, table, database, expected):
    path = tmp_path / "rows.csv"
    path.write_bytes(b"id\n13\n")
    client = Mock()
    if async_mode:
        client.raw_insert = AsyncMock()
        await insert_file_async(client, table, str(path), database=database)
    else:
        insert_file(client, table, str(path), database=database)

    assert client.raw_insert.call_args.args[0] == expected
