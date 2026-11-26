"""Regression test: a session that turns readonly after connect must keep working (issue #1238)."""

import asyncio
import os
import uuid

import pytest

import clickhouse_connect

HOST = os.environ.get("CLICKHOUSE_CONNECT_TEST_HOST", "localhost")
PORT = int(os.environ.get("CLICKHOUSE_CONNECT_TEST_PORT", "8123"))
QUERY = "SELECT toUInt8(getSetting('readonly'))"


def _skip_if_no_server():
    try:
        probe = clickhouse_connect.get_client(host=HOST, port=PORT)
        probe.close()
    except Exception:  # pylint: disable=broad-except
        pytest.skip(f"No ClickHouse server reachable at {HOST}:{PORT}")


@pytest.mark.parametrize("compress", [True, False])
def test_queries_work_after_session_becomes_readonly(compress):
    _skip_if_no_server()
    client = clickhouse_connect.get_client(host=HOST, port=PORT, compress=compress, session_id=f"ro-{uuid.uuid4()}")
    try:
        client.command("SET readonly = 1")
        # Every query after the switch must succeed, not just the first one
        for _ in range(3):
            assert client.query(QUERY).result_rows == [(1,)]
    finally:
        client.close()


@pytest.mark.parametrize("compress", [True, False])
def test_async_queries_work_after_session_becomes_readonly(compress):
    _skip_if_no_server()

    async def run():
        client = await clickhouse_connect.get_async_client(host=HOST, port=PORT, compress=compress, session_id=f"ro-{uuid.uuid4()}")
        try:
            await client.command("SET readonly = 1")
            for _ in range(3):
                result = await client.query(QUERY)
                assert result.result_rows == [(1,)]
        finally:
            await client.close()

    asyncio.run(run())
