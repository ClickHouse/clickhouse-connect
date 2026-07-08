from __future__ import annotations

from collections.abc import Generator, Sequence
from typing import Any

import numpy
import pandas
from typing_extensions import assert_type

import clickhouse_connect
from clickhouse_connect.driver import AsyncClient, Client, create_async_client, create_client
from clickhouse_connect.driver.query import QueryResult
from clickhouse_connect.driver.summary import QuerySummary


def entry_points() -> None:
    # Every name in clickhouse_connect.__all__ must stay importable and typed.
    assert_type(clickhouse_connect.__version__, str)
    assert_type(clickhouse_connect.driver_name, str)
    assert_type(clickhouse_connect.get_client(host="localhost"), Client)
    assert_type(create_client(host="localhost"), Client)


def sync_query() -> None:
    client = clickhouse_connect.get_client(host="localhost", username="default", password="")
    result = client.query("SELECT number FROM system.numbers LIMIT 13")
    assert_type(result, QueryResult)
    assert_type(result.result_rows, Sequence[Sequence[Any]])
    assert_type(result.result_set, Sequence[Sequence[Any]])
    assert_type(result.named_results(), Generator[dict[Any, Any], None, None])

    assert_type(client.query_df("SELECT 13"), pandas.DataFrame)
    assert_type(client.query_np("SELECT 13"), numpy.ndarray)
    assert_type(client.ping(), bool)

    summary = client.insert("target", [[13], [79]], column_names=["value"])
    assert_type(summary, QuerySummary)
    client.close()


def context_manager() -> None:
    with clickhouse_connect.get_client(host="localhost") as client:
        assert_type(client, Client)
        client.command("SELECT 1")


async def async_query() -> None:
    client = await clickhouse_connect.get_async_client(host="localhost")
    assert_type(client, AsyncClient)
    assert_type(await create_async_client(host="localhost"), AsyncClient)
    async with client:
        assert_type(await client.query("SELECT 13"), QueryResult)
