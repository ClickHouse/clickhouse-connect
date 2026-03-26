#!/usr/bin/env python -u

"""
Demonstrates concurrent async queries using clickhouse-connect.

Executes 10 queries with a concurrency limit of 2. Each query sleeps for 2 seconds,
so the total wall time is ~10 seconds rather than ~20.

Sample output:
    Completed query 1, elapsed: 2002ms
    Completed query 0, elapsed: 2003ms
    Completed query 3, elapsed: 4005ms
    Completed query 2, elapsed: 4005ms
    ...
"""

import asyncio
import time

import clickhouse_connect


async def concurrent_queries():
    async with await clickhouse_connect.get_async_client() as client:
        semaphore = asyncio.Semaphore(2)
        start = time.monotonic()

        async def run_query(num: int):
            async with semaphore:
                await client.query("SELECT sleep(2)")
                elapsed = int((time.monotonic() - start) * 1000)
                print(f"Completed query {num}, elapsed: {elapsed}ms")

        await asyncio.gather(*(run_query(i) for i in range(10)))


asyncio.run(concurrent_queries())
