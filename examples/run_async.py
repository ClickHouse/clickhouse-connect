#!/usr/bin/env python -u

import asyncio
from datetime import datetime

import clickhouse_connect
from clickhouse_connect.driver import create_async_client

QUERIES = 10
SEMAPHORE = 2

clickhouse_connect.common.set_setting("autogenerate_session_id", False)


async def concurrent_queries():
    test_query = "SELECT sleep(2)"
    client = await create_async_client()

    start = datetime.now()

    async def semaphore_wrapper(sm: asyncio.Semaphore, num: int):
        async with sm:
            await client.query(query=test_query)
            print(f"Completed query {num}, "
                  f"elapsed ms since start: {int((datetime.now() - start).total_seconds() * 1000)}")

    semaphore = asyncio.Semaphore(SEMAPHORE)
    await asyncio.gather(*[semaphore_wrapper(semaphore, num) for num in range(QUERIES)])


loop = asyncio.get_event_loop()
try:
    loop.run_until_complete(concurrent_queries())
finally:
    loop.close()
