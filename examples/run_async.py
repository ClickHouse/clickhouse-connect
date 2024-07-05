#!/usr/bin/env python -u

"""
This example will execute 10 queries in total, 2 concurrent queries at a time.
Each query will sleep for 2 seconds before returning.
Here's a sample output that shows that the queries are executed concurrently in batches of 2:
```
Completed query 1, elapsed ms since start: 2002
Completed query 0, elapsed ms since start: 2002
Completed query 3, elapsed ms since start: 4004
Completed query 2, elapsed ms since start: 4005
Completed query 4, elapsed ms since start: 6006
Completed query 5, elapsed ms since start: 6007
Completed query 6, elapsed ms since start: 8009
Completed query 7, elapsed ms since start: 8009
Completed query 9, elapsed ms since start: 10011
Completed query 8, elapsed ms since start: 10011
```
"""

import asyncio
from datetime import datetime

import clickhouse_connect

QUERIES = 10
SEMAPHORE = 2

clickhouse_connect.common.set_setting("autogenerate_session_id", False)


async def concurrent_queries():
    test_query = "SELECT sleep(2)"
    client = await clickhouse_connect.get_async_client()

    start = datetime.now()

    async def semaphore_wrapper(sm: asyncio.Semaphore, num: int):
        async with sm:
            await client.query(query=test_query)
            print(f"Completed query {num}, "
                  f"elapsed ms since start: {int((datetime.now() - start).total_seconds() * 1000)}")

    semaphore = asyncio.Semaphore(SEMAPHORE)
    await asyncio.gather(*[semaphore_wrapper(semaphore, num) for num in range(QUERIES)])


async def main():
    await concurrent_queries()


asyncio.run(main())
