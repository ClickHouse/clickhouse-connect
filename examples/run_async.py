#!/usr/bin/env python -u

import asyncio
from datetime import datetime

import clickhouse_connect

QUERIES = 100
SEMAPHORE = 10

clickhouse_connect.common.set_setting("autogenerate_session_id", False)


class AsyncRunner:
    def __init__(self, **client_params):
        self.client = clickhouse_connect.create_client(**client_params)
        self.query_count = 0
        self.row_count = 0

    async def run_query(self, query: str, num: int):
        self.query_count += 1

        def read_data():
            return self.client.query(query).result_set

        result_set = await asyncio.to_thread(read_data)
        self.row_count += len(result_set)
        print(f"completed query {num}")
        return result_set


async def concurrent_queries():
    test_query = ("SELECT * FROM generateRandom('a Array(Int8), d Decimal32(4)," +
                  " c Tuple(DateTime64(3), UUID)', 1, 10, 2) LIMIT 200000")
    runner = AsyncRunner()

    print(datetime.now())

    async def semaphore_wrapper(sm: asyncio.Semaphore, num: int):
        async with sm:
            await runner.run_query(test_query, num)

    semaphore = asyncio.Semaphore(SEMAPHORE)
    await asyncio.gather(*[semaphore_wrapper(semaphore, num) for num in range(QUERIES)])
    print(f"{datetime.now()}  query_count: {runner.query_count}  row_count: {runner.row_count}")


loop = asyncio.get_event_loop()
try:
    loop.run_until_complete(concurrent_queries())
finally:
    loop.close()
