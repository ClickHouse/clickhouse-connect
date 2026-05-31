#!/usr/bin/env python3 -u

"""
Runnable examples: thread-local, request-local, and async client ownership.
Adjust SYNC_CLIENT_CONFIG / ASYNC_CLIENT_CONFIG for your server.
"""

from __future__ import annotations

import asyncio
import threading

import clickhouse_connect

SYNC_CLIENT_CONFIG = {"host": "localhost", "port": 8123, "user": "default", "password": ""}
ASYNC_CLIENT_CONFIG = {"host": "localhost", "port": 8123, "user": "default", "password": ""}

_thread_status = threading.local()


def get_thread_client():
    client = getattr(_thread_status, "client", None)
    if client is None:
        client = clickhouse_connect.get_client(**SYNC_CLIENT_CONFIG)
        _thread_status.client = client
    return client


def close_thread_client():
    client = getattr(_thread_status, "client", None)
    if client is not None:
        client.close()
        del _thread_status.client


def thread_worker(job_id: int):
    client = get_thread_client()
    try:
        result = client.query("SELECT 1")
        print("thread", job_id, result.result_rows[0][0])
    finally:
        close_thread_client()


def handle_request(request_id: int):
    client = clickhouse_connect.get_client(**SYNC_CLIENT_CONFIG)
    try:
        result = client.query("SELECT 1")
        return result.result_rows[0][0]
    finally:
        client.close()


async def handle_async_request(request_id: int):
    async with await clickhouse_connect.get_async_client(**ASYNC_CLIENT_CONFIG) as client:
        result = await client.query("SELECT 1")
        return result.result_rows[0][0]


async def run_async_batch():
    results = await asyncio.gather(*(handle_async_request(i) for i in range(4)))
    print("async results", results)

if __name__ == "__main__":
    thread_worker(1)
    print("request result:", handle_request(1))
    asyncio.run(run_async_batch())
