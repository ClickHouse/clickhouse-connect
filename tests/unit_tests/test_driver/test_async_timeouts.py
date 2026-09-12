import asyncio

import aiohttp
import pytest

from clickhouse_connect.driver.asyncclient import AsyncClient
from clickhouse_connect.driver.exceptions import OperationalError


@pytest.mark.asyncio
@pytest.mark.parametrize("connect_timeout", [0.05, 0, None])
async def test_dns_connection_deadline(mocker, connect_timeout):
    started = asyncio.Event()
    resolving = set()

    async def resolve(*_args, **_kwargs):
        resolving.add(asyncio.current_task())
        started.set()
        await asyncio.Event().wait()

    resolver = aiohttp.resolver.ThreadedResolver()
    resolve_mock = mocker.patch.object(resolver, "resolve", side_effect=resolve)
    client = AsyncClient("http", "timeout.invalid", 8123, connect_timeout=connect_timeout)
    backend = client._backend
    backend.connector_kwargs.update(resolver=resolver, use_dns_cache=False)
    backend.ensure_session()
    connector = backend.session.connector
    task = asyncio.create_task(backend.request(b"SELECT 13", {}))
    try:
        await asyncio.wait_for(started.wait(), 5)
        if connect_timeout:
            with pytest.raises(OperationalError) as exc_info:
                await asyncio.wait_for(task, 5)
            assert isinstance(exc_info.value.__cause__, aiohttp.ServerTimeoutError)
            assert isinstance(exc_info.value.__cause__.__cause__, asyncio.TimeoutError)
            assert resolve_mock.call_count == 2
        else:
            await asyncio.sleep(0.15)
            assert not task.done()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert resolve_mock.call_count == 1
        assert not connector._acquired
        assert backend.session_lease._inflight == 0
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        for resolve_task in resolving:
            resolve_task.cancel()
        await asyncio.gather(*resolving, return_exceptions=True)
        await client.close()
        await resolver.close()


@pytest.mark.asyncio
async def test_proxy_connect_deadline():
    requests = []
    handlers = set()

    async def proxy(reader, writer):
        task = asyncio.current_task()
        handlers.add(task)
        try:
            request = await reader.readuntil(b"\r\n\r\n")
            requests.append(request.split(b"\r\n", 1)[0])
            await reader.read()
        finally:
            writer.close()
            await writer.wait_closed()
            handlers.remove(task)

    server = await asyncio.start_server(proxy, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    client = AsyncClient(
        "https",
        "timeout.invalid",
        443,
        https_proxy=f"http://127.0.0.1:{port}",
        connect_timeout=0.1,
        send_receive_timeout=10,
    )
    client._backend.ensure_session()
    connector = client._session.connector
    try:
        async with server:
            with pytest.raises(OperationalError) as exc_info:
                await asyncio.wait_for(client._backend.request(b"SELECT 13", {}), 5)
            assert isinstance(exc_info.value.__cause__, aiohttp.ServerTimeoutError)
            assert isinstance(exc_info.value.__cause__.__cause__, asyncio.TimeoutError)
            assert requests == [b"CONNECT timeout.invalid:443 HTTP/1.1"] * 2
            assert not connector._acquired
            assert client._backend.session_lease._inflight == 0
    finally:
        await client.close()
        await asyncio.gather(*handlers)
