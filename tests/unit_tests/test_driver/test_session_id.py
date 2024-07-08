import uuid

import pytest

from clickhouse_connect.driver import create_async_client, create_client

SESSION_KEY = 'session_id'


def test_client_default_session_id():
    # by default, the sync client will autogenerate the session id
    client = create_client()
    session_id = client.get_client_setting(SESSION_KEY)
    try:
        uuid.UUID(session_id)
    except ValueError:
        pytest.fail(f"Invalid session_id: {session_id}")
    client.close()


def test_client_autogenerate_session_id():
    client = create_client(autogenerate_session_id=True)
    session_id = client.get_client_setting(SESSION_KEY)
    try:
        uuid.UUID(session_id)
    except ValueError:
        pytest.fail(f"Invalid session_id: {session_id}")
    client.close()


def test_client_custom_session_id():
    session_id = 'custom_session_id'
    client = create_client(session_id=session_id)
    assert client.get_client_setting(SESSION_KEY) == session_id
    client.close()


@pytest.mark.asyncio
async def test_async_client_default_session_id():
    # by default, the async client will NOT autogenerate the session id
    async_client = await create_async_client()
    assert async_client.get_client_setting(SESSION_KEY) is None
    async_client.close()


@pytest.mark.asyncio
async def test_async_client_autogenerate_session_id():
    async_client = await create_async_client(autogenerate_session_id=True)
    session_id = async_client.get_client_setting(SESSION_KEY)
    try:
        uuid.UUID(session_id)
    except ValueError:
        pytest.fail(f"Invalid session_id: {session_id}")
    async_client.close()


@pytest.mark.asyncio
async def test_async_client_custom_session_id():
    session_id = 'custom_session_id'
    async_client = await create_async_client(session_id=session_id)
    assert async_client.get_client_setting(SESSION_KEY) == session_id
    async_client.close()
