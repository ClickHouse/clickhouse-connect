import uuid

import pytest

from typing import Callable

from clickhouse_connect.driver import create_async_client, create_client
from tests.integration_tests.conftest import TestConfig

SESSION_KEY = 'session_id'


def test_client_default_session_id(test_create_client: Callable):
    # by default, the sync client will autogenerate the session id
    client = test_create_client()
    session_id = client.get_client_setting(SESSION_KEY)
    try:
        uuid.UUID(session_id)
    except ValueError:
        pytest.fail(f"Invalid session_id: {session_id}")
    client.close()


def test_client_autogenerate_session_id(test_create_client: Callable):
    client = test_create_client()
    session_id = client.get_client_setting(SESSION_KEY)
    try:
        uuid.UUID(session_id)
    except ValueError:
        pytest.fail(f"Invalid session_id: {session_id}")


def test_client_custom_session_id(test_create_client: Callable):
    session_id = 'custom_session_id'
    client = test_create_client(session_id=session_id)
    assert client.get_client_setting(SESSION_KEY) == session_id
    client.close()


@pytest.mark.asyncio
async def test_async_client_default_session_id(test_config: TestConfig):
    # by default, the async client will NOT autogenerate the session id
    async_client = await create_async_client(database=test_config.test_database,
                                             host=test_config.host,
                                             port=test_config.port,
                                             user=test_config.username,
                                             password=test_config.password)
    assert async_client.get_client_setting(SESSION_KEY) is None
    async_client.close()


@pytest.mark.asyncio
async def test_async_client_autogenerate_session_id(test_config: TestConfig):
    async_client = await create_async_client(database=test_config.test_database,
                                             host=test_config.host,
                                             port=test_config.port,
                                             user=test_config.username,
                                             password=test_config.password,
                                             autogenerate_session_id=True)
    session_id = async_client.get_client_setting(SESSION_KEY)
    try:
        uuid.UUID(session_id)
    except ValueError:
        pytest.fail(f"Invalid session_id: {session_id}")
    async_client.close()


@pytest.mark.asyncio
async def test_async_client_custom_session_id(test_config: TestConfig):
    session_id = 'custom_session_id'
    async_client = await create_async_client(database=test_config.test_database,
                                             host=test_config.host,
                                             port=test_config.port,
                                             user=test_config.username,
                                             password=test_config.password,
                                             session_id=session_id)
    assert async_client.get_client_setting(SESSION_KEY) == session_id
    async_client.close()
