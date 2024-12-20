from datetime import datetime, timezone, timedelta
from os import environ

import jwt
import pytest

from clickhouse_connect.driver import create_client, ProgrammingError, create_async_client
from tests.integration_tests.conftest import TestConfig


def test_jwt_auth_sync_client(test_config: TestConfig):
    if not test_config.cloud:
        pytest.skip('Skipping JWT test in non-Cloud mode')

    access_token = make_access_token()
    client = create_client(
        host=test_config.host,
        port=test_config.port,
        access_token=access_token
    )
    result = client.query(query=CHECK_CLOUD_MODE_QUERY).result_set
    assert result == [(True,)]


def test_jwt_auth_sync_client_config_errors():
    with pytest.raises(ProgrammingError):
        create_client(
            username='bob',
            access_token='foobar'
        )
    with pytest.raises(ProgrammingError):
        create_client(
            username='bob',
            password='secret',
            access_token='foo'
        )
    with pytest.raises(ProgrammingError):
        create_client(
            password='secret',
            access_token='foo'
        )


@pytest.mark.asyncio
async def test_jwt_auth_async_client(test_config: TestConfig):
    if not test_config.cloud:
        pytest.skip('Skipping JWT test in non-Cloud mode')

    access_token = make_access_token()
    client = await create_async_client(
        host=test_config.host,
        port=test_config.port,
        access_token=access_token
    )
    result = (await client.query(query=CHECK_CLOUD_MODE_QUERY)).result_set
    assert result == [(True,)]


@pytest.mark.asyncio
async def test_jwt_auth_async_client_config_errors():
    with pytest.raises(ProgrammingError):
        await create_async_client(
            username='bob',
            access_token='foobar'
        )
    with pytest.raises(ProgrammingError):
        await create_async_client(
            username='bob',
            password='secret',
            access_token='foo'
        )
    with pytest.raises(ProgrammingError):
        await create_async_client(
            password='secret',
            access_token='foo'
        )


CHECK_CLOUD_MODE_QUERY = "SELECT value='1' FROM system.settings WHERE name='cloud_mode'"
JWT_SECRET_ENV_KEY = 'CLICKHOUSE_CONNECT_TEST_JWT_SECRET'


def make_access_token():
    secret = environ.get(JWT_SECRET_ENV_KEY)
    if not secret:
        raise ValueError(f'{JWT_SECRET_ENV_KEY} environment variable is not set')
    payload = {
        'iss': 'ClickHouse',
        'sub': 'CI_Test',
        'aud': '1f7f78b8-da67-480b-8913-726fdd31d2fc',
        'clickhouse:roles': ['default'],
        'clickhouse:grants': [],
        'exp': datetime.now(tz=timezone.utc) + timedelta(minutes=15)
    }
    return jwt.encode(payload, secret, algorithm='RS256')
