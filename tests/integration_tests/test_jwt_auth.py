from os import environ

import pytest

from clickhouse_connect.driver import create_client, ProgrammingError, create_async_client
from tests.integration_tests.conftest import TestConfig

pytest.skip('JWT tests are not yet configured', allow_module_level=True)

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


def test_jwt_auth_sync_client_set_access_token(test_config: TestConfig):
    if not test_config.cloud:
        pytest.skip('Skipping JWT test in non-Cloud mode')

    access_token = make_access_token()
    client = create_client(
        host=test_config.host,
        port=test_config.port,
        access_token=access_token,
    )

    # Should still work after the override
    access_token = make_access_token()
    client.set_access_token(access_token)

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


def test_jwt_auth_sync_client_set_access_token_errors(test_config: TestConfig):
    if not test_config.cloud:
        pytest.skip('Skipping JWT test in non-Cloud mode')

    client = create_client(
        host=test_config.host,
        port=test_config.port,
        username=test_config.username,
        password=test_config.password,
    )

    # Can't use JWT with username/password
    access_token = make_access_token()
    with pytest.raises(ProgrammingError):
        client.set_access_token(access_token)


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
async def test_jwt_auth_async_client_set_access_token(test_config: TestConfig):
    if not test_config.cloud:
        pytest.skip('Skipping JWT test in non-Cloud mode')

    access_token = make_access_token()
    client = await create_async_client(
        host=test_config.host,
        port=test_config.port,
        access_token=access_token,
    )

    access_token = make_access_token()
    client.set_access_token(access_token)

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


@pytest.mark.asyncio
async def test_jwt_auth_async_client_set_access_token_errors(test_config: TestConfig):
    if not test_config.cloud:
        pytest.skip('Skipping JWT test in non-Cloud mode')

    client = await create_async_client(
        host=test_config.host,
        port=test_config.port,
        username=test_config.username,
        password=test_config.password,
    )

    # Can't use JWT with username/password
    access_token = make_access_token()
    with pytest.raises(ProgrammingError):
        client.set_access_token(access_token)


CHECK_CLOUD_MODE_QUERY = "SELECT value='1' FROM system.settings WHERE name='cloud_mode'"
JWT_SECRET_ENV_KEY = 'CLICKHOUSE_CONNECT_TEST_JWT_SECRET'


def make_access_token():
    secret = environ.get(JWT_SECRET_ENV_KEY)
    if not secret:
        raise ValueError(f'{JWT_SECRET_ENV_KEY} environment variable is not set')
    return secret
