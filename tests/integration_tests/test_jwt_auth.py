from os import environ

import pytest

from clickhouse_connect.driver import ProgrammingError, create_async_client, create_client
from tests.integration_tests.conftest import TestConfig

pytest.skip("JWT tests are not yet configured", allow_module_level=True)


def test_jwt_auth_sync_client(test_config: TestConfig):
    if not test_config.cloud:
        pytest.skip("Skipping JWT test in non-Cloud mode")

    access_token = make_access_token()
    client = create_client(
        host=test_config.host,
        port=test_config.port,
        access_token=access_token,
    )
    result = client.query(query=CHECK_CLOUD_MODE_QUERY).result_set
    assert result == [(True,)]


def test_jwt_auth_sync_client_set_access_token(test_config: TestConfig):
    if not test_config.cloud:
        pytest.skip("Skipping JWT test in non-Cloud mode")

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
            username="bob",
            access_token="foobar",
        )
    with pytest.raises(ProgrammingError):
        create_client(
            username="bob",
            password="secret",
            access_token="foo",
        )
    with pytest.raises(ProgrammingError):
        create_client(
            password="secret",
            access_token="foo",
        )


def test_jwt_auth_sync_client_set_access_token_errors(test_config: TestConfig):
    if not test_config.cloud:
        pytest.skip("Skipping JWT test in non-Cloud mode")

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
        pytest.skip("Skipping JWT test in non-Cloud mode")

    access_token = make_access_token()
    client = await create_async_client(
        host=test_config.host,
        port=test_config.port,
        access_token=access_token,
    )
    result = (await client.query(query=CHECK_CLOUD_MODE_QUERY)).result_set
    assert result == [(True,)]


@pytest.mark.asyncio
async def test_jwt_auth_async_client_set_access_token(test_config: TestConfig):
    if not test_config.cloud:
        pytest.skip("Skipping JWT test in non-Cloud mode")

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
            username="bob",
            access_token="foobar",
        )
    with pytest.raises(ProgrammingError):
        await create_async_client(
            username="bob",
            password="secret",
            access_token="foo",
        )
    with pytest.raises(ProgrammingError):
        await create_async_client(password="secret", access_token="foo")


@pytest.mark.asyncio
async def test_jwt_auth_async_client_set_access_token_errors(test_config: TestConfig):
    if not test_config.cloud:
        pytest.skip("Skipping JWT test in non-Cloud mode")

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


def test_token_provider_sync_client(test_config: TestConfig):
    if not test_config.cloud:
        pytest.skip("Skipping JWT test in non-Cloud mode")

    client = create_client(
        host=test_config.host,
        port=test_config.port,
        token_provider=make_access_token,
    )
    result = client.query(query=CHECK_CLOUD_MODE_QUERY).result_set
    assert result == [(True,)]


@pytest.mark.asyncio
async def test_token_provider_async_client(test_config: TestConfig):
    if not test_config.cloud:
        pytest.skip("Skipping JWT test in non-Cloud mode")

    client = await create_async_client(
        host=test_config.host,
        port=test_config.port,
        token_provider=make_access_token,
    )
    result = (await client.query(query=CHECK_CLOUD_MODE_QUERY)).result_set
    assert result == [(True,)]


@pytest.mark.asyncio
async def test_token_provider_async_client_async_callable(test_config: TestConfig):
    # The async client also accepts an async (awaitable) provider.
    if not test_config.cloud:
        pytest.skip("Skipping JWT test in non-Cloud mode")

    provider = make_async_token_provider(make_access_token())
    client = await create_async_client(
        host=test_config.host,
        port=test_config.port,
        token_provider=provider,
    )
    assert provider.calls == 1  # async provider awaited once for the initial token
    result = (await client.query(query=CHECK_CLOUD_MODE_QUERY)).result_set
    assert result == [(True,)]


def test_token_provider_sync_client_refresh(test_config: TestConfig):
    if not test_config.cloud:
        pytest.skip("Skipping JWT test in non-Cloud mode")

    # The provider hands out a valid token for the initial connection and a
    # second valid token for the refresh.
    provider = make_token_provider(make_access_token(), make_access_token())
    client = create_client(
        host=test_config.host,
        port=test_config.port,
        token_provider=provider,
    )
    assert provider.calls == 1  # initial token only

    # A real token can't be made to expire on demand, so simulate the server
    # rejecting the current token by overwriting the auth header with a bad value.
    client.set_access_token("invalid.jwt.token")
    result = client.query(query=CHECK_CLOUD_MODE_QUERY).result_set
    assert result == [(True,)]
    assert provider.calls == 2  # provider re-invoked exactly once to refresh


@pytest.mark.asyncio
async def test_token_provider_async_client_refresh(test_config: TestConfig):
    if not test_config.cloud:
        pytest.skip("Skipping JWT test in non-Cloud mode")

    provider = make_token_provider(make_access_token(), make_access_token())
    client = await create_async_client(
        host=test_config.host,
        port=test_config.port,
        token_provider=provider,
    )
    assert provider.calls == 1  # initial token only

    # See the sync variant above for why the header is poisoned manually.
    client.set_access_token("invalid.jwt.token")
    result = (await client.query(query=CHECK_CLOUD_MODE_QUERY)).result_set
    assert result == [(True,)]
    assert provider.calls == 2  # provider re-invoked exactly once to refresh


def test_token_provider_sync_client_config_errors():
    provider = make_token_provider("foobar")
    with pytest.raises(ProgrammingError):
        create_client(username="bob", token_provider=provider)
    with pytest.raises(ProgrammingError):
        create_client(username="bob", password="secret", token_provider=provider)
    with pytest.raises(ProgrammingError):
        create_client(password="secret", token_provider=provider)
    with pytest.raises(ProgrammingError):
        create_client(access_token="foo", token_provider=provider)
    assert provider.calls == 0  # validation runs before the provider is ever called


@pytest.mark.asyncio
async def test_token_provider_async_client_config_errors():
    provider = make_token_provider("foobar")
    with pytest.raises(ProgrammingError):
        await create_async_client(username="bob", token_provider=provider)
    with pytest.raises(ProgrammingError):
        await create_async_client(username="bob", password="secret", token_provider=provider)
    with pytest.raises(ProgrammingError):
        await create_async_client(password="secret", token_provider=provider)
    with pytest.raises(ProgrammingError):
        await create_async_client(access_token="foo", token_provider=provider)
    assert provider.calls == 0  # validation runs before the provider is ever called


CHECK_CLOUD_MODE_QUERY = "SELECT value='1' FROM system.settings WHERE name='cloud_mode'"
JWT_SECRET_ENV_KEY = "CLICKHOUSE_CONNECT_TEST_JWT_SECRET"


def make_access_token():
    secret = environ.get(JWT_SECRET_ENV_KEY)
    if not secret:
        raise ValueError(f"{JWT_SECRET_ENV_KEY} environment variable is not set")
    return secret


class _SequenceTokenProvider:
    """A token_provider that returns predetermined tokens in sequence.

    Each invocation returns the next token; once the sequence is exhausted the
    last token is repeated. The ``calls`` attribute records how many times the
    provider was invoked, so tests can assert that a refresh actually happened.
    """

    def __init__(self, tokens):
        if not tokens:
            raise ValueError("at least one token is required")
        self._tokens = list(tokens)
        self.calls = 0

    def _next(self):
        token = self._tokens[min(self.calls, len(self._tokens) - 1)]
        self.calls += 1
        return token

    def __call__(self):
        return self._next()


class _AsyncSequenceTokenProvider(_SequenceTokenProvider):
    """Awaitable counterpart of _SequenceTokenProvider for the async client."""

    async def __call__(self):
        return self._next()


def make_token_provider(*tokens):
    """Build a sync token_provider yielding the given tokens in order.

    Mirrors make_access_token, but lets a test hand out a predetermined
    sequence (e.g. an initial token followed by a refreshed one).
    """
    return _SequenceTokenProvider(tokens)


def make_async_token_provider(*tokens):
    """Async (awaitable) counterpart of make_token_provider."""
    return _AsyncSequenceTokenProvider(tokens)
