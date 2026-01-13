from os import environ

import pytest

from clickhouse_connect.driver import ProgrammingError
from tests.integration_tests.conftest import TestConfig

CHECK_CLOUD_MODE_QUERY = "SELECT value='1' FROM system.settings WHERE name='cloud_mode'"
JWT_SECRET_ENV_KEY = "CLICKHOUSE_CONNECT_TEST_JWT_SECRET"


def make_access_token():
    """Get JWT secret from environment for testing."""
    secret = environ.get(JWT_SECRET_ENV_KEY)
    if not secret:
        raise ValueError(f"{JWT_SECRET_ENV_KEY} environment variable is not set")
    return secret


def test_jwt_auth_client(test_config: TestConfig, client_factory, call):
    """Test JWT authentication with both sync and async clients."""
    if not test_config.cloud:
        pytest.skip("Skipping JWT test in non-Cloud mode")

    access_token = make_access_token()
    client = client_factory(access_token=access_token)
    result = call(client.query, CHECK_CLOUD_MODE_QUERY).result_set
    assert result == [(True,)]


def test_jwt_auth_client_set_access_token(test_config: TestConfig, client_factory, call):
    """Test setting JWT access token dynamically with both sync and async clients."""
    if not test_config.cloud:
        pytest.skip("Skipping JWT test in non-Cloud mode")

    access_token = make_access_token()
    client = client_factory(access_token=access_token)

    access_token = make_access_token()
    client.set_access_token(access_token)

    result = call(client.query, CHECK_CLOUD_MODE_QUERY).result_set
    assert result == [(True,)]


def test_jwt_auth_client_config_errors(client_factory):
    """Test JWT configuration validation catches invalid combinations."""
    with pytest.raises(ProgrammingError):
        client_factory(username="bob", access_token="foobar")

    with pytest.raises(ProgrammingError):
        client_factory(username="bob", password="secret", access_token="foo")

    with pytest.raises(ProgrammingError):
        client_factory(password="secret", access_token="foo")


def test_jwt_auth_client_set_access_token_errors(test_config: TestConfig, client_factory):
    """Test that JWT cannot be set when using username/password authentication."""
    if not test_config.cloud:
        pytest.skip("Skipping JWT test in non-Cloud mode")

    client = client_factory(
        username=test_config.username,
        password=test_config.password,
    )

    access_token = make_access_token()
    with pytest.raises(ProgrammingError):
        client.set_access_token(access_token)
