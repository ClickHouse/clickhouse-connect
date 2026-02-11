import pytest

from clickhouse_connect.driver import ProgrammingError
from tests.integration_tests.conftest import TestConfig

pytest.skip("JWT tests are not yet configured", allow_module_level=True)


def test_jwt_auth_sync_client(test_config: TestConfig):
    if not test_config.cloud:
        pytest.skip("Skipping JWT test in non-Cloud mode")

    access_token = make_access_token()
    client = create_client(host=test_config.host, port=test_config.port, access_token=access_token)
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
        create_client(username="bob", access_token="foobar")
    with pytest.raises(ProgrammingError):
        create_client(username="bob", password="secret", access_token="foo")
    with pytest.raises(ProgrammingError):
        create_client(password="secret", access_token="foo")


def test_jwt_auth_sync_client_set_access_token_errors(test_config: TestConfig):
    if not test_config.cloud:
        pytest.skip("Skipping JWT test in non-Cloud mode")

    access_token = make_access_token()
    client = client_factory(username=None, password="", access_token=access_token)
    result = call(client.query, CHECK_CLOUD_MODE_QUERY).result_set
    assert result == [(True,)]


def test_jwt_auth_client_set_access_token(test_config: TestConfig, client_factory, call):
    """Test setting JWT access token dynamically with both sync and async clients."""
    if not test_config.cloud:
        pytest.skip("Skipping JWT test in non-Cloud mode")

    access_token = make_access_token()
    client = client_factory(username=None, password="", access_token=access_token)

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
