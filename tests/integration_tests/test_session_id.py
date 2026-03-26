import uuid

from typing import Callable

import pytest

SESSION_KEY = 'session_id'


def test_client_default_session_id(client_factory: Callable):
    # by default, the sync client will autogenerate the session id
    # for async clients, we need to explicitly enable it
    client = client_factory(autogenerate_session_id=True)
    session_id = client.get_client_setting(SESSION_KEY)
    try:
        uuid.UUID(session_id)
    except ValueError:
        pytest.fail(f"Invalid session_id: {session_id}")


def test_client_autogenerate_session_id(client_factory: Callable):
    client = client_factory(autogenerate_session_id=True)
    session_id = client.get_client_setting(SESSION_KEY)
    try:
        uuid.UUID(session_id)
    except ValueError:
        pytest.fail(f"Invalid session_id: {session_id}")


def test_client_custom_session_id(client_factory: Callable):
    session_id = 'custom_session_id'
    client = client_factory(session_id=session_id)
    assert client.get_client_setting(SESSION_KEY) == session_id


def test_explicit_session_id(client_factory: Callable, call):
    """Test explicit session_id allows sharing state like temp tables."""
    session_id = f"test_session_{uuid.uuid4()}"
    client = client_factory(session_id=session_id)

    assert client.get_client_setting("session_id") == session_id

    call(client.command, "CREATE TEMPORARY TABLE temp_test (id UInt32, val String)")
    call(client.command, "INSERT INTO temp_test VALUES (1, 'a'), (2, 'b')")

    result = call(client.query, "SELECT * FROM temp_test ORDER BY id")
    assert result.row_count == 2
    assert result.result_rows[0] == (1, "a")
    assert result.result_rows[1] == (2, "b")
