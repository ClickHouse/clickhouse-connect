"""Tests for DSN parsing in create_client and create_async_client."""
from clickhouse_connect.driver import _parse_connection_params


def test_dsn_percent_decode_username():
    """Test that username is percent-decoded from DSN."""
    kwargs = {}
    host, username, password, port, database, interface = _parse_connection_params(
        host=None,
        username=None,
        password="",
        port=0,
        database="__default__",
        interface=None,
        secure=False,
        dsn="http://user%40name:password@localhost:8123/mydb",
        kwargs=kwargs,
    )
    assert username == "user@name"
    assert password == "password"
    assert database == "mydb"


def test_dsn_percent_decode_password():
    """Test that password is percent-decoded from DSN."""
    kwargs = {}
    host, username, password, port, database, interface = _parse_connection_params(
        host=None,
        username=None,
        password="",
        port=0,
        database="__default__",
        interface=None,
        secure=False,
        dsn="http://username:pass%20word@localhost:8123/mydb",
        kwargs=kwargs,
    )
    assert username == "username"
    assert password == "pass word"
    assert database == "mydb"


def test_dsn_percent_decode_database():
    """Test that database is percent-decoded from DSN."""
    kwargs = {}
    host, username, password, port, database, interface = _parse_connection_params(
        host=None,
        username=None,
        password="",
        port=0,
        database="__default__",
        interface=None,
        secure=False,
        dsn="http://username:password@localhost:8123/my%20database",
        kwargs=kwargs,
    )
    assert username == "username"
    assert password == "password"
    assert database == "my database"


def test_dsn_percent_decode_all():
    """Test that username, password, and database are all percent-decoded from DSN."""
    kwargs = {}
    host, username, password, port, database, interface = _parse_connection_params(
        host=None,
        username=None,
        password="",
        port=0,
        database="__default__",
        interface=None,
        secure=False,
        dsn="http://user%40name:pass%20word%21@localhost:8123/my%2Ddatabase",
        kwargs=kwargs,
    )
    assert username == "user@name"
    assert password == "pass word!"
    assert database == "my-database"


def test_dsn_no_encoding():
    """Test that DSN without percent-encoding still works."""
    kwargs = {}
    host, username, password, port, database, interface = _parse_connection_params(
        host=None,
        username=None,
        password="",
        port=0,
        database="__default__",
        interface=None,
        secure=False,
        dsn="http://username:password@localhost:8123/mydb",
        kwargs=kwargs,
    )
    assert username == "username"
    assert password == "password"
    assert database == "mydb"


def test_dsn_override_params():
    """Test that explicit parameters override DSN values."""
    kwargs = {}
    host, username, password, port, database, interface = _parse_connection_params(
        host=None,
        username="override_user",
        password="override_pass",
        port=0,
        database="override_db",
        interface=None,
        secure=False,
        dsn="http://user%40name:pass%20word@localhost:8123/my%20database",
        kwargs=kwargs,
    )
    # Explicit parameters should take precedence
    assert username == "override_user"
    assert password == "override_pass"
    assert database == "override_db"


def test_dsn_empty_password():
    """Test that empty password is handled correctly."""
    kwargs = {}
    host, username, password, port, database, interface = _parse_connection_params(
        host=None,
        username=None,
        password="",
        port=0,
        database="__default__",
        interface=None,
        secure=False,
        dsn="http://username@localhost:8123/mydb",
        kwargs=kwargs,
    )
    assert username == "username"
    # When DSN has no password, None is returned (not empty string)
    assert password is None or password == ""
    assert database == "mydb"
