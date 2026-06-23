from clickhouse_connect.driver import _parse_connection_params


def parse(dsn, *, host=None, username=None, password="", port=None, database=None, interface=None, secure=False):
    return _parse_connection_params(
        host=host,
        username=username,
        password=password,
        port=port,
        database=database,
        interface=interface,
        secure=secure,
        dsn=dsn,
        kwargs={},
    )


def test_dsn_percent_decoded():
    _, username, password, _, database, _ = parse("https://user%40name:pass%20word%21@host:8443/my%2Ddb")
    assert username == "user@name"
    assert password == "pass word!"
    assert database == "my-db"


def test_dsn_unencoded_unchanged():
    _, username, password, _, database, _ = parse("http://username:password@host:8123/mydb")
    assert (username, password, database) == ("username", "password", "mydb")


def test_dsn_no_password():
    # A missing DSN password normalizes to "" so Basic auth encodes "username:" not "username:None".
    _, username, password, _, _, _ = parse("http://username@host:8123/mydb")
    assert username == "username"
    assert password == ""


def test_explicit_params_override_dsn():
    _, username, password, _, database, _ = parse(
        "http://user%40name:pass%20word@host:8123/my%20db", username="u", password="p", database="d"
    )
    assert (username, password, database) == ("u", "p", "d")


def test_no_dsn_or_explicit_values_keeps_database_none_and_resolves_port():
    _, _, _, port, database, interface = parse(None)
    assert port == 8123
    assert database is None
    assert interface == "http"


def test_dsn_without_path_keeps_database_none():
    _, _, _, _, database, _ = parse("http://host:8123")
    assert database is None
