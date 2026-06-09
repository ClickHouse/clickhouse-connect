from clickhouse_connect.driver.exceptions import (
    DatabaseError,
    Error,
    OperationalError,
    StreamClosedError,
    error_code_from_header,
    error_name_from_body,
)

UNKNOWN_TABLE_BODY = (
    "Code: 60. DB::Exception: Unknown table expression identifier "
    "'non_existent_table' in scope SELECT * FROM non_existent_table. "
    "(UNKNOWN_TABLE) (version 26.2.4.23 (official build))"
)


class TestErrorCodeFromHeader:
    def test_parses_numeric_code(self):
        assert error_code_from_header("60") == 60

    def test_none_header(self):
        assert error_code_from_header(None) is None

    def test_empty_header(self):
        assert error_code_from_header("") is None

    def test_non_numeric_header(self):
        assert error_code_from_header("not-a-number") is None


class TestErrorNameFromBody:
    def test_extracts_symbolic_name(self):
        assert error_name_from_body(UNKNOWN_TABLE_BODY) == "UNKNOWN_TABLE"

    def test_picks_error_name_over_version_token(self):
        body = "DB::Exception: limit reached (MEMORY_LIMIT_EXCEEDED) (version 26.2.4.23)"
        assert error_name_from_body(body) == "MEMORY_LIMIT_EXCEEDED"

    def test_ignores_camelcase_type_tokens(self):
        assert error_name_from_body("bad cast from (UInt64) value") is None

    def test_empty_body(self):
        assert error_name_from_body("") is None

    def test_none_body(self):
        assert error_name_from_body(None) is None


class TestErrorFields:
    def test_carries_code_and_name(self):
        exc = DatabaseError("boom", code=60, name="UNKNOWN_TABLE")
        assert str(exc) == "boom"
        assert exc.code == 60
        assert exc.name == "UNKNOWN_TABLE"
        assert isinstance(exc, Error)

    def test_defaults_to_none(self):
        exc = OperationalError("network down")
        assert str(exc) == "network down"
        assert exc.code is None
        assert exc.name is None

    def test_subclass_with_custom_init(self):
        exc = StreamClosedError()
        assert exc.code is None
        assert exc.name is None
