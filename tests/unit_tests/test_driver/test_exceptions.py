import pytest

from clickhouse_connect.driver._backend.httpcommon import build_http_error
from clickhouse_connect.driver.common import coerce_show_clickhouse_errors
from clickhouse_connect.driver.exceptions import (
    DatabaseError,
    Error,
    OperationalError,
    ProgrammingError,
    StreamClosedError,
    error_code_from_header,
    error_name_from_body,
    scrub_error_details,
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


class TestScrubErrorDetails:
    def test_strips_official_build_trailer(self):
        assert scrub_error_details(UNKNOWN_TABLE_BODY) == (
            "Code: 60. DB::Exception: Unknown table expression identifier "
            "'non_existent_table' in scope SELECT * FROM non_existent_table. "
            "(UNKNOWN_TABLE)"
        )

    def test_strips_short_version_trailer(self):
        body = "DB::Exception: limit reached (MEMORY_LIMIT_EXCEEDED) (version 26.2.4.23)"
        assert scrub_error_details(body) == "DB::Exception: limit reached (MEMORY_LIMIT_EXCEEDED)"

    def test_strips_altinity_version_trailer(self):
        body = "Code: 60. DB::Exception: missing. (UNKNOWN_TABLE) (version 22.8.15.25.altinitystable)"
        assert scrub_error_details(body) == "Code: 60. DB::Exception: missing. (UNKNOWN_TABLE)"

    def test_preserves_message_without_version(self):
        body = "Code: 60. DB::Exception: missing. (UNKNOWN_TABLE)"
        assert scrub_error_details(body) == body

    def test_does_not_strip_mid_message_version_text(self):
        body = "bad value (version 1.2.3) in expression (UNKNOWN_TABLE)"
        assert scrub_error_details(body) == body


class TestCoerceShowClickhouseErrors:
    def test_accepts_scrub(self):
        assert coerce_show_clickhouse_errors("scrub") == "scrub"
        assert coerce_show_clickhouse_errors("SCRUB") == "scrub"

    def test_accepts_bool_and_string_bools(self):
        assert coerce_show_clickhouse_errors(True) is True
        assert coerce_show_clickhouse_errors(False) is False
        assert coerce_show_clickhouse_errors("true") is True
        assert coerce_show_clickhouse_errors("false") is False

    def test_none_defaults_to_true(self):
        assert coerce_show_clickhouse_errors(None) is True

    def test_rejects_unknown_string(self):
        with pytest.raises(ProgrammingError, match="Unrecognized show_clickhouse_errors"):
            coerce_show_clickhouse_errors("redact")


class TestBuildHttpError:
    URL = "http://10.15.27.43:8123"

    def test_true_includes_version_and_url(self):
        exc = build_http_error(404, "60", UNKNOWN_TABLE_BODY, True, self.URL, False)
        assert isinstance(exc, DatabaseError)
        assert exc.code == 60
        assert exc.name == "UNKNOWN_TABLE"
        assert "non_existent_table" in str(exc)
        assert "version 26.2.4.23" in str(exc)
        assert self.URL in str(exc)

    def test_scrub_keeps_sql_strips_version_and_url(self):
        exc = build_http_error(404, "60", UNKNOWN_TABLE_BODY, "scrub", self.URL, False)
        msg = str(exc)
        assert exc.code == 60
        assert exc.name == "UNKNOWN_TABLE"
        assert "non_existent_table" in msg
        assert "(UNKNOWN_TABLE)" in msg
        assert "version" not in msg
        assert self.URL not in msg
        assert "(for url" not in msg

    def test_false_is_generic_without_url(self):
        exc = build_http_error(404, "60", UNKNOWN_TABLE_BODY, False, self.URL, False)
        assert str(exc) == "The ClickHouse server returned an error"
        assert exc.code == 60
        assert exc.name is None
        assert self.URL not in str(exc)

    def test_scrub_retried_raises_operational_error(self):
        exc = build_http_error(503, "159", "timeout", "scrub", self.URL, True)
        assert isinstance(exc, OperationalError)
        assert exc.code == 159
        assert self.URL not in str(exc)
