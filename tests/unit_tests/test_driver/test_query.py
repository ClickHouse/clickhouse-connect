import zoneinfo
from datetime import timedelta, timezone

import pyarrow as pa
import pytest

from clickhouse_connect.driver import query as query_module
from clickhouse_connect.driver import tzutil
from clickhouse_connect.driver.client import _strip_utc_timezone_from_arrow
from clickhouse_connect.driver.exceptions import ProgrammingError
from clickhouse_connect.driver.query import QueryContext


def test_copy_context():
    settings = {"max_bytes_for_external_group_by": 1024 * 1024 * 100, "read_overflow_mode": "throw"}
    parameters = {"user_id": "user_1"}
    query_formats = {"IPv*": "string"}
    context = QueryContext(
        "SELECT source_ip FROM table WHERE user_id = %(user_id)s",
        settings=settings,
        parameters=parameters,
        query_formats=query_formats,
        use_none=True,
    )
    assert context.use_none is True
    assert context.final_query == "SELECT source_ip FROM table WHERE user_id = 'user_1'"
    assert context.query_formats["IPv*"] == "string"
    assert context.settings["max_bytes_for_external_group_by"] == 104857600

    context_copy = context.updated_copy(
        settings={"max_bytes_for_external_group_by": 1024 * 1024 * 24, "max_execution_time": 120}, parameters={"user_id": "user_2"}
    )
    assert context_copy.settings["read_overflow_mode"] == "throw"
    assert context_copy.settings["max_execution_time"] == 120
    assert context_copy.settings["max_bytes_for_external_group_by"] == 25165824
    assert context_copy.final_query == "SELECT source_ip FROM table WHERE user_id = 'user_2'"


@pytest.mark.parametrize(
    "query, expected_calls",
    [
        ("SELECT 13", 0),
        ("SELECT 13;", 0),
        ("SELECT 'value_1;';", 1),
        ("SELECT 13; -- trailing", 1),
        ("SELECT 13; /* separator */ ;", 1),
        ("SELECT 13; /* inner; */ ;", 1),
        ("SELECT ' INSERT INTO '; -- trailing", 1),
        ("INSERT INTO tbl FORMAT TabSeparated\nvalue_1;\n", 0),
        ("CREATE TABLE tbl (value UInt8) ENGINE Memory;\n", 1),
        ("CREATE TABLE tbl (value UInt8) ENGINE Memory;", 0),
        ("SHOW POLICIES; -- trailing", 1),
    ],
)
def test_query_context_trailing_semicolon_lexer_routing(monkeypatch, query, expected_calls):
    strip = query_module._strip_trailing_semicolons
    calls = []

    def track_strip(sql):
        calls.append(sql)
        return strip(sql)

    monkeypatch.setattr(query_module, "_strip_trailing_semicolons", track_strip)
    QueryContext(query)
    assert len(calls) == expected_calls


@pytest.mark.parametrize(
    "query, expected_calls",
    [("SELECT 13", 1), ("SELECT 13;", 1), ("SELECT 13; -- trailing", 1)],
)
def test_query_context_avoids_common_duplicate_comment_scans(monkeypatch, query, expected_calls):
    remove_comments = query_module.remove_sql_comments
    calls = []

    def track_remove_comments(sql):
        calls.append(sql)
        return remove_comments(sql)

    monkeypatch.setattr(query_module, "remove_sql_comments", track_remove_comments)
    QueryContext(query)
    assert len(calls) == expected_calls


def test_query_context_does_not_route_quoted_insert_literal_as_insert():
    context = QueryContext("SELECT ' INSERT INTO '; -- trailing")
    assert context.is_insert is False
    assert context.final_query == "SELECT ' INSERT INTO ' -- trailing"


def test_query_context_strips_command_terminators_for_streamed_entry_points():
    context = QueryContext("SHOW POLICIES; -- trailing")
    assert context.is_command is True
    assert context.final_query == "SHOW POLICIES -- trailing"

    context = QueryContext("CHECK TABLE tbl; -- trailing")
    assert context.is_command is True
    assert context.final_query == "CHECK TABLE tbl -- trailing"


@pytest.mark.parametrize(
    "query, parameters, expected_insert, expected_query",
    [
        (
            "WITH {SELECT:Int32} AS value INSERT INTO tbl FORMAT TabSeparated\nvalue_1;\n",
            {"SELECT": 13},
            True,
            "WITH {SELECT:Int32} AS value INSERT INTO tbl FORMAT TabSeparated\nvalue_1;\n",
        ),
        (
            "WITH {INSERT:Int32} AS value SELECT value; -- trailing",
            {"INSERT": 13},
            False,
            "WITH {INSERT:Int32} AS value SELECT value -- trailing",
        ),
    ],
)
def test_query_context_ignores_server_placeholder_names_when_routing(query, parameters, expected_insert, expected_query):
    context = QueryContext(query, parameters=parameters)
    assert context.is_insert is expected_insert
    assert context.final_query == expected_query


def test_query_context_reuses_server_bind_template_classification(monkeypatch):
    remove_comments = query_module.remove_sql_comments
    calls = []

    def track_remove_comments(sql):
        calls.append(sql)
        return remove_comments(sql)

    monkeypatch.setattr(query_module, "remove_sql_comments", track_remove_comments)
    QueryContext("SELECT {value:UInt8}; -- trailing", parameters={"value": 13})
    assert len(calls) == 1


def test_query_context_classifies_bound_insert_before_lexing(monkeypatch):
    class SqlKeyword:
        def __str__(self):
            return "INSERT"

    strip = query_module._strip_trailing_semicolons
    calls = []

    def track_strip(sql):
        calls.append(sql)
        return strip(sql)

    monkeypatch.setattr(query_module, "_strip_trailing_semicolons", track_strip)
    query = "%(verb)s INTO tbl FORMAT TabSeparated\nvalue_1;\n"
    context = QueryContext(query, parameters={"verb": SqlKeyword()})

    assert context.is_insert is True
    assert context.final_query == "INSERT INTO tbl FORMAT TabSeparated\nvalue_1;\n"
    assert calls == []


def test_query_context_preserves_bind_completed_insert_payload(monkeypatch):
    class Raw:
        def __str__(self):
            return ""

    strip = query_module._strip_trailing_semicolons
    calls = []

    def track_strip(sql):
        calls.append(sql)
        return strip(sql)

    monkeypatch.setattr(query_module, "_strip_trailing_semicolons", track_strip)
    query = "INSERT %sINTO tbl FORMAT TabSeparated\nvalue_1;\n"
    context = QueryContext(query, parameters=[Raw()])

    assert context.is_insert is True
    assert context.final_query == "INSERT INTO tbl FORMAT TabSeparated\nvalue_1;\n"
    assert calls == []


def test_query_context_preserves_with_prefixed_bound_insert_payload(monkeypatch):
    class SqlKeyword:
        calls = 0

        def __str__(self):
            self.calls += 1
            return "INSERT"

    strip = query_module._strip_trailing_semicolons
    calls = []

    def track_strip(sql):
        calls.append(sql)
        return strip(sql)

    monkeypatch.setattr(query_module, "_strip_trailing_semicolons", track_strip)
    keyword = SqlKeyword()
    query = "WITH 1 AS y %(verb)s INTO tbl FORMAT TabSeparated\nvalue_1;\n"
    context = QueryContext(query, parameters={"verb": keyword})

    assert context.is_insert is True
    assert context.final_query == "WITH 1 AS y INSERT INTO tbl FORMAT TabSeparated\nvalue_1;\n"
    assert keyword.calls == 1
    assert calls == []


@pytest.mark.parametrize(
    "statement, expected_query, expected_lexer_calls",
    [
        ("SELECT 13;", "SELECT 13", 0),
        ("SELECT 13; -- trailing", "SELECT 13 -- trailing", 1),
    ],
)
def test_query_context_normalizes_parameter_generated_terminator_once(monkeypatch, statement, expected_query, expected_lexer_calls):
    class StatefulStatement:
        calls = 0

        def __str__(self):
            self.calls += 1
            return statement

    strip = query_module._strip_trailing_semicolons
    lexer_calls = []

    def track_strip(sql):
        lexer_calls.append(sql)
        return strip(sql)

    monkeypatch.setattr(query_module, "_strip_trailing_semicolons", track_strip)
    value = StatefulStatement()
    context = QueryContext("%(statement)s", parameters={"statement": value})

    assert context.final_query == expected_query
    assert value.calls == 1
    assert len(lexer_calls) == expected_lexer_calls


def test_query_context_binds_parameter_generated_statement_once():
    class StatefulStatement:
        calls = 0

        def __str__(self):
            self.calls += 1
            return "SELECT 13"

    statement = StatefulStatement()
    context = QueryContext("%(statement)s; -- trailing", parameters={"statement": statement})

    assert context.final_query == "SELECT 13 -- trailing"
    assert statement.calls == 1


def test_query_context_binary_bind_lexes_only_the_template(monkeypatch):
    strip = query_module._strip_trailing_semicolons
    calls = []

    def track_strip(sql):
        calls.append(sql)
        return strip(sql)

    monkeypatch.setattr(query_module, "_strip_trailing_semicolons", track_strip)
    query = "SELECT $value$; -- trailing"
    context = QueryContext(query, parameters={"$value$": b"13"})

    assert context.final_query == b"SELECT $value$13$value$ -- trailing"
    assert calls == [query]


def test_query_context_mixed_binary_and_client_bind_skips_unsafe_normalization(monkeypatch):
    class StatefulStatement:
        calls = 0

        def __str__(self):
            self.calls += 1
            return "SELECT 13; -- trailing\n"

    value = StatefulStatement()
    calls = []

    def track_strip(sql):
        calls.append(sql)
        return sql

    monkeypatch.setattr(query_module, "_strip_trailing_semicolons", track_strip)
    context = QueryContext(
        "%(statement)s $value$",
        parameters={"statement": value, "$value$": b"payload"},
    )

    assert context.final_query == b"SELECT 13; -- trailing\n $value$payload$value$"
    assert value.calls == 1
    assert calls == []


def test_query_context_mixed_binary_select_normalizes_template(monkeypatch):
    strip = query_module._strip_trailing_semicolons
    calls = []

    def track_strip(sql):
        calls.append(sql)
        return strip(sql)

    monkeypatch.setattr(query_module, "_strip_trailing_semicolons", track_strip)
    query = "SELECT %(value)s + toUInt8($raw$); -- trailing"
    context = QueryContext(query, parameters={"value": 13, "$raw$": b"79"})

    assert context.final_query == b"SELECT 13 + toUInt8($raw$79$raw$) -- trailing"
    assert calls == [query]


def test_query_context_binary_bind_with_unused_parameter_normalizes_template(monkeypatch):
    strip = query_module._strip_trailing_semicolons
    calls = []

    def track_strip(sql):
        calls.append(sql)
        return strip(sql)

    monkeypatch.setattr(query_module, "_strip_trailing_semicolons", track_strip)
    query = "SELECT toUInt8($raw$); -- trailing"
    context = QueryContext(query, parameters={"$raw$": b"13", "unused": 79})

    assert context.final_query == b"SELECT toUInt8($raw$13$raw$) -- trailing"
    assert calls == [query]


@pytest.mark.parametrize(
    "query",
    [
        "SHOW ROW POLICIES",
        "SHOW POLICIES",
        "show row policies",
        "  SHOW   ROW   POLICIES  ",
        "-- comment\nSHOW ROW POLICIES",
    ],
)
def test_bare_row_policy_show_is_command(query):
    assert QueryContext(query).is_command is True


@pytest.mark.parametrize(
    "query",
    [
        "SHOW ROW POLICIES ON db.table",
        "SHOW ROW POLICIES policy_1",
        "SHOW DATABASES",
        "SHOW TABLES",
        "SHOW USERS",
        "SELECT 1",
        "SHOW",
    ],
)
def test_other_show_queries_are_not_commands(query):
    assert QueryContext(query).is_command is False


@pytest.mark.parametrize(
    "query",
    [
        "--sql\nCREATE OR REPLACE TABLE x (a String) ENGINE=MergeTree ORDER BY tuple()",
        "--anything\nDROP TABLE x",
        "--sql\n  ALTER TABLE x ADD COLUMN y UInt32",
        "/*sql*/CREATE TABLE x (a String) ENGINE=Memory",
    ],
)
def test_leading_no_space_comment_is_command(query):
    assert QueryContext(query).is_command is True


def test_active_tz_utc_defaults_to_naive():
    ctx = QueryContext(query_tz=zoneinfo.ZoneInfo("UTC"))
    assert ctx.tz_mode == "naive_utc"
    assert ctx.active_tz(None) is None


def test_active_tz_utc_opt_in_timezone():
    ctx = QueryContext(query_tz=zoneinfo.ZoneInfo("UTC"), tz_mode="aware")
    assert ctx.tz_mode == "aware"
    assert ctx.active_tz(None) == zoneinfo.ZoneInfo("UTC")


def test_active_tz_etc_utc_defaults_to_naive():
    """Test that Etc/UTC is treated as UTC for naive datetime conversion.

    This test reproduces the bug where zoneinfo.ZoneInfo('Etc/UTC') != zoneinfo.ZoneInfo("UTC"),
    causing timezone-aware datetimes to be returned when they should be naive.
    """
    etc_utc = zoneinfo.ZoneInfo("Etc/UTC")
    ctx = QueryContext(query_tz=etc_utc)
    assert ctx.tz_mode == "naive_utc"
    # BUG: Previously returned etc_utc instead of None
    assert ctx.active_tz(None) is None  # Should return None for naive datetime


def test_active_tz_etc_utc_opt_in_timezone():
    """Test that Etc/UTC with tz_mode='aware' returns timezone."""
    etc_utc = zoneinfo.ZoneInfo("Etc/UTC")
    ctx = QueryContext(query_tz=etc_utc, tz_mode="aware")
    assert ctx.tz_mode == "aware"
    assert ctx.active_tz(None) is not None  # Should return the timezone


def test_is_utc_timezone_zoneinfo_utc():
    assert tzutil.is_utc_timezone(zoneinfo.ZoneInfo("UTC")) is True


def test_is_utc_timezone_etc_utc():
    assert tzutil.is_utc_timezone(zoneinfo.ZoneInfo("Etc/UTC")) is True


def test_is_utc_timezone_utc_string():
    assert tzutil.is_utc_timezone(zoneinfo.ZoneInfo("UTC")) is True


def test_is_utc_timezone_gmt():
    assert tzutil.is_utc_timezone(zoneinfo.ZoneInfo("GMT")) is True


def test_is_utc_timezone_non_utc():
    assert tzutil.is_utc_timezone(zoneinfo.ZoneInfo("America/Denver")) is False
    assert tzutil.is_utc_timezone(zoneinfo.ZoneInfo("Europe/London")) is False


def test_is_utc_timezone_none():
    assert tzutil.is_utc_timezone(None) is False


def test_is_utc_timezone_string():
    """Test is_utc_timezone with string timezone names (used by Arrow field.type.tz)."""
    assert tzutil.is_utc_timezone("UTC") is True
    assert tzutil.is_utc_timezone("Etc/UTC") is True
    assert tzutil.is_utc_timezone("GMT") is True
    assert tzutil.is_utc_timezone("America/Denver") is False
    assert tzutil.is_utc_timezone("Europe/London") is False


def test_strip_utc_timezone_from_arrow_strips_utc():
    ts = pa.array([1705312200000000], type=pa.timestamp("us", "UTC"))
    table = pa.Table.from_arrays([ts], names=["ts"])
    result = _strip_utc_timezone_from_arrow(table)
    assert result.schema[0].type == pa.timestamp("us")


def test_strip_utc_timezone_from_arrow_strips_etc_utc():
    ts = pa.array([1705312200000000], type=pa.timestamp("us", "Etc/UTC"))
    table = pa.Table.from_arrays([ts], names=["ts"])
    result = _strip_utc_timezone_from_arrow(table)
    assert result.schema[0].type == pa.timestamp("us")


def test_strip_utc_timezone_from_arrow_preserves_non_utc():
    ts = pa.array([1705312200000000], type=pa.timestamp("us", "America/Denver"))
    table = pa.Table.from_arrays([ts], names=["ts"])
    result = _strip_utc_timezone_from_arrow(table)
    assert result.schema[0].type == pa.timestamp("us", "America/Denver")


def test_strip_utc_timezone_from_arrow_preserves_naive():
    ts = pa.array([1705312200000000], type=pa.timestamp("us"))
    table = pa.Table.from_arrays([ts], names=["ts"])
    result = _strip_utc_timezone_from_arrow(table)
    assert result.schema[0].type == pa.timestamp("us")


def test_utc_equivalent_timezones_normalize_to_naive():
    """Test that UTC-equivalent timezones (Etc/UCT, GMT, etc.) return naive datetimes by default"""
    utc_equivalents = ["Etc/UCT", "GMT", "Etc/GMT", "Etc/Universal", "Etc/Zulu", "UCT", "Universal"]

    for tz_name in utc_equivalents:
        tz = zoneinfo.ZoneInfo(tz_name)
        ctx = QueryContext(tz_mode="naive_utc")
        result = ctx.active_tz(datatype_tz=tz)
        assert result is None


def test_utc_equivalent_timezones_with_tz_mode_aware():
    """Test that UTC-equivalent timezones return timezone-aware when tz_mode='aware'"""
    utc_equivalents = ["Etc/UCT", "GMT", "Etc/GMT"]

    for tz_name in utc_equivalents:
        tz = zoneinfo.ZoneInfo(tz_name)
        ctx = QueryContext(tz_mode="aware")
        result = ctx.active_tz(datatype_tz=tz)
        assert result == tz


def test_tzutil_normalize_utc_equivalents():
    """Test that tzutil.normalize_timezone properly normalizes UTC-equivalent timezones"""
    utc_equivalents = [
        "UTC",
        "Etc/UTC",
        "Etc/UCT",
        "Etc/Universal",
        "GMT",
        "Etc/GMT",
        "Etc/GMT+0",
        "Etc/GMT-0",
        "Etc/GMT0",
        "GMT+0",
        "GMT-0",
        "GMT0",
        "Greenwich",
        "UCT",
        "Universal",
        "Zulu",
    ]

    for tz_name in utc_equivalents:
        tz = zoneinfo.ZoneInfo(tz_name)
        normalized, is_valid = tzutil.normalize_timezone(tz)
        assert tzutil.is_utc_timezone(normalized)
        assert is_valid is True


def test_resolve_zone_fixed_offset_positive():
    """ClickHouse Fixed/UTC+HH:MM:SS strings resolve to a stdlib fixed-offset timezone."""
    tz = tzutil.resolve_zone("Fixed/UTC+05:30:00")
    assert tz == timezone(timedelta(hours=5, minutes=30))


def test_resolve_zone_fixed_offset_negative():
    tz = tzutil.resolve_zone("Fixed/UTC-03:00:00")
    assert tz == timezone(timedelta(hours=-3))


def test_resolve_zone_fixed_offset_with_seconds():
    tz = tzutil.resolve_zone("Fixed/UTC+05:30:30")
    assert tz == timezone(timedelta(hours=5, minutes=30, seconds=30))


def test_resolve_zone_fixed_offset_zero_collapses_to_utc():
    """A Fixed offset of zero is semantically UTC and should normalize to timezone.utc."""
    tz = tzutil.resolve_zone("Fixed/UTC+00:00:00")
    assert tz is timezone.utc


def test_resolve_zone_fixed_offset_invalid_falls_through():
    """Malformed or unrepresentable Fixed strings raise ZoneInfoNotFoundError."""
    invalid = [
        "Fixed/UTC+25:00:00",  # hour > 24
        "Fixed/UTC+24:00:00",  # server accepts, but Python tzinfo cannot represent +/-24h
        "Fixed/UTC-24:00:00",
        "Fixed/UTC+24:00:01",  # boundary + nonzero
        "Fixed/UTC+24:01:00",
        "Fixed/UTC+05:60:00",  # minute > 59
        "Fixed/UTC+05:00:60",  # second > 59
        "Fixed/UTC+05:30",  # missing seconds component
        "Fixed/UTC+5:30:00",  # single-digit hour, server rejects
    ]
    for s in invalid:
        with pytest.raises(zoneinfo.ZoneInfoNotFoundError):
            tzutil.resolve_zone(s)


def test_tzutil_normalize_fixed_offset_trusted_for_server():
    """Server-init paths pass trust_fixed_offset=True; the offset is taken at face value."""
    tz = timezone(timedelta(hours=5, minutes=30))
    normalized, is_valid = tzutil.normalize_timezone(tz, trust_fixed_offset=True)
    assert normalized is tz
    assert is_valid is True


def test_tzutil_normalize_fixed_offset_default_falls_through():
    """Without trust_fixed_offset, a stdlib fixed offset is not auto-trusted."""
    tz = timezone(timedelta(hours=-7))  # PDT-shaped fixed offset
    normalized, is_valid = tzutil.normalize_timezone(tz)
    # Either tzlocal upgraded it to an IANA zone (then is_valid=True and normalized is
    # a ZoneInfo, not the original fixed offset) or it fell through to (tz, False).
    if is_valid:
        assert not isinstance(normalized, timezone) or normalized is timezone.utc
    else:
        assert normalized is tz


def test_extract_tz_from_type_fixed_offset():
    """_extract_tz_from_type must resolve Fixed/UTC strings used in parameter binding hints."""
    from clickhouse_connect.driver.binding import _extract_tz_from_type

    tz = _extract_tz_from_type("DateTime('Fixed/UTC+05:30:00')")
    assert tz == timezone(timedelta(hours=5, minutes=30))

    tz = _extract_tz_from_type("DateTime64(6, 'Fixed/UTC-03:00:00')")
    assert tz == timezone(timedelta(hours=-3))


def test_tzutil_normalize_named_iana_zones_are_dst_safe():
    """Named IANA zones pass through normalize_timezone unchanged and are marked dst_safe."""
    for tz_name in ("America/Denver", "Europe/Berlin", "Asia/Tokyo", "Australia/Sydney"):
        tz = zoneinfo.ZoneInfo(tz_name)
        normalized, is_valid = tzutil.normalize_timezone(tz)
        assert normalized == tz
        assert is_valid is True


def test_etc_uct_returns_naive_when_tz_mode_naive_utc():
    """
    Regression test for the issue where DateTime('UTC') columns with Etc/UCT
    returned timezone-aware while DateTime columns returned naive
    """
    column_with_explicit_utc = zoneinfo.ZoneInfo("Etc/UCT")
    server_tz = zoneinfo.ZoneInfo("Etc/UCT")
    ctx = QueryContext(tz_mode="naive_utc", server_tz=server_tz, apply_server_tz=True)
    result1 = ctx.active_tz(datatype_tz=column_with_explicit_utc)

    assert result1 is None

    result2 = ctx.active_tz(datatype_tz=None)
    assert result2 is None


def test_schema_mode_with_schema_tz():
    """DateTime('UTC') should return tz-aware in schema mode."""
    ctx = QueryContext(tz_mode="schema")
    result = ctx.active_tz(datatype_tz=zoneinfo.ZoneInfo("UTC"))
    assert result == zoneinfo.ZoneInfo("UTC")


def test_schema_mode_bare_datetime():
    """Bare DateTime (no schema tz) should return naive in schema mode."""
    ctx = QueryContext(tz_mode="schema", server_tz=zoneinfo.ZoneInfo("UTC"), apply_server_tz=True)
    result = ctx.active_tz(datatype_tz=None)
    assert result is None


def test_schema_mode_non_utc_tz():
    """DateTime('America/Denver') should return tz-aware in schema mode."""
    denver = zoneinfo.ZoneInfo("America/Denver")
    ctx = QueryContext(tz_mode="schema")
    result = ctx.active_tz(datatype_tz=denver)
    assert result == denver


def test_schema_mode_with_column_tz_override():
    """Per-column tz override should still work in schema mode."""
    denver = zoneinfo.ZoneInfo("America/Denver")
    ctx = QueryContext(tz_mode="schema", column_tzs={"ts": denver})
    ctx.start_column("ts")
    result = ctx.active_tz(datatype_tz=None)
    assert result == denver


def test_schema_mode_ignores_query_tz():
    """query_tz should not apply to bare DateTime in schema mode."""
    ctx = QueryContext(tz_mode="schema", query_tz=zoneinfo.ZoneInfo("UTC"))
    result = ctx.active_tz(datatype_tz=None)
    assert result is None


def test_schema_mode_ignores_server_tz():
    """Server tz should not apply to bare DateTime in schema mode."""
    denver = zoneinfo.ZoneInfo("America/Denver")
    ctx = QueryContext(tz_mode="schema", server_tz=denver, apply_server_tz=True)
    result = ctx.active_tz(datatype_tz=None)
    assert result is None


def test_schema_mode_etc_utc_schema():
    """DateTime('Etc/UTC') should return tz-aware in schema mode."""
    etc_utc = zoneinfo.ZoneInfo("Etc/UTC")
    ctx = QueryContext(tz_mode="schema")
    result = ctx.active_tz(datatype_tz=etc_utc)
    assert result == etc_utc


def test_tz_mode_invalid_string_raises():
    """Invalid string value for tz_mode should raise ProgrammingError."""
    with pytest.raises(ProgrammingError, match="tz_mode must be"):
        QueryContext(tz_mode="invalid")


def test_invalid_query_tz_string_raises_programming_error():
    """Unknown query_tz string should raise ProgrammingError."""
    with pytest.raises(ProgrammingError, match="query_tz"):
        QueryContext(query_tz="Not/A/Real/Zone")


def test_invalid_column_tz_string_raises_programming_error():
    """Unknown column_tz string must raise ProgrammingError, not NameError."""
    with pytest.raises(ProgrammingError, match="column_tz"):
        QueryContext(column_tzs={"ts": "Not/A/Real/Zone"})


@pytest.mark.parametrize("bad_tz", ["", "/absolute/path", "../foo"])
def test_malformed_query_tz_raises_programming_error(bad_tz):
    """ZoneInfo raises ValueError (not ZoneInfoNotFoundError) for empty/absolute/unnormalized
    keys; the driver must still surface ProgrammingError."""
    with pytest.raises(ProgrammingError, match="query_tz"):
        QueryContext(query_tz=bad_tz)
    with pytest.raises(ProgrammingError, match="column_tz"):
        QueryContext(column_tzs={"ts": bad_tz})


def test_resolve_zone_utc_equivalents_without_tzdata(monkeypatch):
    """UTC-equivalent names must resolve without touching zoneinfo, so hosts without a
    system tzdb (and no `tzdata` extra) can still use the library for UTC."""
    import zoneinfo as zi

    from clickhouse_connect.driver import tzutil as tzutil_mod

    def no_tzdata(*args, **kwargs):
        raise zi.ZoneInfoNotFoundError("tzdata unavailable")

    monkeypatch.setattr(tzutil_mod.zoneinfo, "ZoneInfo", no_tzdata)

    for name in tzutil_mod.UTC_EQUIVALENTS:
        assert tzutil_mod.resolve_zone(name).utcoffset(None).total_seconds() == 0

    # Non-UTC names still require tzdata and surface the underlying error.
    with pytest.raises(zi.ZoneInfoNotFoundError):
        tzutil_mod.resolve_zone("America/Denver")


def test_query_context_utc_query_tz_without_tzdata(monkeypatch):
    """query_tz='UTC' and column_tzs={c: 'UTC'} must succeed without tzdata."""
    import zoneinfo as zi

    from clickhouse_connect.driver import tzutil as tzutil_mod

    monkeypatch.setattr(
        tzutil_mod.zoneinfo,
        "ZoneInfo",
        lambda *a, **kw: (_ for _ in ()).throw(zi.ZoneInfoNotFoundError("tzdata unavailable")),
    )

    ctx = QueryContext(query_tz="UTC")
    assert tzutil_mod.is_utc_timezone(ctx.query_tz)

    ctx = QueryContext(column_tzs={"ts": "Etc/UTC"})
    assert tzutil_mod.is_utc_timezone(ctx.column_tzs["ts"])
