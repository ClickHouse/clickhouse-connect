import pytz
import pytest

import pyarrow as pa

from clickhouse_connect.driver.exceptions import ProgrammingError
from clickhouse_connect.driver.query import QueryContext
from clickhouse_connect.driver.client import _strip_utc_timezone_from_arrow
from clickhouse_connect.driver import tzutil


def test_copy_context():
    settings = {'max_bytes_for_external_group_by': 1024 * 1024 * 100,
                'read_overflow_mode': 'throw'}
    parameters = {'user_id': 'user_1'}
    query_formats = {'IPv*': 'string'}
    context = QueryContext('SELECT source_ip FROM table WHERE user_id = %(user_id)s',
                           settings=settings,
                           parameters=parameters,
                           query_formats=query_formats,
                           use_none=True)
    assert context.use_none is True
    assert context.final_query == "SELECT source_ip FROM table WHERE user_id = 'user_1'"
    assert context.query_formats['IPv*'] == 'string'
    assert context.settings['max_bytes_for_external_group_by'] == 104857600

    context_copy = context.updated_copy(
        settings={'max_bytes_for_external_group_by': 1024 * 1024 * 24, 'max_execution_time': 120},
        parameters={'user_id': 'user_2'}
    )
    assert context_copy.settings['read_overflow_mode'] == 'throw'
    assert context_copy.settings['max_execution_time'] == 120
    assert context_copy.settings['max_bytes_for_external_group_by'] == 25165824
    assert context_copy.final_query == "SELECT source_ip FROM table WHERE user_id = 'user_2'"


def test_active_tz_utc_defaults_to_naive():
    ctx = QueryContext(query_tz=pytz.UTC)
    assert ctx.utc_tz_aware is False
    assert ctx.active_tz(None) is None


def test_active_tz_utc_opt_in_timezone():
    ctx = QueryContext(query_tz=pytz.UTC, utc_tz_aware=True)
    assert ctx.utc_tz_aware is True
    assert ctx.active_tz(None) == pytz.UTC


def test_active_tz_etc_utc_defaults_to_naive():
    """Test that Etc/UTC is treated as UTC for naive datetime conversion.

    This test reproduces the bug where pytz.timezone('Etc/UTC') != pytz.UTC,
    causing timezone-aware datetimes to be returned when they should be naive.
    """
    etc_utc = pytz.timezone('Etc/UTC')
    ctx = QueryContext(query_tz=etc_utc)
    assert ctx.utc_tz_aware is False
    # BUG: Previously returned etc_utc instead of None
    assert ctx.active_tz(None) is None  # Should return None for naive datetime


def test_active_tz_etc_utc_opt_in_timezone():
    """Test that Etc/UTC with utc_tz_aware=True returns timezone."""
    etc_utc = pytz.timezone('Etc/UTC')
    ctx = QueryContext(query_tz=etc_utc, utc_tz_aware=True)
    assert ctx.utc_tz_aware is True
    assert ctx.active_tz(None) is not None  # Should return the timezone


def test_is_utc_timezone_pytz_utc():
    assert tzutil.is_utc_timezone(pytz.UTC) is True


def test_is_utc_timezone_etc_utc():
    assert tzutil.is_utc_timezone(pytz.timezone('Etc/UTC')) is True


def test_is_utc_timezone_utc_string():
    assert tzutil.is_utc_timezone(pytz.timezone('UTC')) is True


def test_is_utc_timezone_gmt():
    assert tzutil.is_utc_timezone(pytz.timezone('GMT')) is True


def test_is_utc_timezone_non_utc():
    assert tzutil.is_utc_timezone(pytz.timezone('America/Denver')) is False
    assert tzutil.is_utc_timezone(pytz.timezone('Europe/London')) is False


def test_is_utc_timezone_none():
    assert tzutil.is_utc_timezone(None) is False


def test_is_utc_timezone_string():
    """Test is_utc_timezone with string timezone names (used by Arrow field.type.tz)."""
    assert tzutil.is_utc_timezone('UTC') is True
    assert tzutil.is_utc_timezone('Etc/UTC') is True
    assert tzutil.is_utc_timezone('GMT') is True
    assert tzutil.is_utc_timezone('America/Denver') is False
    assert tzutil.is_utc_timezone('Europe/London') is False


def test_strip_utc_timezone_from_arrow_strips_utc():
    ts = pa.array([1705312200000000], type=pa.timestamp('us', 'UTC'))
    table = pa.Table.from_arrays([ts], names=['ts'])
    result = _strip_utc_timezone_from_arrow(table)
    assert result.schema[0].type == pa.timestamp('us')


def test_strip_utc_timezone_from_arrow_strips_etc_utc():
    ts = pa.array([1705312200000000], type=pa.timestamp('us', 'Etc/UTC'))
    table = pa.Table.from_arrays([ts], names=['ts'])
    result = _strip_utc_timezone_from_arrow(table)
    assert result.schema[0].type == pa.timestamp('us')


def test_strip_utc_timezone_from_arrow_preserves_non_utc():
    ts = pa.array([1705312200000000], type=pa.timestamp('us', 'America/Denver'))
    table = pa.Table.from_arrays([ts], names=['ts'])
    result = _strip_utc_timezone_from_arrow(table)
    assert result.schema[0].type == pa.timestamp('us', 'America/Denver')


def test_strip_utc_timezone_from_arrow_preserves_naive():
    ts = pa.array([1705312200000000], type=pa.timestamp('us'))
    table = pa.Table.from_arrays([ts], names=['ts'])
    result = _strip_utc_timezone_from_arrow(table)
    assert result.schema[0].type == pa.timestamp('us')


def test_utc_equivalent_timezones_normalize_to_naive():
    """Test that UTC-equivalent timezones (Etc/UCT, GMT, etc.) return naive datetimes by default"""
    utc_equivalents = ['Etc/UCT', 'GMT', 'Etc/GMT', 'Etc/Universal', 'Etc/Zulu', 'UCT', 'Universal']

    for tz_name in utc_equivalents:
        tz = pytz.timezone(tz_name)
        ctx = QueryContext(utc_tz_aware=False)
        result = ctx.active_tz(datatype_tz=tz)
        assert result is None


def test_utc_equivalent_timezones_with_utc_tz_aware():
    """Test that UTC-equivalent timezones return timezone-aware when utc_tz_aware=True"""
    utc_equivalents = ['Etc/UCT', 'GMT', 'Etc/GMT']

    for tz_name in utc_equivalents:
        tz = pytz.timezone(tz_name)
        ctx = QueryContext(utc_tz_aware=True)
        result = ctx.active_tz(datatype_tz=tz)
        assert result == tz


def test_tzutil_normalize_utc_equivalents():
    """Test that tzutil.normalize_timezone properly normalizes UTC-equivalent timezones"""
    utc_equivalents = [
        'UTC', 'Etc/UTC', 'Etc/UCT', 'Etc/Universal',
        'GMT', 'Etc/GMT', 'Etc/GMT+0', 'Etc/GMT-0', 'Etc/GMT0',
        'GMT+0', 'GMT-0', 'GMT0',
        'Greenwich', 'UCT', 'Universal', 'Zulu'
    ]

    for tz_name in utc_equivalents:
        tz = pytz.timezone(tz_name)
        normalized, is_valid = tzutil.normalize_timezone(tz)
        assert normalized == pytz.UTC
        assert is_valid is True


def test_etc_uct_returns_naive_when_utc_tz_aware_false():
    """
    Regression test for the issue where DateTime('UTC') columns with Etc/UCT
    returned timezone-aware while DateTime columns returned naive
    """
    column_with_explicit_utc = pytz.timezone('Etc/UCT')
    server_tz = pytz.timezone('Etc/UCT')
    ctx = QueryContext(utc_tz_aware=False, server_tz=server_tz, apply_server_tz=True)
    result1 = ctx.active_tz(datatype_tz=column_with_explicit_utc)

    assert result1 is None

    result2 = ctx.active_tz(datatype_tz=None)
    assert result2 is None


def test_schema_mode_with_schema_tz():
    """DateTime('UTC') should return tz-aware in schema mode."""
    ctx = QueryContext(utc_tz_aware="schema")
    result = ctx.active_tz(datatype_tz=pytz.UTC)
    assert result == pytz.UTC


def test_schema_mode_bare_datetime():
    """Bare DateTime (no schema tz) should return naive in schema mode."""
    ctx = QueryContext(utc_tz_aware="schema", server_tz=pytz.UTC, apply_server_tz=True)
    result = ctx.active_tz(datatype_tz=None)
    assert result is None


def test_schema_mode_non_utc_tz():
    """DateTime('America/Denver') should return tz-aware in schema mode."""
    denver = pytz.timezone("America/Denver")
    ctx = QueryContext(utc_tz_aware="schema")
    result = ctx.active_tz(datatype_tz=denver)
    assert result == denver


def test_schema_mode_with_column_tz_override():
    """Per-column tz override should still work in schema mode."""
    denver = pytz.timezone("America/Denver")
    ctx = QueryContext(utc_tz_aware="schema", column_tzs={"ts": denver})
    ctx.start_column("ts")
    result = ctx.active_tz(datatype_tz=None)
    assert result == denver


def test_schema_mode_ignores_query_tz():
    """query_tz should not apply to bare DateTime in schema mode."""
    ctx = QueryContext(utc_tz_aware="schema", query_tz=pytz.UTC)
    result = ctx.active_tz(datatype_tz=None)
    assert result is None


def test_schema_mode_ignores_server_tz():
    """Server tz should not apply to bare DateTime in schema mode."""
    denver = pytz.timezone("America/Denver")
    ctx = QueryContext(utc_tz_aware="schema", server_tz=denver, apply_server_tz=True)
    result = ctx.active_tz(datatype_tz=None)
    assert result is None


def test_schema_mode_etc_utc_schema():
    """DateTime('Etc/UTC') should return tz-aware in schema mode."""
    etc_utc = pytz.timezone("Etc/UTC")
    ctx = QueryContext(utc_tz_aware="schema")
    result = ctx.active_tz(datatype_tz=etc_utc)
    assert result == etc_utc


def test_schema_mode_invalid_string_raises():
    """Invalid string value for utc_tz_aware should raise ProgrammingError."""
    with pytest.raises(ProgrammingError, match="utc_tz_aware must be"):
        QueryContext(utc_tz_aware="invalid")
