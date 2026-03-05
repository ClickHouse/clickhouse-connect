import warnings

import pytz
import pytest

import pyarrow as pa

from clickhouse_connect.driver.exceptions import ProgrammingError
from clickhouse_connect.driver.query import QueryContext, _resolve_tz_mode
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
    assert ctx.tz_mode == "naive_utc"
    assert ctx.active_tz(None) is None


def test_active_tz_utc_opt_in_timezone():
    ctx = QueryContext(query_tz=pytz.UTC, tz_mode="aware")
    assert ctx.tz_mode == "aware"
    assert ctx.active_tz(None) == pytz.UTC


def test_active_tz_etc_utc_defaults_to_naive():
    """Test that Etc/UTC is treated as UTC for naive datetime conversion.

    This test reproduces the bug where pytz.timezone('Etc/UTC') != pytz.UTC,
    causing timezone-aware datetimes to be returned when they should be naive.
    """
    etc_utc = pytz.timezone('Etc/UTC')
    ctx = QueryContext(query_tz=etc_utc)
    assert ctx.tz_mode == "naive_utc"
    # BUG: Previously returned etc_utc instead of None
    assert ctx.active_tz(None) is None  # Should return None for naive datetime


def test_active_tz_etc_utc_opt_in_timezone():
    """Test that Etc/UTC with tz_mode='aware' returns timezone."""
    etc_utc = pytz.timezone('Etc/UTC')
    ctx = QueryContext(query_tz=etc_utc, tz_mode="aware")
    assert ctx.tz_mode == "aware"
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
        ctx = QueryContext(tz_mode="naive_utc")
        result = ctx.active_tz(datatype_tz=tz)
        assert result is None


def test_utc_equivalent_timezones_with_tz_mode_aware():
    """Test that UTC-equivalent timezones return timezone-aware when tz_mode='aware'"""
    utc_equivalents = ['Etc/UCT', 'GMT', 'Etc/GMT']

    for tz_name in utc_equivalents:
        tz = pytz.timezone(tz_name)
        ctx = QueryContext(tz_mode="aware")
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


def test_etc_uct_returns_naive_when_tz_mode_naive_utc():
    """
    Regression test for the issue where DateTime('UTC') columns with Etc/UCT
    returned timezone-aware while DateTime columns returned naive
    """
    column_with_explicit_utc = pytz.timezone('Etc/UCT')
    server_tz = pytz.timezone('Etc/UCT')
    ctx = QueryContext(tz_mode="naive_utc", server_tz=server_tz, apply_server_tz=True)
    result1 = ctx.active_tz(datatype_tz=column_with_explicit_utc)

    assert result1 is None

    result2 = ctx.active_tz(datatype_tz=None)
    assert result2 is None


def test_schema_mode_with_schema_tz():
    """DateTime('UTC') should return tz-aware in schema mode."""
    ctx = QueryContext(tz_mode="schema")
    result = ctx.active_tz(datatype_tz=pytz.UTC)
    assert result == pytz.UTC


def test_schema_mode_bare_datetime():
    """Bare DateTime (no schema tz) should return naive in schema mode."""
    ctx = QueryContext(tz_mode="schema", server_tz=pytz.UTC, apply_server_tz=True)
    result = ctx.active_tz(datatype_tz=None)
    assert result is None


def test_schema_mode_non_utc_tz():
    """DateTime('America/Denver') should return tz-aware in schema mode."""
    denver = pytz.timezone('America/Denver')
    ctx = QueryContext(tz_mode="schema")
    result = ctx.active_tz(datatype_tz=denver)
    assert result == denver


def test_schema_mode_with_column_tz_override():
    """Per-column tz override should still work in schema mode."""
    denver = pytz.timezone('America/Denver')
    ctx = QueryContext(tz_mode="schema", column_tzs={'ts': denver})
    ctx.start_column('ts')
    result = ctx.active_tz(datatype_tz=None)
    assert result == denver


def test_schema_mode_ignores_query_tz():
    """query_tz should not apply to bare DateTime in schema mode."""
    ctx = QueryContext(tz_mode="schema", query_tz=pytz.UTC)
    result = ctx.active_tz(datatype_tz=None)
    assert result is None


def test_schema_mode_ignores_server_tz():
    """Server tz should not apply to bare DateTime in schema mode."""
    denver = pytz.timezone('America/Denver')
    ctx = QueryContext(tz_mode="schema", server_tz=denver, apply_server_tz=True)
    result = ctx.active_tz(datatype_tz=None)
    assert result is None


def test_schema_mode_etc_utc_schema():
    """DateTime('Etc/UTC') should return tz-aware in schema mode."""
    etc_utc = pytz.timezone('Etc/UTC')
    ctx = QueryContext(tz_mode="schema")
    result = ctx.active_tz(datatype_tz=etc_utc)
    assert result == etc_utc


def test_tz_mode_invalid_string_raises():
    """Invalid string value for tz_mode should raise ProgrammingError."""
    with pytest.raises(ProgrammingError, match='tz_mode must be'):
        QueryContext(tz_mode="invalid")


def test_utc_tz_aware_false_maps_to_naive_utc():
    """utc_tz_aware=False should map to tz_mode='naive_utc' with a DeprecationWarning."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        ctx = QueryContext(utc_tz_aware=False)
        assert ctx.tz_mode == "naive_utc"
        assert len(w) == 1
        assert issubclass(w[0].category, DeprecationWarning)
        assert "utc_tz_aware is deprecated" in str(w[0].message)


def test_utc_tz_aware_true_maps_to_aware():
    """utc_tz_aware=True should map to tz_mode='aware' with a DeprecationWarning."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        ctx = QueryContext(utc_tz_aware=True)
        assert ctx.tz_mode == "aware"
        assert len(w) == 1
        assert issubclass(w[0].category, DeprecationWarning)


def test_utc_tz_aware_schema_maps_to_schema():
    """utc_tz_aware='schema' should map to tz_mode='schema' with a DeprecationWarning."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        ctx = QueryContext(utc_tz_aware="schema")
        assert ctx.tz_mode == "schema"
        assert len(w) == 1
        assert issubclass(w[0].category, DeprecationWarning)


def test_both_tz_mode_and_utc_tz_aware_raises():
    """Providing both tz_mode and utc_tz_aware should raise ProgrammingError."""
    with pytest.raises(ProgrammingError, match='Cannot specify both'):
        QueryContext(tz_mode="aware", utc_tz_aware=True)


def test_utc_tz_aware_invalid_string_raises():
    """Invalid string value for utc_tz_aware should raise ProgrammingError."""
    with pytest.raises(ProgrammingError, match='utc_tz_aware must be'):
        QueryContext(utc_tz_aware="invalid")


def test_resolve_tz_mode_defaults():
    """No arguments should return 'naive_utc'."""
    assert _resolve_tz_mode() == "naive_utc"


def test_resolve_tz_mode_string_bool_coercion():
    """String booleans from URL params should be coerced correctly."""
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        assert _resolve_tz_mode(utc_tz_aware="true") == "aware"
        assert _resolve_tz_mode(utc_tz_aware="false") == "naive_utc"
        assert _resolve_tz_mode(utc_tz_aware="True") == "aware"
        assert _resolve_tz_mode(utc_tz_aware="False") == "naive_utc"
        assert _resolve_tz_mode(utc_tz_aware="1") == "aware"
        assert _resolve_tz_mode(utc_tz_aware="0") == "naive_utc"


def test_utc_tz_aware_property_returns_legacy_value():
    """Accessing ctx.utc_tz_aware should return the legacy equivalent with a DeprecationWarning."""
    ctx = QueryContext(tz_mode="naive_utc")
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        assert ctx.utc_tz_aware is False
        assert len(w) == 1
        assert issubclass(w[0].category, DeprecationWarning)

    ctx2 = QueryContext(tz_mode="aware")
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        assert ctx2.utc_tz_aware is True

    ctx3 = QueryContext(tz_mode="schema")
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        assert ctx3.utc_tz_aware == "schema"
