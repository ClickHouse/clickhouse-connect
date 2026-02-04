import pytz

from clickhouse_connect.driver.query import QueryContext
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
