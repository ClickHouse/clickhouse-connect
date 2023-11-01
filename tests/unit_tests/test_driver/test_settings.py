from clickhouse_connect import common


def test_setting():
    try:
        assert common.get_setting('autogenerate_session_id')
        common.set_setting('autogenerate_session_id', False)
        assert common.get_setting('autogenerate_session_id') is False
    finally:
        common.set_setting('autogenerate_session_id', True)
