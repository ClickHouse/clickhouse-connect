from clickhouse_connect.driver import Client


def test_protocol_version(param_client: Client, call):
    query = "select toDateTime(1676369730, 'Asia/Shanghai') as dt FORMAT Native"
    raw = call(param_client.raw_query, query)
    assert raw.hex() == '0101026474084461746554696d65425feb63'

    if param_client.min_version('23.3'):
        raw = call(param_client.raw_query, query, settings={'client_protocol_version': 54337})
        ch_type = raw[14:39].decode()
        assert ch_type == "DateTime('Asia/Shanghai')"
