from clickhouse_connect.driver import Client, ProgrammingError


def test_uint64_format(test_client: Client):
    # Default should be unsigned
    result = test_client.query('SELECT toUInt64(9523372036854775807) as value')
    assert result.result_set[0][0] == 9523372036854775807
    result = test_client.query('SELECT toUInt64(9523372036854775807) as value', query_formats={'UInt64': 'signed'})
    assert result.result_set[0][0] == -8923372036854775809
    result = test_client.query('SELECT toUInt64(9523372036854775807) as value', query_formats={'UInt64': 'native'})
    assert result.result_set[0][0] == 9523372036854775807
    try:
        test_client.query('SELECT toUInt64(9523372036854775807) as signed', query_formats={'UInt64': 'huh'})
    except ProgrammingError:
        pass
