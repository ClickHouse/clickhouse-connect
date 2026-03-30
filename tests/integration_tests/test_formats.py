from clickhouse_connect.driver import Client


def test_uint64_format(param_client: Client, call):
    # Default should be unsigned
    result = call(param_client.query, "SELECT toUInt64(9523372036854775807) as value")
    assert result.result_set[0][0] == 9523372036854775807
    result = call(param_client.query, "SELECT toUInt64(9523372036854775807) as value", query_formats={"UInt64": "signed"})
    assert result.result_set[0][0] == -8923372036854775809
    result = call(param_client.query, "SELECT toUInt64(9523372036854775807) as value", query_formats={"UInt64": "native"})
    assert result.result_set[0][0] == 9523372036854775807
