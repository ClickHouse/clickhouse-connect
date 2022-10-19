from clickhouse_connect.driver import Client, ProgrammingError


def test_uint64_format(test_client: Client):
    # Default should be unsigned
    rs = test_client.query("SELECT 9523372036854775807::UInt64 as value")
    assert rs.result_set[0][0] == 9523372036854775807
    rs = test_client.query("SELECT 9523372036854775807::UInt64 as value", query_formats={'UInt64': 'signed'})
    assert rs.result_set[0][0] == -8923372036854775809
    rs = test_client.query("SELECT 9523372036854775807::UInt64 as value", query_formats={'UInt64': 'native'})
    assert rs.result_set[0][0] == 9523372036854775807
    try:
        rs = test_client.query("SELECT 9523372036854775807::UInt64 as signed", query_formats={'UInt64': 'huh'})
    except ProgrammingError:
        pass


