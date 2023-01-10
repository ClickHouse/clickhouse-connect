from clickhouse_connect.driver import Client


def test_numbers_stream(test_client: Client):
    query_result = test_client.query('SELECT number FROM numbers(1000000) LIMIT 1000000', column_oriented=True)
    total = 0
    blocks = 0
    with query_result:
        for x in query_result.stream_blocks():
            total += len(x[0])
            blocks += 1
    assert blocks > 0
    assert total == 1000000
