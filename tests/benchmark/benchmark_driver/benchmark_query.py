import pytest
import pytz

import clickhouse_connect
from clickhouse_connect.datatypes.format import set_default_formats
from examples.benchmark import create_table


@pytest.mark.parametrize("col_name", ['datetime', 'dt64','dt64d'])
@pytest.mark.parametrize("query_tz", [None, pytz.UTC], ids=['NoTZ','UTC'])
def test_benchmark_dt64(benchmark, col_name, query_tz):
    rows = 100000
    client = clickhouse_connect.get_client(compress=False)
    set_default_formats('IP*', 'native', '*Int64', 'native')
    create_table(client, [col_name], rows)
    result = benchmark(
            client.query,
            f'SELECT * FROM benchmark_test LIMIT {rows}', query_tz=query_tz, column_oriented=True)
    assert result.row_count == rows
