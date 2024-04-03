from datetime import datetime, date
from typing import Callable

from clickhouse_connect.driver import Client


def test_params(test_client: Client, table_context: Callable):
    result = test_client.query('SELECT name, database FROM system.tables WHERE database = {db:String}',
                               parameters={'db': 'system'})
    assert result.first_item['database'] == 'system'
    if test_client.min_version('21'):
        result = test_client.query('SELECT name, {col:String} FROM system.tables WHERE table ILIKE {t:String}',
                                   parameters={'t': '%rr%', 'col': 'database'})
        assert 'rr' in result.first_item['name']

    first_date = datetime.strptime('Jun 1 2005  1:33PM', '%b %d %Y %I:%M%p')
    first_date = test_client.server_tz.localize(first_date)
    second_date = datetime.strptime('Dec 25 2022  5:00AM', '%b %d %Y %I:%M%p')
    second_date = test_client.server_tz.localize(second_date)
    with table_context('test_bind_params', ['key UInt64', 'dt DateTime', 'value String', 't Tuple(String, String)']):
        test_client.insert('test_bind_params',
                           [[1, first_date, 'v11', ('one', 'two')],
                            [2, second_date, 'v21', ('t1', 't2')],
                            [3, datetime.now(), 'v31', ('str1', 'str2')]])
        result = test_client.query('SELECT * FROM test_bind_params WHERE dt = {dt:DateTime}',
                                   parameters={'dt': second_date})
        assert result.first_item['key'] == 2
        result = test_client.query('SELECT * FROM test_bind_params WHERE dt = %(dt)s',
                                   parameters={'dt': first_date})
        assert result.first_item['key'] == 1
        result = test_client.query("SELECT * FROM test_bind_params WHERE value != %(v)s AND value like '%%1'",
                                   parameters={'v': 'v11'})
        assert result.row_count == 2
        result = test_client.query('SELECT * FROM test_bind_params WHERE value IN %(tp)s',
                                   parameters={'tp': ('v18', 'v31')})
        assert result.first_item['key'] == 3

    result = test_client.query('SELECT number FROM numbers(10) WHERE {n:Nullable(String)} IS NULL',
                               parameters={'n': None}).result_rows
    assert len(result) == 10

    date_params = [date(2023, 6, 1), date(2023, 8, 5)]
    result = test_client.query('SELECT {l:Array(Date)}', parameters={'l': date_params}).first_row
    assert date_params == result[0]

    dt_params = [datetime(2023, 6, 1, 7, 40, 2), datetime(2023, 8, 17, 20, 0, 10)]
    result = test_client.query('SELECT {l:Array(DateTime)}', parameters={'l': dt_params}).first_row
    assert dt_params == result[0]

    num_array_params = [2.5, 5.3, 7.4]
    result = test_client.query('SELECT {l:Array(Float64)}', parameters={'l': num_array_params}).first_row
    assert num_array_params == result[0]
    result = test_client.query('SELECT %(l)s', parameters={'l': num_array_params}).first_row
    assert num_array_params == result[0]

    tp_params = ('str1', 'str2')
    result = test_client.query('SELECT %(tp)s', parameters={'tp': tp_params}).first_row
    assert tp_params == result[0]

    num_params = {'p_0': 2, 'p_1': 100523.55}
    result = test_client.query(
        'SELECT count() FROM system.tables WHERE total_rows > %(p_0)d and total_rows < %(p_1)f', parameters=num_params)
    assert result.first_row[0] > 0
