from datetime import datetime

from clickhouse_connect.driver import Client


def test_params(test_client: Client, table_context: callable):
    result = test_client.query('SELECT name, database FROM system.tables WHERE database = {db:String}',
                               parameters={'db': 'system'})
    assert result.first_item['database'] == 'system'
    result = test_client.query('SELECT name, {col:String} FROM system.tables WHERE table ILIKE {t:String}',
                               parameters={'t': '%rr%', 'col': 'database'})
    assert 'rr' in result.first_item['name']

    first_date = datetime.strptime('Jun 1 2005  1:33PM', '%b %d %Y %I:%M%p')
    second_date = datetime.strptime('Dec 25 2022  5:00AM', '%b %d %Y %I:%M%p')
    with table_context('test_bind_params', ['key UInt64', 'dt DateTime']):
        test_client.insert('test_bind_params', [[1, first_date], [2, second_date], [3, datetime.now()]])
        result = test_client.query('SELECT * FROM test_bind_params WHERE dt = {dt:DateTime}',
                                   parameters={'dt': second_date})
        assert result.first_item['key'] == 2
        result = test_client.query('SELECT * FROM test_bind_params WHERE dt = %(dt)s',
                                   parameters={'dt': first_date})
        assert result.first_item['key'] == 1
