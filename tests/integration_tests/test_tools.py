from pathlib import Path
from typing import Callable

from clickhouse_connect.driver import Client
from clickhouse_connect.driver.tools import insert_file
from tests.integration_tests.conftest import TestConfig


def test_csv_upload(test_client: Client, table_context: Callable):
    data_file = f'{Path(__file__).parent}/movies.csv.gz'
    with table_context('test_csv_upload', ['movie String', 'year UInt16', 'rating Decimal32(3)']):
        insert_result = insert_file(test_client, 'test_csv_upload', data_file,
                                    settings={'input_format_allow_errors_ratio': .2,
                                              'input_format_allow_errors_num': 5})
        assert 248 == insert_result.written_rows
        res = test_client.query(
            'SELECT count() as count, sum(rating) as rating, max(year) as year FROM test_csv_upload').first_item
        assert res['count'] == 248
        assert res['year'] == 2022


def test_parquet_upload(test_config: TestConfig, test_client: Client, table_context: Callable):
    data_file = f'{Path(__file__).parent}/movies.parquet'
    full_table = f'{test_config.test_database}.test_parquet_upload'
    with table_context(full_table, ['movie String', 'year UInt16', 'rating Float64']):
        insert_result = insert_file(test_client, full_table, data_file, 'Parquet',
                                    settings={'output_format_parquet_string_as_string': 1})
        assert 250 == insert_result.written_rows
        res = test_client.query(
            f'SELECT count() as count, sum(rating) as rating, max(year) as year FROM {full_table}').first_item
        assert res['count'] == 250
        assert res['year'] == 2022


def test_json_insert(test_client: Client, table_context: Callable):
    data_file = f'{Path(__file__).parent}/json_test.ndjson'
    with table_context('test_json_upload', ['key UInt16', 'flt_val Float64', 'int_val Int8']):
        insert_file(test_client, 'test_json_upload', data_file, 'JSONEachRow')
        res = test_client.query('SELECT * FROM test_json_upload ORDER BY key').result_rows
        assert res[1][0] == 17
        assert res[1][1] == 5.3
        assert res[1][2] == 121
