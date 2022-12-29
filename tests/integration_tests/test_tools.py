from pathlib import Path
from typing import Callable

from clickhouse_connect.driver import Client
from clickhouse_connect.driver.tools import insert_csv_file, insert_file


def test_csv_upload(test_client: Client, table_context: Callable):
    data_file = f'{Path(__file__).parent}/IMDB_Top_250.csv'
    with table_context('test_csv_upload', ['movie String', 'year UInt16', 'rating Decimal32(3)']):
        insert_csv_file(test_client, 'test_csv_upload', data_file)
        res = test_client.query(
            'SELECT count() as count, sum(rating) as rating, max(year) as year FROM test_csv_upload').first_item
        assert res['count'] == 250
        assert res['year'] == 2022


def test_parquet_upload(test_client: Client, table_context: Callable):
    data_file = f'{Path(__file__).parent}/IMDB_Top_250.parquet'
    with table_context('test_parquet_upload', ['movie String', 'year UInt16', 'rating Float64']):
        insert_file(test_client, 'test_parquet_upload', data_file, 'Parquet')
        res = test_client.query(
            'SELECT count() as count, sum(rating) as rating, max(year) as year FROM test_parquet_upload').first_item
        assert res['count'] == 250
        assert res['year'] == 2022
