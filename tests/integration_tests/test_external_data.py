from pathlib import Path

from clickhouse_connect.driver import Client
from clickhouse_connect.driver.external import ExternalData

ext_settings = {'input_format_allow_errors_num': 10, 'input_format_allow_errors_ratio': .2}


def test_external_simple(test_client: Client):
    data_file = f'{Path(__file__).parent}/movies.csv'
    data = ExternalData(data_file, fmt='CSVWithNames',
                        structure=['movie String', 'year UInt16', 'rating Decimal32(3)'])
    result = test_client.query('SELECT * FROM movies ORDER BY movie',
                               external_data=data,
                               settings=ext_settings).result_rows
    assert result[0][0] == '12 Angry Men'


def test_external_multiple(test_client: Client):
    movies_file = f'{Path(__file__).parent}/movies.csv'
    data = ExternalData(movies_file, fmt='CSVWithNames',
                        structure=['movie String', 'year UInt16', 'rating Decimal32(3)'])
    actors_file = f'{Path(__file__).parent}/actors.csv'
    data.add_file(actors_file, fmt='CSV', types='String,UInt16,String')
    result = test_client.query('SELECT * FROM actors', external_data=data, settings={
        'input_format_allow_errors_num': 10, 'input_format_allow_errors_ratio': .2}).result_rows
    assert result[1][1] == 1940
    result = test_client.query(
        'SELECT _1, movie FROM actors INNER JOIN movies ON actors._3 = movies.movie AND actors._2 = 1940',
        external_data=data,
        settings=ext_settings).result_rows
    assert len(result) == 1
    assert result[0][1] == 'Scarface'


def test_external_parquet(test_client: Client):
    movies_file = f'{Path(__file__).parent}/movies.parquet'
    test_client.command("""
CREATE TABLE IF NOT EXISTS num
ENGINE = MergeTree
ORDER BY number AS
SELECT number, concat(toString(number), 'x') as t
FROM numbers(2500)
WHERE (number > 1950) AND (number < 2025)
    """)
    data = ExternalData(movies_file, fmt='Parquet', structure=['movie String', 'year UInt16', 'rating Float64'])
    result = test_client.query(
        "SELECT * FROM movies INNER JOIN num ON movies.year = number AND t = '2000x' ORDER BY movie",
        external_data=data).result_rows
    assert len(result) == 5
    assert result[2][0] == 'Memento'
    test_client.command('DROP TABLE num')


def test_external_binary(test_client: Client):
    actors = 'Robert Redford\t1936\tThe Sting\nAl Pacino\t1940\tScarface'.encode()
    data = ExternalData(file_name='actors.csv', data=actors,
                        structure='name String, birth_year UInt16, movie String')
    result = test_client.query('SELECT * FROM actors ORDER BY birth_year DESC', external_data=data).result_rows
    assert len(result) == 2
    assert result[1][2] == 'The Sting'


def test_external_raw(test_client: Client):
    movies_file = f'{Path(__file__).parent}/movies.parquet'
    data = ExternalData(movies_file, fmt='Parquet', structure=['movie String', 'year UInt16', 'rating Float64'])
    result = test_client.raw_query('SELECT avg(rating) FROM movies', external_data=data)
    assert '8.25' == result.decode()[0:4]
