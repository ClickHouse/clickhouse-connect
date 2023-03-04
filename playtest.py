from io import BytesIO

import pyarrow.parquet as pq

import clickhouse_connect
from clickhouse_connect import common


def main():
    # print(f'\nClickHouse Connect installed version: {clickhouse_connect.version()}')
    # print(build_client_name('Clickhouse client'))
    common.set_setting('readonly', 1)
    client = clickhouse_connect.get_client(host='localhost',
                                           user='default',
                                           port=9123)
    print(f'ClickHouse Play current version and timezone: {client.server_version} ({client.server_tz})')
    Q = 'select 1000 as value'
    # result = client.query_df(Q)
    result = client.raw_query(Q, fmt='Parquet')
    with BytesIO(result) as b_in:
        table = pq.read_table(
            source=b_in,
            use_threads=True,
            memory_map=True,
            buffer_size=10000,
        )
    # print(table.to_pandas(split_blocks=True, self_destruct=True))
    print(table.to_pandas())
    # print(result)
    # result = client.query('SHOW DATABASES')
    # print('ClickHouse play Databases:')
    # for row in result.result_set:
    #     print(f'  {row[0]}')
    client.close()


if __name__ == '__main__':
    main()
