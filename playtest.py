import clickhouse_connect


def main():
    print(f'\nClickHouse Connect installed version: {clickhouse_connect.version()}')
    client = clickhouse_connect.get_client(host='play.clickhouse.com',
                                           username='play',
                                           password='clickhouse',
                                           port=443)
    print(f'ClickHouse Play current version and timezone: {client.server_version} ({client.server_tz})')
    result = client.query('SHOW DATABASES')
    print('ClickHouse play Databases:')
    for row in result.result_set:
        print(f'  {row[0]}')
    client.close()


if __name__ == '__main__':
    main()
