import os
from pathlib import Path

import pytest
from urllib3 import ProxyManager

import clickhouse_connect
from tests.integration_tests.conftest import TestConfig


def test_proxies(test_config: TestConfig):
    if not test_config.proxy_address:
        pytest.skip('Proxy address not configured')
    if test_config.port in (8123, 10723):
        client = clickhouse_connect.get_client(host=test_config.host,
                                               port=test_config.port,
                                               username=test_config.username,
                                               password=test_config.password,
                                               http_proxy=test_config.proxy_address)
        assert '2' in client.command('SELECT version()')
        client.close()

        try:
            os.environ['HTTP_PROXY'] = f'http://{test_config.proxy_address}'
            client = clickhouse_connect.get_client(host=test_config.host,
                                                   port=test_config.port,
                                                   username=test_config.username,
                                                   password=test_config.password)
            assert isinstance(client.http, ProxyManager)
            assert '2' in client.command('SELECT version()')
            client.close()
        finally:
            os.environ.pop('HTTP_PROXY', None)
    else:
        cert_file = f'{Path(__file__).parent}/proxy_ca_cert.crt'
        client = clickhouse_connect.get_client(host=test_config.host,
                                               port=test_config.port,
                                               username=test_config.username,
                                               password=test_config.password,
                                               ca_cert=cert_file,
                                               https_proxy=test_config.proxy_address)
        assert '2' in client.command('SELECT version()')
        client.close()

        try:
            os.environ['HTTPS_PROXY'] = f'{test_config.proxy_address}'
            client = clickhouse_connect.get_client(host=test_config.host,
                                                   port=test_config.port,
                                                   username=test_config.username,
                                                   password=test_config.password,
                                                   ca_cert=cert_file)
            assert isinstance(client.http, ProxyManager)
            assert '2' in client.command('SELECT version()')
            client.close()
        finally:
            os.environ.pop('HTTPS_PROXY', None)
