import os

import pytest
from urllib3.exceptions import SSLError

from clickhouse_connect import get_client
from clickhouse_connect.driver.common import coerce_bool
from clickhouse_connect.driver.exceptions import OperationalError
from tests.helpers import PROJECT_ROOT_DIR

# See .docker/clickhouse/single_node_tls for the server configuration
cert_dir = f'{PROJECT_ROOT_DIR}/.docker/clickhouse/single_node_tls/certificates/'
host = 'server1.clickhouse.test'


def test_basic_tls():
    if not coerce_bool(os.environ.get('CLICKHOUSE_CONNECT_TEST_TLS', 'False')):
        pytest.skip('TLS tests not enabled')
    client = get_client(interface='https', host=host, port=10843, verify=False)
    assert client.command("SELECT 'insecure'") == 'insecure'
    client.http.clear()

    client = get_client(interface='https', host=host, port=10843, ca_cert=f'{cert_dir}ca.crt')
    assert client.command("SELECT 'verify_server'") == 'verify_server'
    client.http.clear()

    try:
        get_client(interface='https', host='localhost', port=10843, ca_cert=f'{cert_dir}ca.crt')
        pytest.fail('Expected TLS exception with a different hostname')
    except OperationalError as ex:
        assert isinstance(ex.__cause__.reason, SSLError)
    client.http.clear()

    try:
        get_client(interface='https', host=host, port=10843)
        pytest.fail('Expected TLS exception with a self-signed cert')
    except OperationalError as ex:
        assert isinstance(ex.__cause__.reason, SSLError)


def test_mutual_tls():
    if not coerce_bool(os.environ.get('CLICKHOUSE_CONNECT_TEST_TLS', 'False')):
        pytest.skip('TLS tests not enabled')
    client = get_client(interface='https',
                        username='cert_user',
                        host=host,
                        port=10843,
                        ca_cert=f'{cert_dir}ca.crt',
                        client_cert=f'{cert_dir}client.crt',
                        client_cert_key=f'{cert_dir}client.key')
    assert client.command('SELECT user()') == 'cert_user'
