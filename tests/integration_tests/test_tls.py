import os

import pytest

from clickhouse_connect.driver.common import coerce_bool
from clickhouse_connect.driver.exceptions import OperationalError
from tests.helpers import PROJECT_ROOT_DIR

# See .docker/clickhouse/single_node_tls for the server configuration
cert_dir = f'{PROJECT_ROOT_DIR}/.docker/clickhouse/single_node_tls/certificates/'
host = 'server1.clickhouse.test'


def test_basic_tls(client_factory, call):
    if not coerce_bool(os.environ.get('CLICKHOUSE_CONNECT_TEST_TLS', 'False')):
        pytest.skip('TLS tests not enabled')
    client = client_factory(interface='https', host=host, port=10843, verify=False, database='default')
    assert call(client.command, "SELECT 'insecure'") == 'insecure'

    client = client_factory(interface='https', host=host, port=10843, ca_cert=f'{cert_dir}ca.crt', database='default')
    assert call(client.command, "SELECT 'verify_server'") == 'verify_server'

    try:
        client_factory(interface='https', host='localhost', port=10843, ca_cert=f'{cert_dir}ca.crt', database='default')
        pytest.fail('Expected TLS exception with a different hostname')
    except OperationalError as ex:
        # For sync (urllib3): ex.__cause__.reason is SSLError
        # For async (aiohttp): ex.__cause__ is ClientConnectorCertificateError
        assert ex.__cause__ is not None
        assert 'SSL' in str(ex.__cause__) or 'certificate' in str(ex.__cause__).lower()

    try:
        client_factory(interface='https', host='localhost', port=10843, database='default')
        pytest.fail('Expected TLS exception with a self-signed cert')
    except OperationalError as ex:
        assert ex.__cause__ is not None
        assert 'SSL' in str(ex.__cause__) or 'certificate' in str(ex.__cause__).lower()


def test_mutual_tls(client_factory, call):
    if not coerce_bool(os.environ.get('CLICKHOUSE_CONNECT_TEST_TLS', 'False')):
        pytest.skip('TLS tests not enabled')
    client = client_factory(interface='https',
                        username='cert_user',
                        host=host,
                        port=10843,
                        ca_cert=f'{cert_dir}ca.crt',
                        client_cert=f'{cert_dir}client.crt',
                        client_cert_key=f'{cert_dir}client.key',
                        database='default')
    assert call(client.command, 'SELECT user()') == 'cert_user'
