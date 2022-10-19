from pathlib import Path

import pytest
from requests.exceptions import SSLError

from clickhouse_connect import get_client
from clickhouse_connect.driver.exceptions import OperationalError
from clickhouse_connect.driver.httpclient import reset_connections

cert_dir = f'{Path(__file__).parent}/'
host = 'server.clickhouseconnect.test'

"""
Sample openSSL section for clickhouse server configuration using the certificates in this directory

<openSSL>
    <server>
        <certificateFile>server.crt</certificateFile>
        <privateKeyFile>server.key</privateKeyFile>
        <verificationMode>strict</verificationMode>
        <caConfig>ca.crt</caConfig>
        <cacheSessions>true</cacheSessions>
        <disableProtocols>sslv2,sslv3,tlsv1</disableProtocols>
        <preferServerCiphers>true</preferServerCiphers>
    </server>
</openSSL>

Sample xml user for clickhouse server configuration (within the <users> element in users.xml)
<cert_user>
    <ssl_certificates>
        <common_name>client.clickhouseconnect.test</common_name>
    </ssl_certificates>
    <profile>default</profile>
</cert_user>
"""


def test_basic_tls(request):
    if not request.config.getoption('tls'):
        pytest.skip('TLS tests not enabled')
    reset_connections()
    client = get_client(interface='https', host=host, port=8443, verify=False)
    assert client.command("SELECT 'insecure'") == 'insecure'

    reset_connections()  # Otherwise, the requests connection pool reuses the "unverified" connection
    client = get_client(interface='https', host=host, port=8443, ca_cert=f'{cert_dir}ca.crt')
    assert client.command("SELECT 'verify_server'") == 'verify_server'

    reset_connections()
    try:
        get_client(interface='https', host='localhost', port=8443, ca_cert=f'{cert_dir}ca.crt')
        pytest.fail('Expected TLS exception with different hostname')
    except OperationalError as ex:
        assert isinstance(ex.__cause__, SSLError)

    reset_connections()
    try:
        get_client(interface='https', host=host, port=8443)
        pytest.fail('Expected TLS exception with self signed cert')
    except OperationalError as ex:
        assert isinstance(ex.__cause__, SSLError)


def test_mutual_tls(request):
    if not request.config.getoption('tls'):
        pytest.skip('TLS tests not enabled')
    reset_connections()
    client = get_client(interface='https', username='cert_user', host=host, port=8443, ca_cert=f'{cert_dir}ca.crt',
                        client_cert=f'{cert_dir}client.crt', client_cert_key=f'{cert_dir}client.key')
    assert client.command('SELECT user()') == 'cert_user'
