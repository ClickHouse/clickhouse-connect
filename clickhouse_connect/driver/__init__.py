from typing import Optional, Union

from clickhouse_connect.driver.exceptions import ProgrammingError
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.httpclient import HttpClient


# pylint: disable=too-many-arguments
def create_client(host: str = 'localhost', username: str = '', password: str = '', database: str = '__default__',
                  interface: Optional[str] = None, port: int = 0, secure: Union[bool, str] = False,
                  **kwargs) -> Client:
    use_tls = str(secure).lower() == 'true' or interface == 'https' or (not interface and port in (443, 8443))
    if not interface:
        interface = 'https' if use_tls else 'http'
    port = port or default_port(interface, use_tls)
    if password and not username:
        username = 'default'
    if interface.startswith('http'):
        client = HttpClient(interface, host, port, username, password, database, **kwargs)
    else:
        raise ProgrammingError(f'Unrecognized client type {interface}')
    return client


def default_port(interface: str, secure: bool):
    if interface.startswith('http'):
        return 8443 if secure else 8123
    raise ValueError('Unrecognized ClickHouse interface')
