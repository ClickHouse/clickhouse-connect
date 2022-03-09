from typing import Optional, Union

from clickhouse_connect.driver.basedriver import BaseDriver
from clickhouse_connect.driver.httpdriver import HttpDriver


def create_driver(host: str = 'localhost', username: str = '', password: str = '', database: str = '__default__',
                  interface: Optional[str] = None, port: int = 0, secure: Union[bool, str] = False,
                  **kwargs) -> BaseDriver:
    use_tls = str(secure).lower() == 'true' or interface == 'https'
    if not interface:
        interface = 'https' if use_tls else 'http'
    port = port or default_port(interface, use_tls)
    if password and not username:
        username = 'default'
    if interface.startswith('http'):
        return HttpDriver(interface, host, port, username, password, database, **kwargs)


def default_port(interface: str, secure: bool):
    if interface.startswith('http'):
        return 8443 if secure else 8123
    raise ValueError("Unrecognized ClickHouse interface")
