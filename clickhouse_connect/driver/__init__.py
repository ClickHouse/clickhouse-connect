from inspect import signature
from typing import Optional, Union, Dict, Any
from urllib.parse import urlparse, parse_qs

import clickhouse_connect.driver.ctypes
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.common import dict_copy
from clickhouse_connect.driver.exceptions import ProgrammingError
from clickhouse_connect.driver.httpclient import HttpClient


# pylint: disable=too-many-arguments,too-many-locals,too-many-branches
def create_client(host: str = None,
                  username: str = None,
                  password: str = '',
                  database: str = '__default__',
                  interface: Optional[str] = None,
                  port: int = 0,
                  secure: Union[bool, str] = False,
                  dsn: Optional[str] = None,
                  settings: Optional[Dict[str, Any]] = None,
                  generic_args: Optional[Dict[str, Any]] = None,
                  **kwargs) -> Client:
    if dsn:
        parsed = urlparse(dsn)
        username = username or parsed.username
        password = password or parsed.password
        host = host or parsed.hostname
        port = port or parsed.port
        if parsed.path and not database:
            database = parsed.path[1:].split('/')[0]
        database = database or parsed.path
        kwargs.update(dict(parse_qs(parsed.query)))
    use_tls = str(secure).lower() == 'true' or interface == 'https' or (not interface and port in (443, 8443))
    if not host:
        host = 'localhost'
    if not interface:
        interface = 'https' if use_tls else 'http'
    port = port or default_port(interface, use_tls)
    if username is None and 'user' in kwargs:
        username = kwargs.pop('user')
    if username is None and 'user_name' in kwargs:
        username = kwargs.pop('user_name')
    if password and username is None:
        username = 'default'
    if 'compression' in kwargs and 'compress' not in kwargs:
        kwargs['compress'] = kwargs.pop('compression')
    settings = settings or {}
    if interface.startswith('http'):
        if generic_args:
            client_params = signature(HttpClient).parameters
            for name, value in generic_args.items():
                if name in client_params:
                    kwargs[name] = value
                else:
                    if name.startswith('ch_'):
                        name = name[3:]
                    settings[name] = value
        return HttpClient(interface, host, port, username, password, database, settings=settings, **kwargs)
    raise ProgrammingError(f'Unrecognized client type {interface}')


def default_port(interface: str, secure: bool):
    if interface.startswith('http'):
        return 8443 if secure else 8123
    raise ValueError('Unrecognized ClickHouse interface')
