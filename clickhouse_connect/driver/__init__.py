from typing import Optional, Union

from clickhouse_connect.driver.base import BaseDriver
from clickhouse_connect.driver.httpdriver import HttpDriver


def create_driver(host:str, scheme: Optional[str] = None, port:int = 0, secure:Union[bool, str] = False,
                  username:str = '', password:str = '', **kwargs) -> BaseDriver:
    use_tls = str(secure).lower() == 'true'
    if not scheme:
        scheme = 'https' if use_tls else 'http'
    if not port:
        port = 8443 if use_tls else 8123
    if password and not username:
        username = 'default'
    return HttpDriver(scheme, host, port, username, password, **kwargs)