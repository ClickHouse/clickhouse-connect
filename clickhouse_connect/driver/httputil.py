import http
import sys
import socket
from typing import Optional

import certifi
from urllib3 import poolmanager
from urllib3.response import HTTPResponse

# Increase this number just to be safe when ClickHouse is returning progress headers
http._MAXHEADERS = 10000  # pylint: disable=protected-access

DEFAULT_KEEP_INTERVAL = 30
DEFAULT_KEEP_COUNT = 3
DEFAULT_KEEP_IDLE = 30

SOCKET_TCP = socket.IPPROTO_TCP

core_socket_options = [
    (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
    (SOCKET_TCP, socket.TCP_NODELAY, 1),
    (socket.SOL_SOCKET, socket.SO_SNDBUF, 1024 * 256),
    (socket.SOL_SOCKET, socket.SO_SNDBUF, 1024 * 256)
]


def get_pool_manager(connections: int = 4,
                     max_size: int = 8,
                     keep_interval: int = DEFAULT_KEEP_INTERVAL,
                     keep_count: int = DEFAULT_KEEP_COUNT,
                     keep_idle: int = DEFAULT_KEEP_IDLE,
                     **kwargs):
    options = core_socket_options.copy()
    if getattr(socket, 'TCP_KEEPINTVL', None) is not None:
        options.append((SOCKET_TCP, socket.TCP_KEEPINTVL, keep_interval))
    if getattr(socket, 'TCP_KEEPCNT', None) is not None:
        options.append((SOCKET_TCP, socket.TCP_KEEPCNT, keep_count))
    if getattr(socket, 'TCP_KEEPIDLE', None) is not None:
        options.append((SOCKET_TCP, socket.TCP_KEEPIDLE, keep_idle))
    if sys.platform == 'darwin':
        options.append((SOCKET_TCP, getattr(socket, 'TCP_KEEPALIVE', 0x10), keep_interval))
    return poolmanager.PoolManager(num_pools=connections,
                                   maxsize=max_size,
                                   block=False,
                                   socket_options=options,
                                   **kwargs
                                   )


def get_https_pool_manager(ca_cert: str = None,
                           verify: bool = True,
                           client_cert: str = None,
                           client_cert_key=None, **kwargs):
    return get_pool_manager()


class ResponseSource:
    def __init__(self, response: HTTPResponse, chunk_size: int = 2 ** 16):
        self.response = response
        self.gen = response.stream(chunk_size, decode_content=False)

    def close(self):
        self.response.drain_conn()
        self.response.close()
