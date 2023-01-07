import atexit
import http
import sys
import socket
from typing import Optional

from requests import Response
from requests.adapters import HTTPAdapter
from urllib3 import poolmanager


# Increase this number just to be safe when ClickHouse is returning progress headers
http._MAXHEADERS = 10000  # pylint: disable=protected-access

KEEP_INTERVAL = 30
KEEP_COUNT = 3
KEEP_IDLE = 30

SOCKET_TCP = socket.IPPROTO_TCP

core_socket_options = [
    (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
    (SOCKET_TCP, socket.TCP_NODELAY, 1),
    (socket.SOL_SOCKET, socket.SO_SNDBUF, 1024 * 256),
    (socket.SOL_SOCKET, socket.SO_SNDBUF, 1024 * 256)
]


class KeepAliveAdapter(HTTPAdapter):
    """
    Extended HTTP adapter that sets preferred keep alive options
    """

    # pylint: disable=no-member
    def __init__(self, **kwargs):
        options = core_socket_options.copy()
        interval = kwargs.pop('keep_interval', KEEP_INTERVAL)
        count = kwargs.pop('keep_count', KEEP_COUNT)
        idle = kwargs.pop('keep_idle', KEEP_IDLE)

        if getattr(socket, 'TCP_KEEPINTVL', None) is not None:
            options.append((SOCKET_TCP, socket.TCP_KEEPINTVL, interval))
        if getattr(socket, 'TCP_KEEPCNT', None) is not None:
            options.append((SOCKET_TCP, socket.TCP_KEEPCNT, count))
        if getattr(socket, 'TCP_KEEPIDLE', None) is not None:
            options.append((SOCKET_TCP, socket.TCP_KEEPIDLE, idle))
        if sys.platform == 'darwin':
            options.append((SOCKET_TCP, getattr(socket, 'TCP_KEEPALIVE', 0x10), interval))
        self.socket_options = options
        super().__init__(**kwargs)

    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        self.poolmanager = poolmanager.PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            socket_options=self.socket_options,
            **pool_kwargs)


class ResponseSource:
    def __init__(self, response: Response, chunk_size: Optional[int] = None):
        self.response = response
        self.gen = response.iter_content(chunk_size)

    def close(self):
        if self.response.raw:
            self.response.raw.drain_conn()
        self.response.close()


# Create a single HttpAdapter that will be shared by all client sessions.  This is intended to make
# the client as thread safe as possible while sharing a single connection pool.  For the same reason we
# don't call the Session.close() method from the client so the connection pool remains available
default_adapter = KeepAliveAdapter(pool_connections=4, pool_maxsize=8, max_retries=0)
atexit.register(default_adapter.close)


def reset_connections():
    """
    Used for tests to force new connection by resetting the singleton HttpAdapter
    """
    global default_adapter  # pylint: disable=global-statement
    default_adapter = KeepAliveAdapter(pool_connections=4, pool_maxsize=8, max_retries=0)
