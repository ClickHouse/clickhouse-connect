import sys
import socket

from requests.adapters import HTTPAdapter
from urllib3 import poolmanager

KEEP_INTERVAL = 30
KEEP_COUNT = 3
KEEP_IDLE = 30
SOCKET_TCP = socket.IPPROTO_TCP

core_socket_options = [
    (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
    (SOCKET_TCP, socket.TCP_NODELAY, 1)
]


class KeepAliveAdapter(HTTPAdapter):
    """
    Extended HTTP adapter that sets preferred keep alive options
    """

    # pylint: disable=no-member
    def __init__(self, **kwargs):
        self.socket_options = core_socket_options.copy()
        interval = kwargs.pop('keep_interval', KEEP_INTERVAL)
        count = kwargs.pop('keep_count', KEEP_COUNT)
        idle = kwargs.pop('keep_idle', KEEP_IDLE)

        if getattr(socket, 'TCP_KEEPINTVL', None) is not None:
            self.socket_options.append((SOCKET_TCP, socket.TCP_KEEPINTVL, interval))
        if getattr(socket, 'TCP_KEEPCNT', None) is not None:
            self.socket_options.append((SOCKET_TCP, socket.TCP_KEEPCNT, count))
        if getattr(socket, 'TCP_KEEPIDLE', None) is not None:
            self.socket_options.append((SOCKET_TCP, socket.TCP_KEEPIDLE, idle))
        if sys.platform == 'darwin':
            self.socket_options.append((SOCKET_TCP, getattr(socket, 'TCP_KEEPALIVE', 0x10), interval))
        super().__init__(**kwargs)

    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        self.poolmanager = poolmanager.PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            socket_options=self.socket_options,
            **pool_kwargs)
