import http
import sys
import socket
from typing import Optional

import certifi
import lz4.frame
import zstandard
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


def get_error_msg(response: HTTPResponse) -> Optional[str]:
    encoding = response.headers.get('content-encoding', None)
    if encoding == 'zstd':
        try:
            zstd_decom = zstandard.ZstdDecompressor()
            msg = zstd_decom.stream_reader(response.data).read()
        except zstandard.ZstdError:
            msg = response.data
    elif encoding == 'lz4':
        lz4_decom = lz4.frame.LZ4FrameDecompressor()
        msg = lz4_decom.decompress(response.data, len(response.data))
    else:
        msg = response.data
    if msg:
        return msg.decode(errors='backslashreplace')
    return None


class ResponseSource:
    def __init__(self, response: HTTPResponse, chunk_size: int = 1024 * 1024):
        self.response = response
        compress = response.headers.get('content-encoding', None)
        if compress == 'zstd':
            zstd_decom = zstandard.ZstdDecompressor()
            reader = zstd_decom.stream_reader(self, read_across_frames=False)

            def decompress():
                while True:
                    chunk = reader.read()
                    if not chunk:
                        break
                    yield chunk

            self.gen = decompress()
        elif compress == 'lz4':
            lz4_decom = lz4.frame.LZ4FrameDecompressor()

            def decompress():
                while lz4_decom.needs_input:
                    data = self.response.read(chunk_size)
                    if lz4_decom.unused_data:
                        data = lz4_decom.unused_data + data
                    if not data:
                        return
                    chunk = lz4_decom.decompress(data)
                    if chunk:
                        yield chunk

            self.gen = decompress()
        else:
            self.gen = response.stream(decode_content=True)

    def read(self, n: int) -> bytes:
        return self.response.read(n)

    def close(self):
        self.response.drain_conn()
        self.response.close()
