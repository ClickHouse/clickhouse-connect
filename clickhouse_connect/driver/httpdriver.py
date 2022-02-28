import logging
import httpx

from clickhouse_connect.driver.exceptions import ServerError, DriverError
from clickhouse_connect.driver.rbparser import parse_response

logger = logging.getLogger(__name__)

format_str = ' FORMAT RowBinaryWithNamesAndTypes'


class HttpDriver:
    @staticmethod
    def format_query(query):
        if not query.strip().endswith(format_str):
            query += format_str
        return {'query': query}

    def __init__(self, *args, **kwargs):
        https = kwargs.get('secure', 'false').lower() == 'true'
        port = kwargs.get('port')
        if not port:
            port = 8443 if https else 8123
        scheme = 'https' if https else 'http'
        self.url = '{}://{}:{}'.format(scheme, kwargs.get('host'), port)
        self.username = kwargs.get('username', 'default')
        self.password = kwargs.get('password', '')
        timeout = httpx.Timeout(connect=30.0, read=120.0, pool=10, write=120.0)
        self.client = httpx.Client(http2=True, timeout=timeout)

    def close(self):
        self.client.close()

    def query(self, query):
        params = self.format_query(query)
        try:
            response = self.client.get(self.url, auth=(self.username, self.password), params=params)
            if response.status_code == 200:
                return parse_response(response.content)
            err_str = f"HTTPDriver returned response code {response.status_code})"
            logger.error(err_str)
            if response.content:
                logger.error(str(response.content))
            raise ServerError(err_str)
        except Exception:
            logger.exception("Unexpected Http Driver Exception")
            raise DriverError(f"Error executing HTTP request {self.url}")
