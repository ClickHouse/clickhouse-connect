import logging
import requests

from clickhouse_connect.driver import BaseDriver
from clickhouse_connect.driver.exceptions import ServerError, DriverError
from clickhouse_connect.driver.rbparser import parse_response

logger = logging.getLogger(__name__)

format_str = ' FORMAT RowBinaryWithNamesAndTypes'


def format_query(query:str) -> str:
    if not query.strip().endswith(format_str):
        query += format_str
    return query


class HttpDriver(BaseDriver):
    def __init__(self, scheme: str, host:str, port: int, username:str, password: str, database: str, **kwargs):
        self.params = {}
        self.url = '{}://{}:{}'.format(scheme, host, port)
        if database != '__default__':
            self.params['database'] = kwargs['database']
        self.auth = (username, password) if username else None

    def query(self, query:str):
        try:
            response = requests.post(self.url,
                                     auth=self.auth,
                                     timeout=(10, 60),
                                     data=format_query(query),
                                     params=self.params)
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
