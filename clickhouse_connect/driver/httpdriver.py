import logging
import json
from typing import Optional, Dict

from requests import Response, request

from clickhouse_connect.driver import BaseDriver
from clickhouse_connect.driver.exceptions import ServerError, DriverError
from clickhouse_connect.driver.query import QueryResult
from clickhouse_connect.driver.rbparser import parse_response

logger = logging.getLogger(__name__)

format_str = ' FORMAT RowBinaryWithNamesAndTypes'

def format_query(query:str) -> str:
    if not query.strip().endswith(format_str):
        query += format_str
    return query


class HttpDriver(BaseDriver):
    def __init__(self, scheme: str, host:str, port: int, username:str, password: str, database: str,
                 compress: bool = True, **kwargs):
        self.params = {}
        self.headers = {'Content-Type': 'text/plain'}
        if compress:
            self.params['enable_http_compression'] = 1
            self.headers['Accept-Encoding'] = 'br, gzip'
        self.url = '{}://{}:{}'.format(scheme, host, port)
        if database != '__default__':
            self.params['database'] = kwargs['database']
        self.auth = (username, password) if username else None

    def query(self, query:str) -> QueryResult:
        response = self.raw_request(format_query(query))
        result_set, column_names, column_types = parse_response(response.content)
        summary = {}
        if 'X-ClickHouse-Summary' in response.headers:
            try:
                summary = json.loads(response.headers['X-ClickHouse-Summary'])
            except json.JSONDecodeError:
                pass
        return QueryResult(result_set,
                           column_names,
                           column_types,
                           response.headers.get('X-ClickHouse-Query-Id'),
                           summary)

    def raw_request(self, data=None, method='post', headers: Optional[Dict] = None):
        try:
            req_headers = self.headers
            if headers:
                req_headers.update(headers)
            response:Response = request(method, self.url,
                                     auth=self.auth,
                                     headers=req_headers,
                                     timeout=(10, 60),
                                     data=data,
                                     params=self.params)
            if 200 <= response.status_code < 300:
                return response
            err_str = f"HTTPDriver returned response code {response.status_code})"
            logger.error(err_str)
            if response.content:
                logger.error(str(response.content))
            raise ServerError(err_str)
        except Exception:
            logger.exception("Unexpected Http Driver Exception")
            raise DriverError(f"Error executing HTTP request {self.url}")
